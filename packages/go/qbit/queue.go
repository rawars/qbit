package qbit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxEvents = 10_000

const recoverBatchSize = 100

const (
	defaultCompletedRetention = 24 * time.Hour
	defaultFailedRetention    = 7 * 24 * time.Hour
)

var (
	// ErrNoJob indicates that no group currently has runnable work.
	ErrNoJob = errors.New("qbit: no job available")
	// ErrReservationLost indicates that the caller no longer owns the job lock.
	ErrReservationLost = errors.New("qbit: reservation lost")
	// ErrQueuePaused indicates that an administrator paused new reservations.
	ErrQueuePaused = errors.New("qbit: queue is paused")
)

// Queue stores and reserves grouped jobs in Redis.
type Queue struct {
	client             redis.UniversalClient
	keys               queueKeys
	name               string
	completedRetention time.Duration
	failedRetention    time.Duration
	registryMu         sync.Mutex
	registered         bool
}

// NewQueue constructs a queue using an existing Redis client.
func NewQueue(name string, client redis.UniversalClient, options ...QueueOption) (*Queue, error) {
	if client == nil {
		return nil, errors.New("qbit: Redis client is required")
	}
	keys, err := newQueueKeys(name)
	if err != nil {
		return nil, err
	}
	config := queueOptions{
		completedRetention: defaultCompletedRetention,
		failedRetention:    defaultFailedRetention,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.completedRetention <= 0 || config.failedRetention <= 0 {
		return nil, errors.New("qbit: retention durations must be positive")
	}
	return &Queue{
		client:             client,
		keys:               keys,
		name:               name,
		completedRetention: config.completedRetention,
		failedRetention:    config.failedRetention,
	}, nil
}

// Add appends a job to its group and makes an idle group runnable.
func (queue *Queue) Add(ctx context.Context, name string, payload []byte, options ...AddOption) (*Job, error) {
	if name == "" {
		return nil, errors.New("qbit: job name is required")
	}
	if err := validateIdentifier("job name", name, maxIdentifierLength, "{}"); err != nil {
		return nil, err
	}
	config := addOptions{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.group != "" {
		if err := validateIdentifier("group", config.group, maxIdentifierLength, ":{}"); err != nil {
			return nil, err
		}
	}
	if config.hasJobID && config.jobID == "" {
		return nil, errors.New("qbit: job ID cannot be empty")
	}
	if config.hasJobID {
		if err := validateIdentifier("job ID", config.jobID, maxIdentifierLength, ":{}"); err != nil {
			return nil, err
		}
		if len(config.jobID) >= len("auto-") && config.jobID[:len("auto-")] == "auto-" {
			return nil, errors.New("qbit: job ID uses the reserved auto- prefix")
		}
	}
	if err := queue.ensureRegistered(ctx); err != nil {
		return nil, err
	}

	result, err := addScript.Run(ctx, queue.client,
		[]string{queue.keys.id(), queue.keys.ready(), queue.keys.readySet(), queue.keys.active(), queue.keys.events(), queue.keys.marker(), queue.keys.metrics()},
		queue.keys.base, name, config.jobID, config.group, payload, time.Now().UnixMilli(), maxEvents,
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("qbit: add job: %w", err)
	}
	return &Job{
		ID:        stringValue(result[0]),
		Group:     stringValue(result[1]),
		Duplicate: intValue(result[2]) == 1,
		Name:      stringValue(result[3]),
		Payload:   []byte(stringValue(result[4])),
	}, nil
}

// Publish adds a job to the queue. It is the high-level API name for Add;
// both methods use the same Redis protocol and options.
func (queue *Queue) Publish(ctx context.Context, name string, payload []byte, options ...PublishOption) (*Job, error) {
	return queue.Add(ctx, name, payload, options...)
}

// Reserve atomically selects the next group and locks one of its jobs.
func (queue *Queue) Reserve(ctx context.Context, options ...ReserveOption) (*Job, error) {
	config := reserveOptions{lockTTL: 30 * time.Second}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.lockTTL <= 0 {
		return nil, errors.New("qbit: lock TTL must be positive")
	}
	if err := queue.ensureRegistered(ctx); err != nil {
		return nil, err
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	result, err := reserveScript.Run(ctx, queue.client,
		[]string{queue.keys.ready(), queue.keys.readySet(), queue.keys.active(), queue.keys.events(), queue.keys.activeJobs(), queue.keys.marker(), queue.keys.metrics(), queue.keys.paused()},
		queue.keys.base, token, config.lockTTL.Milliseconds(), time.Now().UnixMilli(), maxEvents, recoverBatchSize,
	).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("qbit: reserve job: %w", err)
	}
	if len(result) == 1 && stringValue(result[0]) == "__qbit_paused__" {
		return nil, ErrQueuePaused
	}
	if len(result) < 5 {
		return nil, errors.New("qbit: reserve returned an invalid response")
	}
	return &Job{
		ID:       stringValue(result[0]),
		Name:     stringValue(result[1]),
		Group:    stringValue(result[2]),
		Payload:  []byte(stringValue(result[3])),
		Token:    token,
		Attempts: int(intValue(result[4])),
	}, nil
}

func (queue *Queue) ensureRegistered(ctx context.Context) error {
	queue.registryMu.Lock()
	defer queue.registryMu.Unlock()
	if queue.registered {
		return nil
	}
	if err := queue.client.SAdd(ctx, queueRegistryKey, queue.name).Err(); err != nil {
		return fmt.Errorf("qbit: register queue: %w", err)
	}
	queue.registered = true
	return nil
}

// ReserveBlocking waits until a grouped job is available or wait elapses. A
// zero wait blocks until the context is cancelled.
func (queue *Queue) ReserveBlocking(ctx context.Context, wait time.Duration, options ...ReserveOption) (*Job, error) {
	if wait < 0 {
		return nil, errors.New("qbit: wait duration cannot be negative")
	}
	var deadline time.Time
	if wait > 0 {
		deadline = time.Now().Add(wait)
	}
	for {
		job, err := queue.Reserve(ctx, options...)
		if errors.Is(err, ErrQueuePaused) {
			pollFor := 250 * time.Millisecond
			if !deadline.IsZero() {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					return nil, ErrQueuePaused
				}
				if remaining < pollFor {
					pollFor = remaining
				}
			}
			timer := time.NewTimer(pollFor)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if !errors.Is(err, ErrNoJob) {
			return job, err
		}

		blockFor := time.Second
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, ErrNoJob
			}
			if remaining < blockFor {
				blockFor = remaining
			}
		}
		timeoutSeconds := strconv.FormatFloat(blockFor.Seconds(), 'f', -1, 64)
		_, err = queue.client.Do(ctx, "BZPOPMIN", queue.keys.marker(), timeoutSeconds).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return nil, ErrNoJob
			}
			return nil, fmt.Errorf("qbit: wait for job: %w", err)
		}
	}
}

// Name returns the queue name supplied to NewQueue or Client.Queue.
func (queue *Queue) Name() string {
	if queue == nil {
		return ""
	}
	return queue.name
}

// Pause prevents new reservations across every worker replica. Jobs already
// being processed are allowed to finish.
func (queue *Queue) Pause(ctx context.Context) error {
	if err := queue.ensureRegistered(ctx); err != nil {
		return err
	}
	if err := pauseScript.Run(ctx, queue.client,
		[]string{queue.keys.paused(), queue.keys.events()},
		time.Now().UnixMilli(), maxEvents,
	).Err(); err != nil {
		return fmt.Errorf("qbit: pause queue: %w", err)
	}
	return nil
}

// Resume allows workers to reserve new jobs and wakes blocked workers.
func (queue *Queue) Resume(ctx context.Context) error {
	if err := queue.ensureRegistered(ctx); err != nil {
		return err
	}
	if err := resumeScript.Run(ctx, queue.client,
		[]string{queue.keys.paused(), queue.keys.ready(), queue.keys.marker(), queue.keys.events()},
		maxEvents,
	).Err(); err != nil {
		return fmt.Errorf("qbit: resume queue: %w", err)
	}
	return nil
}

// Paused reports whether new reservations are disabled for this queue.
func (queue *Queue) Paused(ctx context.Context) (bool, error) {
	result, err := queue.client.Exists(ctx, queue.keys.paused()).Result()
	if err != nil {
		return false, fmt.Errorf("qbit: read queue pause state: %w", err)
	}
	return result == 1, nil
}

// Renew extends a reservation while the caller still owns its token.
func (queue *Queue) Renew(ctx context.Context, job *Job, ttl time.Duration) error {
	if job == nil || job.ID == "" || job.Token == "" {
		return errors.New("qbit: reserved job and token are required")
	}
	if ttl <= 0 {
		return errors.New("qbit: lock TTL must be positive")
	}
	result, err := renewScript.Run(ctx, queue.client,
		[]string{queue.keys.lock(job.ID), queue.keys.activeJobs()},
		job.Token, ttl.Milliseconds(), time.Now().UnixMilli(), job.ID,
	).Int64()
	if err != nil {
		return fmt.Errorf("qbit: renew job: %w", err)
	}
	if result != 1 {
		return ErrReservationLost
	}
	return nil
}

// Complete acknowledges a successfully processed job.
func (queue *Queue) Complete(ctx context.Context, job *Job) error {
	return queue.finish(ctx, job, "completed", "")
}

// Fail acknowledges an unsuccessfully processed job.
func (queue *Queue) Fail(ctx context.Context, job *Job, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return queue.finish(ctx, job, "failed", message)
}

// Retry records an unsuccessful processing attempt and returns the job to the
// head of its group. A later successful Complete is reported as a recovery.
func (queue *Queue) Retry(ctx context.Context, job *Job, cause error) error {
	if job == nil || job.ID == "" || job.Token == "" {
		return errors.New("qbit: reserved job and token are required")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := retryScript.Run(ctx, queue.client,
		[]string{queue.keys.lock(job.ID), queue.keys.job(job.ID), queue.keys.active(), queue.keys.activeJobs(), queue.keys.ready(), queue.keys.readySet(), queue.keys.events(), queue.keys.marker(), queue.keys.metrics()},
		queue.keys.base, job.Token, time.Now().UnixMilli(), message, maxEvents, job.ID,
	).Int64()
	if err != nil {
		return fmt.Errorf("qbit: retry job: %w", err)
	}
	if result != 1 {
		return ErrReservationLost
	}
	return nil
}

func (queue *Queue) finish(ctx context.Context, job *Job, state, message string) error {
	if job == nil || job.ID == "" || job.Token == "" {
		return errors.New("qbit: reserved job and token are required")
	}
	retention := queue.completedRetention
	if state == "failed" {
		retention = queue.failedRetention
	}
	result, err := finishScript.Run(ctx, queue.client,
		[]string{queue.keys.lock(job.ID), queue.keys.job(job.ID), queue.keys.active(), queue.keys.activeJobs(), queue.keys.ready(), queue.keys.readySet(), queue.keys.events(), queue.keys.marker(), queue.keys.metrics()},
		queue.keys.base, job.Token, state, time.Now().UnixMilli(), message, maxEvents, job.ID, retention.Milliseconds(),
	).Int64()
	if err != nil {
		return fmt.Errorf("qbit: finish job: %w", err)
	}
	if result != 1 {
		return ErrReservationLost
	}
	return nil
}

func randomToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("qbit: create lock token: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprint(value)
	}
}

func intValue(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseInt(string(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}
