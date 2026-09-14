package qbit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

const defaultWorkerTTL = 15 * time.Second

var ErrWorkerRegistrationLost = errors.New("qbit: worker registration lost")

// WorkerInfo describes one active worker process. Concurrency is the number of
// jobs that process can handle simultaneously.
type WorkerInfo struct {
	ID              string    `json:"id"`
	Instance        string    `json:"instance"`
	Concurrency     int       `json:"concurrency"`
	StartedAt       time.Time `json:"started_at"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// WorkerRegistration is a leased worker presence record in Redis.
type WorkerRegistration struct {
	queue *Queue
	info  WorkerInfo
	ttl   time.Duration
}

// RegisterWorker registers one worker process and its concurrency. Call
// Maintain for the lifetime of the worker so crashed processes disappear
// automatically after the registration TTL.
func (queue *Queue) RegisterWorker(ctx context.Context, concurrency int, options ...WorkerOption) (*WorkerRegistration, error) {
	if concurrency <= 0 {
		return nil, errors.New("qbit: worker concurrency must be positive")
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	config := workerOptions{instance: hostname, ttl: defaultWorkerTTL}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.id == "" {
		config.id, err = randomToken()
		if err != nil {
			return nil, err
		}
	}
	if err := validateIdentifier("worker ID", config.id, maxIdentifierLength, ":{}"); err != nil {
		return nil, err
	}
	if err := validateIdentifier("worker instance", config.instance, maxIdentifierLength, "{}"); err != nil {
		return nil, err
	}
	if config.ttl < 3*time.Second {
		return nil, errors.New("qbit: worker TTL must be at least 3 seconds")
	}
	if err := queue.ensureRegistered(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	if err := registerWorkerScript.Run(ctx, queue.client,
		[]string{queue.keys.workers(), queue.keys.worker(config.id)},
		config.id, config.instance, concurrency, now.UnixMilli(), config.ttl.Milliseconds(),
	).Err(); err != nil {
		return nil, fmt.Errorf("qbit: register worker: %w", err)
	}
	return &WorkerRegistration{
		queue: queue,
		info: WorkerInfo{
			ID:              config.id,
			Instance:        config.instance,
			Concurrency:     concurrency,
			StartedAt:       now.UTC(),
			LastHeartbeatAt: now.UTC(),
			ExpiresAt:       now.Add(config.ttl).UTC(),
		},
		ttl: config.ttl,
	}, nil
}

// Info returns the registration metadata last written by this process.
func (registration *WorkerRegistration) Info() WorkerInfo { return registration.info }

// Heartbeat renews a worker registration lease.
func (registration *WorkerRegistration) Heartbeat(ctx context.Context) error {
	if registration == nil || registration.queue == nil {
		return ErrWorkerRegistrationLost
	}
	now := time.Now()
	result, err := heartbeatWorkerScript.Run(ctx, registration.queue.client,
		[]string{registration.queue.keys.workers(), registration.queue.keys.worker(registration.info.ID)},
		registration.info.ID, now.UnixMilli(), registration.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return fmt.Errorf("qbit: heartbeat worker: %w", err)
	}
	if result != 1 {
		return ErrWorkerRegistrationLost
	}
	registration.info.LastHeartbeatAt = now.UTC()
	registration.info.ExpiresAt = now.Add(registration.ttl).UTC()
	return nil
}

// Maintain sends heartbeats until ctx is cancelled, then unregisters the
// worker. It returns only heartbeat or unregister errors.
func (registration *WorkerRegistration) Maintain(ctx context.Context) error {
	if registration == nil || registration.queue == nil {
		return ErrWorkerRegistrationLost
	}
	ticker := time.NewTicker(registration.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			closeContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return registration.Unregister(closeContext)
		case <-ticker.C:
			if err := registration.Heartbeat(ctx); err != nil {
				return err
			}
		}
	}
}

// Unregister removes the worker immediately instead of waiting for its lease
// to expire.
func (registration *WorkerRegistration) Unregister(ctx context.Context) error {
	if registration == nil || registration.queue == nil {
		return ErrWorkerRegistrationLost
	}
	if err := unregisterWorkerScript.Run(ctx, registration.queue.client,
		[]string{registration.queue.keys.workers(), registration.queue.keys.worker(registration.info.ID)},
		registration.info.ID,
	).Err(); err != nil {
		return fmt.Errorf("qbit: unregister worker: %w", err)
	}
	return nil
}

func workerInfo(values map[string]string, expiresAt int64) (WorkerInfo, bool) {
	if len(values) == 0 {
		return WorkerInfo{}, false
	}
	concurrency, err := strconv.Atoi(values["concurrency"])
	if err != nil || concurrency <= 0 {
		return WorkerInfo{}, false
	}
	startedAt, _ := strconv.ParseInt(values["started_at"], 10, 64)
	heartbeatAt, _ := strconv.ParseInt(values["heartbeat_at"], 10, 64)
	return WorkerInfo{
		ID:              values["id"],
		Instance:        values["instance"],
		Concurrency:     concurrency,
		StartedAt:       time.UnixMilli(startedAt).UTC(),
		LastHeartbeatAt: time.UnixMilli(heartbeatAt).UTC(),
		ExpiresAt:       time.UnixMilli(expiresAt).UTC(),
	}, true
}
