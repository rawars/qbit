package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	qbit "github.com/rawars/qbit/packages/go"
	"github.com/redis/go-redis/v9"
)

//go:embed web/*
var webFiles embed.FS

type Config struct {
	RedisAddress             string           `json:"redis_address"`
	Queue                    string           `json:"queue"`
	TrafficProfiles          []TrafficProfile `json:"traffic_profiles"`
	ArrivalWeights           []float64        `json:"arrival_weights"`
	VirtualDurationMinutes   int              `json:"virtual_duration_minutes"`
	TestDurationSeconds      int              `json:"test_duration_seconds"`
	PublishRate              float64          `json:"publish_rate"`
	PublisherConcurrency     int              `json:"publisher_concurrency"`
	PublisherRedisPoolSize   int              `json:"publisher_redis_pool_size"`
	PayloadBytes             int              `json:"payload_bytes"`
	WorkerReplicas           int              `json:"worker_replicas"`
	WorkerConcurrency        int              `json:"worker_concurrency"`
	WorkerRedisPoolSize      int              `json:"worker_redis_pool_size"`
	MonitorRedisPoolSize     int              `json:"monitor_redis_pool_size"`
	ProcessingMillis         int              `json:"processing_millis"`
	ProcessingJitterMillis   int              `json:"processing_jitter_millis"`
	QueueWaitSLAms           int              `json:"queue_wait_sla_ms"`
	TransientFailurePercent  float64          `json:"transient_failure_percent"`
	PermanentFailurePercent  float64          `json:"permanent_failure_percent"`
	DuplicatePublishPercent  float64          `json:"duplicate_publish_percent"`
	MaxAttempts              int              `json:"max_attempts"`
	RetryInitialMillis       int              `json:"retry_initial_millis"`
	RetryMaximumMillis       int              `json:"retry_maximum_millis"`
	CompletedRetentionMinute int              `json:"completed_retention_minutes"`
	FailedRetentionMinute    int              `json:"failed_retention_minutes"`
}

type TrafficProfile struct {
	Account           string `json:"account"`
	Agent             string `json:"agent"`
	PeoplePerHour     int    `json:"people_per_hour"`
	MessagesPerPerson int    `json:"messages_per_person"`
}

func defaultConfig(redisAddress string) Config {
	return Config{
		RedisAddress: redisAddress,
		Queue:        "fazpi-sim-" + time.Now().Format("150405"),
		TrafficProfiles: []TrafficProfile{
			{Account: "Casur", Agent: "Kata en línea", PeoplePerHour: 100_000, MessagesPerPerson: 1},
			{Account: "Casur", Agent: "Agente 2 (completar tasa)", PeoplePerHour: 0, MessagesPerPerson: 1},
			{Account: "Pascual", Agent: "Agente principal", PeoplePerHour: 500, MessagesPerPerson: 1},
			{Account: "Cuenta 3", Agent: "Agente principal", PeoplePerHour: 100, MessagesPerPerson: 1},
			{Account: "Cuenta 4", Agent: "Agente principal", PeoplePerHour: 100, MessagesPerPerson: 1},
		},
		ArrivalWeights:           []float64{0.35, 0.45, 0.65, 1.0, 1.55, 2.5, 2.0, 1.35, 0.9, 0.6, 0.4, 0.25},
		VirtualDurationMinutes:   60,
		TestDurationSeconds:      60,
		PublishRate:              0,
		PublisherConcurrency:     8,
		PublisherRedisPoolSize:   64,
		PayloadBytes:             512,
		WorkerReplicas:           4,
		WorkerConcurrency:        25,
		WorkerRedisPoolSize:      35,
		MonitorRedisPoolSize:     16,
		ProcessingMillis:         50,
		ProcessingJitterMillis:   25,
		QueueWaitSLAms:           2_000,
		TransientFailurePercent:  5,
		PermanentFailurePercent:  1,
		DuplicatePublishPercent:  2,
		MaxAttempts:              3,
		RetryInitialMillis:       100,
		RetryMaximumMillis:       2_000,
		CompletedRetentionMinute: 15,
		FailedRetentionMinute:    60,
	}
}

func (config Config) validate() error {
	if strings.TrimSpace(config.RedisAddress) == "" {
		return errors.New("Redis address is required")
	}
	if strings.TrimSpace(config.Queue) == "" {
		return errors.New("queue is required")
	}
	if len(config.TrafficProfiles) == 0 || len(config.TrafficProfiles) > 10_000 {
		return errors.New("traffic profiles must contain between 1 and 10000 agents")
	}
	for index, profile := range config.TrafficProfiles {
		if strings.TrimSpace(profile.Account) == "" || strings.TrimSpace(profile.Agent) == "" {
			return fmt.Errorf("traffic profile %d requires an account and agent", index+1)
		}
		if profile.PeoplePerHour < 0 || profile.PeoplePerHour > 100_000_000 {
			return fmt.Errorf("people per hour for %s / %s must be between 0 and 100000000", profile.Account, profile.Agent)
		}
		if profile.MessagesPerPerson <= 0 || profile.MessagesPerPerson > 10_000 {
			return fmt.Errorf("messages per person for %s / %s must be between 1 and 10000", profile.Account, profile.Agent)
		}
	}
	if len(config.ArrivalWeights) != 12 {
		return errors.New("arrival weights must contain exactly 12 five-minute windows")
	}
	weightTotal := float64(0)
	for _, weight := range config.ArrivalWeights {
		if weight < 0 || weight > 100 {
			return errors.New("each arrival weight must be between 0 and 100")
		}
		weightTotal += weight
	}
	if weightTotal == 0 {
		return errors.New("at least one arrival weight must be greater than zero")
	}
	if config.VirtualDurationMinutes <= 0 || config.VirtualDurationMinutes > 24*60 {
		return errors.New("virtual duration must be between 1 and 1440 minutes")
	}
	if config.TestDurationSeconds <= 0 || config.TestDurationSeconds > 24*60*60 {
		return errors.New("test duration must be between 1 and 86400 seconds")
	}
	expected := config.expectedUnique()
	if expected <= 0 || expected > 100_000_000 {
		return errors.New("the scenario must contain at most 100000000 unique messages")
	}
	if config.PublishRate < 0 || config.PublishRate > 1_000_000 {
		return errors.New("publish rate must be between 0 and 1000000")
	}
	if config.PublisherConcurrency <= 0 || config.PublisherConcurrency > 10_000 {
		return errors.New("publisher concurrency must be between 1 and 10000")
	}
	if config.PublisherRedisPoolSize < config.PublisherConcurrency || config.PublisherRedisPoolSize > 1_000_000 {
		return errors.New("publisher Redis pool size must be at least publisher concurrency and at most 1000000")
	}
	if config.PayloadBytes < 0 || config.PayloadBytes > 1<<20 {
		return errors.New("payload bytes must be between 0 and 1048576")
	}
	if config.WorkerReplicas <= 0 || config.WorkerReplicas > 1_000 {
		return errors.New("worker replicas must be between 1 and 1000")
	}
	if config.WorkerConcurrency <= 0 || config.WorkerConcurrency > 10_000 {
		return errors.New("worker concurrency must be between 1 and 10000")
	}
	if config.WorkerRedisPoolSize < config.WorkerConcurrency+2 || config.WorkerRedisPoolSize > 1_000_000 {
		return errors.New("worker Redis pool size per replica must be at least worker concurrency plus 2 and at most 1000000")
	}
	if config.MonitorRedisPoolSize < 2 || config.MonitorRedisPoolSize > 1_000_000 {
		return errors.New("monitor Redis pool size must be between 2 and 1000000")
	}
	if int64(config.PublisherRedisPoolSize)+int64(config.WorkerReplicas)*int64(config.WorkerRedisPoolSize)+int64(config.MonitorRedisPoolSize) > 10_000_000 {
		return errors.New("combined Redis pool capacity must not exceed 10000000 connections")
	}
	if config.ProcessingMillis < 0 || config.ProcessingJitterMillis < 0 {
		return errors.New("processing durations cannot be negative")
	}
	if config.QueueWaitSLAms <= 0 || config.QueueWaitSLAms > 3_600_000 {
		return errors.New("queue wait SLA must be between 1 and 3600000 milliseconds")
	}
	for name, value := range map[string]float64{
		"transient failure percent": config.TransientFailurePercent,
		"permanent failure percent": config.PermanentFailurePercent,
		"duplicate publish percent": config.DuplicatePublishPercent,
	} {
		if value < 0 || value > 100 {
			return fmt.Errorf("%s must be between 0 and 100", name)
		}
	}
	if config.TransientFailurePercent+config.PermanentFailurePercent > 100 {
		return errors.New("transient and permanent failure percentages cannot exceed 100 combined")
	}
	if config.MaxAttempts <= 0 || config.MaxAttempts > 100 {
		return errors.New("max attempts must be between 1 and 100")
	}
	if config.TransientFailurePercent > 0 && config.MaxAttempts < 2 {
		return errors.New("max attempts must be at least 2 when transient failures are enabled")
	}
	if config.RetryInitialMillis < 0 || config.RetryMaximumMillis < config.RetryInitialMillis {
		return errors.New("retry maximum must be greater than or equal to retry initial")
	}
	if config.CompletedRetentionMinute <= 0 || config.FailedRetentionMinute <= 0 {
		return errors.New("retention values must be positive")
	}
	return nil
}

func (config Config) conversationsFor(profile TrafficProfile) int64 {
	return (int64(profile.PeoplePerHour)*int64(config.VirtualDurationMinutes) + 59) / 60
}

func (config Config) expectedUnique() int64 {
	var messages int64
	for _, profile := range config.TrafficProfiles {
		messages += config.conversationsFor(profile) * int64(profile.MessagesPerPerson)
	}
	return messages
}

func (config Config) redisPoolPlan() RedisPoolPlan {
	workersTotal := config.WorkerReplicas * config.WorkerRedisPoolSize
	return RedisPoolPlan{
		Publisher:          config.PublisherRedisPoolSize,
		WorkerPerReplica:   config.WorkerRedisPoolSize,
		WorkerReplicas:     config.WorkerReplicas,
		WorkersTotal:       workersTotal,
		Monitor:            config.MonitorRedisPoolSize,
		MaximumConnections: config.PublisherRedisPoolSize + workersTotal + config.MonitorRedisPoolSize,
	}
}

type simulatedMessage struct {
	RunID               string `json:"run_id"`
	Profile             int    `json:"profile"`
	Account             string `json:"account"`
	AgentID             string `json:"agent_id"`
	ThreadID            string `json:"thread_id"`
	Message             string `json:"message_id"`
	Sequence            int    `json:"sequence"`
	PublishedAtUnixNano int64  `json:"published_at_unix_nano"`
}

type counters struct {
	PublishAttempts       atomic.Int64
	PublishedUnique       atomic.Int64
	DuplicateAttempts     atomic.Int64
	DuplicatesConfirmed   atomic.Int64
	PublishErrors         atomic.Int64
	HandlerAttempts       atomic.Int64
	Completed             atomic.Int64
	TransientFailures     atomic.Int64
	PermanentFailures     atomic.Int64
	OrderingViolations    atomic.Int64
	ConcurrencyViolations atomic.Int64
}

type CounterSnapshot struct {
	PublishAttempts       int64 `json:"publish_attempts"`
	PublishedUnique       int64 `json:"published_unique"`
	DuplicateAttempts     int64 `json:"duplicate_attempts"`
	DuplicatesConfirmed   int64 `json:"duplicates_confirmed"`
	PublishErrors         int64 `json:"publish_errors"`
	HandlerAttempts       int64 `json:"handler_attempts"`
	Completed             int64 `json:"completed"`
	TransientFailures     int64 `json:"transient_failures"`
	PermanentFailures     int64 `json:"permanent_failures"`
	OrderingViolations    int64 `json:"ordering_violations"`
	ConcurrencyViolations int64 `json:"concurrency_violations"`
}

type profileCounters struct {
	published       atomic.Int64
	handlerAttempts atomic.Int64
	completed       atomic.Int64
	permanent       atomic.Int64
	transient       atomic.Int64
	active          atomic.Int64
	maxActive       atomic.Int64
	maxBacklog      atomic.Int64
	waitNanos       atomic.Int64
	waitSamples     atomic.Int64
	maxWaitNanos    atomic.Int64
}

type ProfileSnapshot struct {
	Account               string  `json:"account"`
	Agent                 string  `json:"agent"`
	PeoplePerHour         int     `json:"people_per_hour"`
	MessagesPerPerson     int     `json:"messages_per_person"`
	ExpectedConversations int64   `json:"expected_conversations"`
	ExpectedMessages      int64   `json:"expected_messages"`
	Published             int64   `json:"published"`
	HandlerAttempts       int64   `json:"handler_attempts"`
	Completed             int64   `json:"completed"`
	PermanentFailures     int64   `json:"permanent_failures"`
	TransientFailures     int64   `json:"transient_failures"`
	Active                int64   `json:"active"`
	CurrentBacklog        int64   `json:"current_backlog"`
	PeakBacklog           int64   `json:"peak_backlog"`
	AverageQueueWaitMS    float64 `json:"average_queue_wait_ms"`
	MaximumQueueWaitMS    float64 `json:"maximum_queue_wait_ms"`
}

type CapacityPlan struct {
	TargetMessagesPerSecond float64 `json:"target_messages_per_second"`
	EstimatedWorkerCapacity float64 `json:"estimated_worker_capacity_per_second"`
	EstimatedRequiredSlots  int     `json:"estimated_required_slots"`
	ConfiguredSlots         int     `json:"configured_slots"`
	EstimatedCapacityMargin float64 `json:"estimated_capacity_margin"`
}

type RedisPoolPlan struct {
	Publisher          int `json:"publisher"`
	WorkerPerReplica   int `json:"worker_per_replica"`
	WorkerReplicas     int `json:"worker_replicas"`
	WorkersTotal       int `json:"workers_total"`
	Monitor            int `json:"monitor"`
	MaximumConnections int `json:"maximum_connections"`
}

type RedisPoolUsage struct {
	Configured       int     `json:"configured"`
	TotalConnections uint64  `json:"total_connections"`
	IdleConnections  uint64  `json:"idle_connections"`
	InUseConnections uint64  `json:"in_use_connections"`
	Hits             uint64  `json:"hits"`
	Misses           uint64  `json:"misses"`
	WaitCount        uint64  `json:"wait_count"`
	WaitDurationMS   float64 `json:"wait_duration_ms"`
	Timeouts         uint64  `json:"timeouts"`
}

type RedisPoolsSnapshot struct {
	Plan      RedisPoolPlan  `json:"plan"`
	Publisher RedisPoolUsage `json:"publisher"`
	Workers   RedisPoolUsage `json:"workers"`
	Monitor   RedisPoolUsage `json:"monitor"`
}

type PublicationTiming struct {
	TargetDurationSeconds    float64 `json:"target_duration_seconds"`
	ToleranceSeconds         float64 `json:"tolerance_seconds"`
	ProducerElapsedSeconds   float64 `json:"producer_elapsed_seconds"`
	AverageScheduleDelayMS   float64 `json:"average_schedule_delay_ms"`
	MaximumScheduleDelayMS   float64 `json:"maximum_schedule_delay_ms"`
	PublishedWithinTolerance bool    `json:"published_within_tolerance"`
}

func storeMaximum(target *atomic.Int64, value int64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (values *counters) snapshot() CounterSnapshot {
	return CounterSnapshot{
		PublishAttempts:       values.PublishAttempts.Load(),
		PublishedUnique:       values.PublishedUnique.Load(),
		DuplicateAttempts:     values.DuplicateAttempts.Load(),
		DuplicatesConfirmed:   values.DuplicatesConfirmed.Load(),
		PublishErrors:         values.PublishErrors.Load(),
		HandlerAttempts:       values.HandlerAttempts.Load(),
		Completed:             values.Completed.Load(),
		TransientFailures:     values.TransientFailures.Load(),
		PermanentFailures:     values.PermanentFailures.Load(),
		OrderingViolations:    values.OrderingViolations.Load(),
		ConcurrencyViolations: values.ConcurrencyViolations.Load(),
	}
}

type Sample struct {
	At            time.Time `json:"at"`
	Waiting       int64     `json:"waiting"`
	Active        int64     `json:"active"`
	Expired       int64     `json:"expired"`
	PublishedRate float64   `json:"published_rate"`
	CompletedRate float64   `json:"completed_rate"`
}

type LogEntry struct {
	At      time.Time `json:"at"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

type Validation struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Explanation string `json:"explanation"`
}

type Snapshot struct {
	RunID          string               `json:"run_id,omitempty"`
	Status         string               `json:"status"`
	Error          string               `json:"error,omitempty"`
	Config         Config               `json:"config"`
	ExpectedUnique int64                `json:"expected_unique"`
	StartedAt      time.Time            `json:"started_at,omitempty"`
	FinishedAt     time.Time            `json:"finished_at,omitempty"`
	ElapsedSeconds float64              `json:"elapsed_seconds"`
	ProducerDone   bool                 `json:"producer_done"`
	Counters       CounterSnapshot      `json:"counters"`
	Profiles       []ProfileSnapshot    `json:"profiles"`
	Capacity       CapacityPlan         `json:"capacity"`
	RedisPools     RedisPoolsSnapshot   `json:"redis_pools"`
	Publication    PublicationTiming    `json:"publication_timing"`
	QueueStats     qbit.Stats           `json:"queue_stats"`
	QueueTotals    qbit.LifecycleCounts `json:"queue_totals_for_run"`
	Samples        []Sample             `json:"samples"`
	Logs           []LogEntry           `json:"logs"`
	Events         []qbit.Event         `json:"events"`
	Validations    []Validation         `json:"validations"`
}

type simulation struct {
	config          Config
	runID           string
	expectedUnique  int64
	publisherClient *qbit.Client
	publisherQueue  *qbit.Queue
	monitorClient   *qbit.Client
	monitorQueue    *qbit.Queue
	workerClients   []*qbit.Client
	workerQueues    []*qbit.Queue
	baseline        qbit.LifecycleCounts
	ctx             context.Context
	cancel          context.CancelFunc
	startedAt       time.Time

	counts        counters
	profileCounts []profileCounters

	validationMu sync.Mutex
	activeGroups map[string]int
	lastSequence map[string]int

	stateMu            sync.RWMutex
	status             string
	errorMessage       string
	finishedAt         time.Time
	producerFinishedAt time.Time
	producerDone       bool
	stopRequested      bool
	stats              qbit.Stats
	samples            []Sample
	logs               []LogEntry
	events             []qbit.Event

	producerWG sync.WaitGroup
	workerWG   sync.WaitGroup
	finishOnce sync.Once

	publishScheduleDelayNanos atomic.Int64
	publishScheduleSamples    atomic.Int64
	maximumPublishDelayNanos  atomic.Int64
}

type application struct {
	mu                sync.RWMutex
	current           *simulation
	defaultRedis      string
	cleanupInProgress bool
}

type cleanupRequest struct {
	RedisAddress string `json:"redis_address"`
	Queue        string `json:"queue"`
}

type cleanupResult struct {
	Queue        string `json:"queue"`
	KeysUnlinked int64  `json:"keys_unlinked"`
}

func newApplication(defaultRedis string) *application {
	return &application{defaultRedis: defaultRedis}
}

func newQueueClient(config Config, poolSize int) (*qbit.Client, *qbit.Queue, error) {
	client, err := qbit.NewClient(qbit.ClientOptions{Redis: qbit.RedisOptions{
		Address:  config.RedisAddress,
		PoolSize: poolSize,
	}})
	if err != nil {
		return nil, nil, err
	}
	pingContext, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	err = client.Ping(pingContext)
	cancelPing()
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	queue, err := client.Queue(config.Queue,
		qbit.WithRetention(
			time.Duration(config.CompletedRetentionMinute)*time.Minute,
			time.Duration(config.FailedRetentionMinute)*time.Minute,
		),
	)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return client, queue, nil
}

func closeQbitClients(clients ...*qbit.Client) {
	for _, client := range clients {
		if client == nil {
			continue
		}
		_ = client.Close()
	}
}

func (app *application) start(config Config) (*simulation, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.cleanupInProgress {
		return nil, errors.New("Redis cleanup is still in progress")
	}
	if app.current != nil {
		snapshot := app.current.snapshot()
		if snapshot.Status == "starting" || snapshot.Status == "running" || snapshot.Status == "stopping" {
			return nil, errors.New("another scenario is already active")
		}
	}

	publisherClient, publisherQueue, err := newQueueClient(config, config.PublisherRedisPoolSize)
	if err != nil {
		return nil, fmt.Errorf("create publisher Redis client: %w", err)
	}
	monitorClient, monitorQueue, err := newQueueClient(config, config.MonitorRedisPoolSize)
	if err != nil {
		closeQbitClients(publisherClient)
		return nil, fmt.Errorf("create monitor Redis client: %w", err)
	}
	workerClients := make([]*qbit.Client, 0, config.WorkerReplicas)
	workerQueues := make([]*qbit.Queue, 0, config.WorkerReplicas)
	for replica := 0; replica < config.WorkerReplicas; replica++ {
		workerClient, workerQueue, workerErr := newQueueClient(config, config.WorkerRedisPoolSize)
		if workerErr != nil {
			closeQbitClients(workerClients...)
			closeQbitClients(monitorClient, publisherClient)
			return nil, fmt.Errorf("create Redis client for worker replica %d: %w", replica+1, workerErr)
		}
		workerClients = append(workerClients, workerClient)
		workerQueues = append(workerQueues, workerQueue)
	}
	statsContext, cancelStats := context.WithTimeout(context.Background(), 3*time.Second)
	baselineStats, err := monitorQueue.Stats(statsContext, 10*time.Second)
	cancelStats()
	if err != nil {
		closeQbitClients(workerClients...)
		closeQbitClients(monitorClient, publisherClient)
		return nil, err
	}
	if baselineStats.Waiting > 0 || baselineStats.Active > 0 || baselineStats.ExpiredReservations > 0 {
		closeQbitClients(workerClients...)
		closeQbitClients(monitorClient, publisherClient)
		return nil, fmt.Errorf("queue %q is not empty: waiting=%d active=%d expired=%d; use a new queue name or clean the previous run", config.Queue, baselineStats.Waiting, baselineStats.Active, baselineStats.ExpiredReservations)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	sim := &simulation{
		config:          config,
		runID:           runID,
		expectedUnique:  config.expectedUnique(),
		publisherClient: publisherClient,
		publisherQueue:  publisherQueue,
		monitorClient:   monitorClient,
		monitorQueue:    monitorQueue,
		workerClients:   workerClients,
		workerQueues:    workerQueues,
		baseline:        baselineStats.Totals,
		ctx:             ctx,
		cancel:          cancel,
		startedAt:       time.Now(),
		status:          "starting",
		activeGroups:    make(map[string]int),
		lastSequence:    make(map[string]int),
		profileCounts:   make([]profileCounters, len(config.TrafficProfiles)),
		stats:           baselineStats,
	}
	app.current = sim
	sim.addLog("info", fmt.Sprintf("Scenario %s created for queue %s", runID, config.Queue))
	sim.launch()
	return sim, nil
}

func (app *application) snapshot() Snapshot {
	app.mu.RLock()
	current := app.current
	app.mu.RUnlock()
	if current == nil {
		return Snapshot{Status: "idle", Config: defaultConfig(app.defaultRedis)}
	}
	return current.snapshot()
}

func (app *application) activeSimulation() (*simulation, error) {
	app.mu.RLock()
	defer app.mu.RUnlock()
	if app.current == nil {
		return nil, errors.New("no scenario exists")
	}
	return app.current, nil
}

func (sim *simulation) launch() {
	sim.setStatus("running")
	workers := make([]*qbit.Worker, 0, sim.config.WorkerReplicas)
	for replica := 0; replica < sim.config.WorkerReplicas; replica++ {
		worker, err := qbit.NewWorker(sim.workerQueues[replica], sim.handleJob, qbit.WorkerOptions{
			Concurrency: sim.config.WorkerConcurrency,
			Retry: qbit.RetryPolicy{
				MaxAttempts: sim.config.MaxAttempts,
				Backoff: qbit.ExponentialBackoff(
					time.Duration(sim.config.RetryInitialMillis)*time.Millisecond,
					time.Duration(sim.config.RetryMaximumMillis)*time.Millisecond,
				),
			},
			ID:              fmt.Sprintf("%s-worker-%d", sim.runID, replica+1),
			Instance:        fmt.Sprintf("fazpi-simulator-%d", replica+1),
			ShutdownTimeout: 15 * time.Second,
		})
		if err != nil {
			sim.fail(err)
			sim.finalize("failed", err.Error())
			return
		}
		workers = append(workers, worker)
	}

	sim.workerWG.Add(len(workers))
	sim.producerWG.Add(1)
	go sim.monitor()
	for replica, worker := range workers {
		go func(replicaNumber int) {
			defer sim.workerWG.Done()
			if runErr := worker.Run(sim.ctx); runErr != nil && sim.ctx.Err() == nil {
				sim.fail(fmt.Errorf("worker replica %d: %w", replicaNumber, runErr))
			}
		}(replica + 1)
	}

	go func() {
		defer sim.producerWG.Done()
		err := sim.publishAll()
		sim.stateMu.Lock()
		sim.producerDone = err == nil
		sim.producerFinishedAt = time.Now()
		sim.stateMu.Unlock()
		if err != nil {
			if sim.ctx.Err() == nil {
				sim.fail(err)
			}
			return
		}
		sim.addLog("info", "Producer finished publishing the scenario")
	}()
}

type conversationTask struct {
	profileIndex      int
	conversationIndex int64
	scheduledAt       time.Time
}

func (sim *simulation) publishAll() error {
	tasks := make(chan conversationTask, sim.config.PublisherConcurrency*2)
	errChannel := make(chan error, sim.config.PublisherConcurrency+len(sim.config.TrafficProfiles))
	started := time.Now()
	var schedulerWG sync.WaitGroup
	for profileIndex, profile := range sim.config.TrafficProfiles {
		conversations := sim.config.conversationsFor(profile)
		if conversations == 0 {
			continue
		}
		schedulerWG.Add(1)
		go func(index int, count int64) {
			defer schedulerWG.Done()
			for conversation := int64(0); conversation < count; conversation++ {
				fraction := arrivalFraction(conversation, count, sim.config.ArrivalWeights)
				target := started.Add(time.Duration(float64(sim.config.TestDurationSeconds) * float64(time.Second) * fraction))
				if err := waitUntil(sim.ctx, target); err != nil {
					return
				}
				select {
				case tasks <- conversationTask{profileIndex: index, conversationIndex: conversation + 1, scheduledAt: target}:
				case <-sim.ctx.Done():
					return
				}
			}
		}(profileIndex, conversations)
	}
	go func() {
		schedulerWG.Wait()
		close(tasks)
	}()

	var rateIndex atomic.Int64
	var publishersWG sync.WaitGroup
	for publisher := 0; publisher < sim.config.PublisherConcurrency; publisher++ {
		publishersWG.Add(1)
		go func() {
			defer publishersWG.Done()
			for task := range tasks {
				messagesPerPerson := sim.config.TrafficProfiles[task.profileIndex].MessagesPerPerson
				for sequence := 1; sequence <= messagesPerPerson; sequence++ {
					index := rateIndex.Add(1) - 1
					if err := waitForRate(sim.ctx, started, index, sim.config.PublishRate); err != nil {
						return
					}
					if err := sim.publishMessage(task, sequence); err != nil {
						select {
						case errChannel <- err:
						default:
						}
						// Preserve the first Redis/Qbit error before cancellation. Calling
						// cancel directly made the monitor classify this as a manual stop
						// and hid the real publication failure from the dashboard.
						sim.fail(err)
						return
					}
				}
			}
		}()
	}
	publishersWG.Wait()
	select {
	case err := <-errChannel:
		return err
	default:
		return sim.ctx.Err()
	}
}

func arrivalFraction(index, count int64, weights []float64) float64 {
	if count <= 0 || len(weights) == 0 {
		return 0
	}
	total := float64(0)
	for _, weight := range weights {
		total += weight
	}
	quantile := (float64(index) + 0.5) / float64(count) * total
	cumulative := float64(0)
	for bucket, weight := range weights {
		if weight == 0 {
			continue
		}
		if quantile <= cumulative+weight || bucket == len(weights)-1 {
			inside := (quantile - cumulative) / weight
			inside = min(1, max(0, inside))
			return (float64(bucket) + inside) / float64(len(weights))
		}
		cumulative += weight
	}
	return 1
}

func (sim *simulation) publishMessage(task conversationTask, sequence int) error {
	if !task.scheduledAt.IsZero() {
		delay := time.Since(task.scheduledAt)
		if delay > 0 {
			sim.publishScheduleDelayNanos.Add(delay.Nanoseconds())
			sim.publishScheduleSamples.Add(1)
			storeMaximum(&sim.maximumPublishDelayNanos, delay.Nanoseconds())
		}
	}
	profile := sim.config.TrafficProfiles[task.profileIndex]
	group := fmt.Sprintf("profile-%05d-thread-%09d", task.profileIndex+1, task.conversationIndex)
	jobID := fmt.Sprintf("run-%s-p%05d-t%09d-m%06d", sim.runID, task.profileIndex+1, task.conversationIndex, sequence)
	message := simulatedMessage{
		RunID:               sim.runID,
		Profile:             task.profileIndex,
		Account:             profile.Account,
		AgentID:             profile.Agent,
		ThreadID:            fmt.Sprintf("thread-%09d", task.conversationIndex),
		Message:             jobID,
		Sequence:            sequence,
		PublishedAtUnixNano: time.Now().UnixNano(),
	}
	payload, err := makePayload(message, sim.config.PayloadBytes)
	if err != nil {
		return err
	}
	sim.counts.PublishAttempts.Add(1)
	job, err := sim.publisherQueue.Publish(sim.ctx, "process-message", payload, qbit.WithGroup(group), qbit.WithJobID(jobID))
	if err != nil {
		sim.counts.PublishErrors.Add(1)
		return fmt.Errorf("publish %s: %w", jobID, err)
	}
	if job.Duplicate {
		sim.counts.DuplicatesConfirmed.Add(1)
	} else {
		sim.counts.PublishedUnique.Add(1)
		profileCounts := &sim.profileCounts[task.profileIndex]
		published := profileCounts.published.Add(1)
		terminal := profileCounts.completed.Load() + profileCounts.permanent.Load()
		storeMaximum(&profileCounts.maxBacklog, published-terminal)
	}

	if selected(jobID, sim.config.DuplicatePublishPercent, 0) {
		sim.counts.DuplicateAttempts.Add(1)
		sim.counts.PublishAttempts.Add(1)
		duplicate, duplicateErr := sim.publisherQueue.Publish(sim.ctx, "process-message", payload, qbit.WithGroup(group), qbit.WithJobID(jobID))
		if duplicateErr != nil {
			sim.counts.PublishErrors.Add(1)
			return fmt.Errorf("duplicate publish %s: %w", jobID, duplicateErr)
		}
		if duplicate.Duplicate {
			sim.counts.DuplicatesConfirmed.Add(1)
		}
	}
	return nil
}

func (sim *simulation) handleJob(ctx context.Context, job *qbit.Job) error {
	sim.counts.HandlerAttempts.Add(1)
	var message simulatedMessage
	if err := json.Unmarshal(job.Payload, &message); err != nil {
		return qbit.Permanent(fmt.Errorf("decode simulated message: %w", err))
	}
	if message.Profile < 0 || message.Profile >= len(sim.profileCounts) {
		return qbit.Permanent(fmt.Errorf("invalid traffic profile index %d", message.Profile))
	}
	profileCounts := &sim.profileCounts[message.Profile]
	profileCounts.handlerAttempts.Add(1)
	active := profileCounts.active.Add(1)
	storeMaximum(&profileCounts.maxActive, active)
	defer profileCounts.active.Add(-1)
	if job.Attempts == 1 && message.PublishedAtUnixNano > 0 {
		wait := time.Since(time.Unix(0, message.PublishedAtUnixNano))
		if wait > 0 {
			profileCounts.waitNanos.Add(wait.Nanoseconds())
			profileCounts.waitSamples.Add(1)
			storeMaximum(&profileCounts.maxWaitNanos, wait.Nanoseconds())
		}
	}

	sim.validationMu.Lock()
	sim.activeGroups[job.Group]++
	if sim.activeGroups[job.Group] > 1 {
		sim.counts.ConcurrencyViolations.Add(1)
	}
	expectedSequence := sim.lastSequence[job.Group] + 1
	if message.Sequence != expectedSequence {
		sim.counts.OrderingViolations.Add(1)
	}
	sim.validationMu.Unlock()
	defer func() {
		sim.validationMu.Lock()
		sim.activeGroups[job.Group]--
		sim.validationMu.Unlock()
	}()

	processing := sim.config.ProcessingMillis
	if sim.config.ProcessingJitterMillis > 0 {
		processing += int(hashValue(job.ID+"-jitter") % uint32(sim.config.ProcessingJitterMillis+1))
	}
	if processing > 0 {
		timer := time.NewTimer(time.Duration(processing) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}

	if selected(job.ID, sim.config.PermanentFailurePercent, 0) {
		sim.counts.PermanentFailures.Add(1)
		profileCounts.permanent.Add(1)
		sim.markTerminal(job.Group, message.Sequence)
		return qbit.Permanent(errors.New("simulated permanent failure"))
	}
	if selected(job.ID, sim.config.TransientFailurePercent, sim.config.PermanentFailurePercent) && job.Attempts == 1 {
		sim.counts.TransientFailures.Add(1)
		profileCounts.transient.Add(1)
		return errors.New("simulated transient failure")
	}

	sim.markTerminal(job.Group, message.Sequence)
	sim.counts.Completed.Add(1)
	profileCounts.completed.Add(1)
	return nil
}

func (sim *simulation) markTerminal(group string, sequence int) {
	sim.validationMu.Lock()
	defer sim.validationMu.Unlock()
	sim.lastSequence[group] = sequence
}

func (sim *simulation) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sim.refreshObservability()
			counts := sim.counts.snapshot()
			sim.stateMu.RLock()
			producerDone := sim.producerDone
			stats := sim.stats
			sim.stateMu.RUnlock()
			terminal := counts.Completed + counts.PermanentFailures
			if producerDone && terminal == sim.expectedUnique && stats.Waiting == 0 && stats.Active == 0 && stats.ExpiredReservations == 0 {
				sim.finalize("completed", "")
				return
			}
		case <-sim.ctx.Done():
			sim.refreshObservability()
			sim.stateMu.RLock()
			failure := sim.errorMessage
			requested := sim.stopRequested
			sim.stateMu.RUnlock()
			if failure != "" {
				sim.finalize("failed", failure)
			} else if requested {
				sim.finalize("stopped", "")
			} else {
				sim.finalize("stopped", "")
			}
			return
		}
	}
}

func (sim *simulation) refreshObservability() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	stats, err := sim.monitorQueue.Stats(ctx, 10*time.Second)
	if err == nil {
		events, eventsErr := sim.monitorQueue.RecentEvents(ctx, 30)
		sim.stateMu.Lock()
		sim.stats = stats
		sim.samples = append(sim.samples, Sample{
			At:            time.Now().UTC(),
			Waiting:       stats.Waiting,
			Active:        stats.Active,
			Expired:       stats.ExpiredReservations,
			PublishedRate: stats.Rates.Published,
			CompletedRate: stats.Rates.Completed,
		})
		if len(sim.samples) > 180 {
			sim.samples = append([]Sample(nil), sim.samples[len(sim.samples)-180:]...)
		}
		if eventsErr == nil {
			sim.events = events
		}
		sim.stateMu.Unlock()
	}
	cancel()
	if err != nil && sim.ctx.Err() == nil {
		sim.addLog("warning", "Unable to refresh Redis statistics: "+err.Error())
	}
}

func (sim *simulation) fail(err error) {
	if err == nil {
		return
	}
	sim.stateMu.Lock()
	if sim.errorMessage == "" {
		sim.errorMessage = err.Error()
		sim.logs = append(sim.logs, LogEntry{At: time.Now().UTC(), Level: "error", Message: err.Error()})
	}
	sim.stateMu.Unlock()
	sim.cancel()
}

func (sim *simulation) stop() {
	sim.stateMu.Lock()
	if sim.status == "running" || sim.status == "starting" {
		sim.status = "stopping"
		sim.stopRequested = true
	}
	sim.stateMu.Unlock()
	sim.cancel()
}

func (sim *simulation) finalize(status, message string) {
	sim.finishOnce.Do(func() {
		sim.cancel()
		sim.producerWG.Wait()
		sim.workerWG.Wait()
		sim.refreshObservability()
		closeQbitClients(sim.workerClients...)
		closeQbitClients(sim.monitorClient, sim.publisherClient)
		sim.stateMu.Lock()
		sim.status = status
		sim.errorMessage = message
		sim.finishedAt = time.Now()
		sim.logs = append(sim.logs, LogEntry{At: time.Now().UTC(), Level: "info", Message: "Scenario finished with status " + status})
		sim.stateMu.Unlock()
	})
}

func (sim *simulation) pause() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sim.monitorQueue.Pause(ctx); err != nil {
		return err
	}
	sim.addLog("info", "Queue paused")
	return nil
}

func (sim *simulation) resume() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sim.monitorQueue.Resume(ctx); err != nil {
		return err
	}
	sim.addLog("info", "Queue resumed")
	return nil
}

func (sim *simulation) setStatus(status string) {
	sim.stateMu.Lock()
	sim.status = status
	sim.stateMu.Unlock()
}

func (sim *simulation) addLog(level, message string) {
	sim.stateMu.Lock()
	sim.logs = append(sim.logs, LogEntry{At: time.Now().UTC(), Level: level, Message: message})
	if len(sim.logs) > 100 {
		sim.logs = append([]LogEntry(nil), sim.logs[len(sim.logs)-100:]...)
	}
	sim.stateMu.Unlock()
}

func (sim *simulation) snapshot() Snapshot {
	sim.stateMu.RLock()
	status := sim.status
	errorMessage := sim.errorMessage
	finishedAt := sim.finishedAt
	producerDone := sim.producerDone
	stats := sim.stats
	samples := append([]Sample(nil), sim.samples...)
	logs := append([]LogEntry(nil), sim.logs...)
	events := append([]qbit.Event(nil), sim.events...)
	sim.stateMu.RUnlock()
	counts := sim.counts.snapshot()
	elapsedEnd := time.Now()
	if !finishedAt.IsZero() {
		elapsedEnd = finishedAt
	}
	return Snapshot{
		RunID:          sim.runID,
		Status:         status,
		Error:          errorMessage,
		Config:         sim.config,
		ExpectedUnique: sim.expectedUnique,
		StartedAt:      sim.startedAt.UTC(),
		FinishedAt:     finishedAt.UTC(),
		ElapsedSeconds: elapsedEnd.Sub(sim.startedAt).Seconds(),
		ProducerDone:   producerDone,
		Counters:       counts,
		Profiles:       sim.profileSnapshots(),
		Capacity:       sim.capacityPlan(),
		RedisPools:     sim.redisPoolsSnapshot(),
		Publication:    sim.publicationTiming(producerDone),
		QueueStats:     stats,
		QueueTotals:    subtractCounts(stats.Totals, sim.baseline),
		Samples:        samples,
		Logs:           logs,
		Events:         events,
		Validations:    sim.validations(status, producerDone, counts, stats),
	}
}

func (sim *simulation) profileSnapshots() []ProfileSnapshot {
	result := make([]ProfileSnapshot, 0, len(sim.config.TrafficProfiles))
	for index, profile := range sim.config.TrafficProfiles {
		values := &sim.profileCounts[index]
		published := values.published.Load()
		completed := values.completed.Load()
		permanent := values.permanent.Load()
		waitSamples := values.waitSamples.Load()
		averageWait := float64(0)
		if waitSamples > 0 {
			averageWait = float64(values.waitNanos.Load()) / float64(waitSamples) / float64(time.Millisecond)
		}
		result = append(result, ProfileSnapshot{
			Account:               profile.Account,
			Agent:                 profile.Agent,
			PeoplePerHour:         profile.PeoplePerHour,
			MessagesPerPerson:     profile.MessagesPerPerson,
			ExpectedConversations: sim.config.conversationsFor(profile),
			ExpectedMessages:      sim.config.conversationsFor(profile) * int64(profile.MessagesPerPerson),
			Published:             published,
			HandlerAttempts:       values.handlerAttempts.Load(),
			Completed:             completed,
			PermanentFailures:     permanent,
			TransientFailures:     values.transient.Load(),
			Active:                values.active.Load(),
			CurrentBacklog:        max(0, published-completed-permanent),
			PeakBacklog:           values.maxBacklog.Load(),
			AverageQueueWaitMS:    averageWait,
			MaximumQueueWaitMS:    float64(values.maxWaitNanos.Load()) / float64(time.Millisecond),
		})
	}
	return result
}

func (sim *simulation) capacityPlan() CapacityPlan {
	target := float64(sim.expectedUnique) / float64(sim.config.TestDurationSeconds)
	averageProcessingSeconds := float64(sim.config.ProcessingMillis)/1000 + float64(sim.config.ProcessingJitterMillis)/2000
	slots := sim.config.WorkerReplicas * sim.config.WorkerConcurrency
	capacity := float64(0)
	required := 0
	margin := float64(0)
	if averageProcessingSeconds > 0 {
		capacity = float64(slots) / averageProcessingSeconds
		required = int(target*averageProcessingSeconds + 0.999999)
		if target > 0 {
			margin = capacity / target
		}
	}
	return CapacityPlan{
		TargetMessagesPerSecond: target,
		EstimatedWorkerCapacity: capacity,
		EstimatedRequiredSlots:  required,
		ConfiguredSlots:         slots,
		EstimatedCapacityMargin: margin,
	}
}

func redisPoolUsage(configured int, clients ...*qbit.Client) RedisPoolUsage {
	usage := RedisPoolUsage{Configured: configured}
	for _, client := range clients {
		stats := client.PoolStats()
		usage.TotalConnections += uint64(stats.TotalConnections)
		usage.IdleConnections += uint64(stats.IdleConnections)
		usage.Hits += uint64(stats.Hits)
		usage.Misses += uint64(stats.Misses)
		usage.WaitCount += uint64(stats.WaitCount)
		usage.WaitDurationMS += float64(stats.WaitDuration) / float64(time.Millisecond)
		usage.Timeouts += uint64(stats.Timeouts)
	}
	if usage.TotalConnections >= usage.IdleConnections {
		usage.InUseConnections = usage.TotalConnections - usage.IdleConnections
	}
	return usage
}

func (sim *simulation) redisPoolsSnapshot() RedisPoolsSnapshot {
	plan := sim.config.redisPoolPlan()
	return RedisPoolsSnapshot{
		Plan:      plan,
		Publisher: redisPoolUsage(plan.Publisher, sim.publisherClient),
		Workers:   redisPoolUsage(plan.WorkersTotal, sim.workerClients...),
		Monitor:   redisPoolUsage(plan.Monitor, sim.monitorClient),
	}
}

func publicationWindowTolerance(duration time.Duration) time.Duration {
	tolerance := duration / 100
	if tolerance < 2*time.Second {
		return 2 * time.Second
	}
	if tolerance > 30*time.Second {
		return 30 * time.Second
	}
	return tolerance
}

func (sim *simulation) publicationTiming(producerDone bool) PublicationTiming {
	target := time.Duration(sim.config.TestDurationSeconds) * time.Second
	tolerance := publicationWindowTolerance(target)
	sim.stateMu.RLock()
	finishedAt := sim.producerFinishedAt
	sim.stateMu.RUnlock()
	elapsed := time.Duration(0)
	if !sim.startedAt.IsZero() {
		end := time.Now()
		if !finishedAt.IsZero() {
			end = finishedAt
		}
		elapsed = end.Sub(sim.startedAt)
		if elapsed < 0 {
			elapsed = 0
		}
	}
	samples := sim.publishScheduleSamples.Load()
	averageDelay := float64(0)
	if samples > 0 {
		averageDelay = float64(sim.publishScheduleDelayNanos.Load()) / float64(samples) / float64(time.Millisecond)
	}
	maximumDelay := time.Duration(sim.maximumPublishDelayNanos.Load())
	return PublicationTiming{
		TargetDurationSeconds:    target.Seconds(),
		ToleranceSeconds:         tolerance.Seconds(),
		ProducerElapsedSeconds:   elapsed.Seconds(),
		AverageScheduleDelayMS:   averageDelay,
		MaximumScheduleDelayMS:   float64(maximumDelay) / float64(time.Millisecond),
		PublishedWithinTolerance: producerDone && elapsed <= target+tolerance && maximumDelay <= tolerance,
	}
}

func (sim *simulation) validations(status string, producerDone bool, counts CounterSnapshot, stats qbit.Stats) []Validation {
	finished := status == "completed"
	slaPassed, slaExplanation := sim.queueWaitSLA()
	publication := sim.publicationTiming(producerDone)
	result := []Validation{
		invariantValidation("Orden FIFO por thread", counts.OrderingViolations == 0, finished,
			fmt.Sprintf("%d violaciones detectadas", counts.OrderingViolations)),
		invariantValidation("Un solo mensaje activo por thread", counts.ConcurrencyViolations == 0, finished,
			fmt.Sprintf("%d solapamientos detectados", counts.ConcurrencyViolations)),
		completionValidation("Publicación idempotente", counts.DuplicateAttempts == counts.DuplicatesConfirmed, producerDone,
			fmt.Sprintf("%d de %d duplicados confirmados", counts.DuplicatesConfirmed, counts.DuplicateAttempts)),
		invariantValidation("Sin errores de publicación", counts.PublishErrors == 0, finished,
			fmt.Sprintf("%d errores de publicación", counts.PublishErrors)),
		completionValidation("Todos los mensajes terminaron", counts.Completed+counts.PermanentFailures == sim.expectedUnique, finished,
			fmt.Sprintf("%d de %d terminales", counts.Completed+counts.PermanentFailures, sim.expectedUnique)),
		completionValidation("Cola drenada", stats.Waiting == 0 && stats.Active == 0 && stats.ExpiredReservations == 0, finished,
			fmt.Sprintf("%d esperando, %d activos y %d reservas vencidas", stats.Waiting, stats.Active, stats.ExpiredReservations)),
		invariantValidation("SLA de espera por agente", slaPassed, finished, slaExplanation),
		completionValidation("Publicación dentro de la ventana", publication.PublishedWithinTolerance, producerDone,
			fmt.Sprintf("productor %.1f s; objetivo %.1f s + %.1f s de tolerancia; retraso máximo %.1f ms",
				publication.ProducerElapsedSeconds, publication.TargetDurationSeconds, publication.ToleranceSeconds, publication.MaximumScheduleDelayMS)),
	}
	if status == "failed" {
		for index := range result {
			if result[index].Status == "pending" {
				result[index].Status = "failed"
			}
		}
	}
	return result
}

func (sim *simulation) queueWaitSLA() (bool, string) {
	worstWait := float64(0)
	worstAgent := "sin muestras"
	for _, profile := range sim.profileSnapshots() {
		if profile.MaximumQueueWaitMS > worstWait {
			worstWait = profile.MaximumQueueWaitMS
			worstAgent = profile.Account + " / " + profile.Agent
		}
	}
	return worstWait <= float64(sim.config.QueueWaitSLAms), fmt.Sprintf("máximo %.1f ms en %s; SLA %d ms", worstWait, worstAgent, sim.config.QueueWaitSLAms)
}

func invariantValidation(name string, condition, finished bool, explanation string) Validation {
	status := "pending"
	if !condition {
		status = "failed"
	} else if finished {
		status = "passed"
	}
	return Validation{Name: name, Status: status, Explanation: explanation}
}

func completionValidation(name string, condition, ready bool, explanation string) Validation {
	status := "pending"
	if ready {
		if condition {
			status = "passed"
		} else {
			status = "failed"
		}
	}
	return Validation{Name: name, Status: status, Explanation: explanation}
}

func subtractCounts(current, baseline qbit.LifecycleCounts) qbit.LifecycleCounts {
	return qbit.LifecycleCounts{
		Published: current.Published - baseline.Published,
		Reserved:  current.Reserved - baseline.Reserved,
		Completed: current.Completed - baseline.Completed,
		Failed:    current.Failed - baseline.Failed,
		Stalled:   current.Stalled - baseline.Stalled,
		Retried:   current.Retried - baseline.Retried,
		Recovered: current.Recovered - baseline.Recovered,
	}
}

func makePayload(message simulatedMessage, targetBytes int) ([]byte, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if targetBytes > len(payload) {
		payload = append(payload, []byte(strings.Repeat(" ", targetBytes-len(payload)))...)
	}
	return payload, nil
}

func waitForRate(ctx context.Context, started time.Time, index int64, rate float64) error {
	if rate == 0 {
		return nil
	}
	target := started.Add(time.Duration(float64(index) / rate * float64(time.Second)))
	wait := time.Until(target)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitUntil(ctx context.Context, target time.Time) error {
	wait := time.Until(target)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func hashValue(value string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(value))
	return hash.Sum32()
}

func selected(id string, percent, offset float64) bool {
	if percent <= 0 {
		return false
	}
	bucket := float64(hashValue(id)%10_000) / 100
	return bucket >= offset && bucket < offset+percent
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func decodeConfig(request *http.Request) (Config, error) {
	defer request.Body.Close()
	var config Config
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func decodeCleanupRequest(request *http.Request) (cleanupRequest, error) {
	defer request.Body.Close()
	var value cleanupRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, 4<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return cleanupRequest{}, err
	}
	value.Queue = strings.TrimSpace(value.Queue)
	value.RedisAddress = strings.TrimSpace(value.RedisAddress)
	if value.Queue == "" {
		return cleanupRequest{}, errors.New("queue is required")
	}
	if value.RedisAddress == "" {
		return cleanupRequest{}, errors.New("Redis address is required")
	}
	return value, nil
}

func isLoopbackRedisAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") || strings.EqualFold(host, "host.docker.internal") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// redisScanLiteral escapes Redis glob metacharacters so cleanup remains scoped
// to the exact queue selected in the laboratory.
func redisScanLiteral(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`*`, `\*`,
		`?`, `\?`,
		`[`, `\[`,
		`]`, `\]`,
	)
	return replacer.Replace(value)
}

func purgeQueueData(ctx context.Context, redisAddress, queue string) (int64, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         redisAddress,
		PoolSize:     4,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return 0, fmt.Errorf("connect to Redis for cleanup: %w", err)
	}

	pattern := "qbit:{" + redisScanLiteral(queue) + "}:*"
	var unlinked int64
	// SCAN can move across buckets while UNLINK removes keys. Repeat complete
	// passes until one finds nothing, which is safe because terminal laboratory
	// runs no longer have producers or workers writing to this queue.
	for {
		var cursor uint64
		var passUnlinked int64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 1_000).Result()
			if err != nil {
				return unlinked, fmt.Errorf("scan queue %q: %w", queue, err)
			}
			if len(keys) > 0 {
				removed, unlinkErr := client.Unlink(ctx, keys...).Result()
				if unlinkErr != nil {
					return unlinked, fmt.Errorf("unlink queue %q: %w", queue, unlinkErr)
				}
				passUnlinked += removed
				unlinked += removed
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if passUnlinked == 0 {
			break
		}
	}
	if err := client.SRem(ctx, "qbit:registry:{queues}:names", queue).Err(); err != nil {
		return unlinked, fmt.Errorf("remove queue %q from registry: %w", queue, err)
	}
	return unlinked, nil
}

func (app *application) cleanup(redisAddress, queue string) (cleanupResult, error) {
	if !isLoopbackRedisAddress(redisAddress) {
		return cleanupResult{}, errors.New("laboratory cleanup is restricted to a loopback Redis address")
	}
	app.mu.Lock()
	if app.cleanupInProgress {
		app.mu.Unlock()
		return cleanupResult{}, errors.New("Redis cleanup is already in progress")
	}
	current := app.current
	if current != nil {
		snapshot := current.snapshot()
		if snapshot.Status == "starting" || snapshot.Status == "running" || snapshot.Status == "stopping" {
			app.mu.Unlock()
			return cleanupResult{}, errors.New("stop the current scenario and wait for it to finish before cleaning Redis")
		}
	}
	app.cleanupInProgress = true
	app.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	keys, err := purgeQueueData(ctx, redisAddress, queue)

	app.mu.Lock()
	app.cleanupInProgress = false
	if err == nil && app.current == current && current != nil {
		snapshot := current.snapshot()
		if snapshot.Config.RedisAddress == redisAddress && snapshot.Config.Queue == queue {
			app.current = nil
		}
	}
	app.mu.Unlock()
	if err != nil {
		return cleanupResult{}, err
	}
	return cleanupResult{Queue: queue, KeysUnlinked: keys}, nil
}

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/defaults", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, defaultConfig(app.defaultRedis))
	})
	mux.HandleFunc("GET /api/state", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, app.snapshot())
	})
	mux.HandleFunc("POST /api/start", func(writer http.ResponseWriter, request *http.Request) {
		config, err := decodeConfig(request)
		if err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		sim, err := app.start(config)
		if err != nil {
			writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusCreated, sim.snapshot())
	})
	mux.HandleFunc("POST /api/stop", func(writer http.ResponseWriter, _ *http.Request) {
		sim, err := app.activeSimulation()
		if err != nil {
			writeJSON(writer, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		sim.stop()
		writeJSON(writer, http.StatusAccepted, map[string]string{"status": "stopping"})
	})
	mux.HandleFunc("POST /api/cleanup", func(writer http.ResponseWriter, request *http.Request) {
		value, err := decodeCleanupRequest(request)
		if err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		result, err := app.cleanup(value.RedisAddress, value.Queue)
		if err != nil {
			writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, result)
	})
	for path, action := range map[string]func(*simulation) error{
		"POST /api/pause":  (*simulation).pause,
		"POST /api/resume": (*simulation).resume,
	} {
		mux.HandleFunc(path, func(writer http.ResponseWriter, _ *http.Request) {
			sim, err := app.activeSimulation()
			if err == nil {
				err = action(sim)
			}
			if err != nil {
				writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
		})
	}

	assets, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(assets)))
	return mux
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func main() {
	listenAddress := flag.String("listen", envOr("FAZPI_LOADTEST_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	redisAddress := flag.String("redis", envOr("QBIT_REDIS_ADDR", "127.0.0.1:6379"), "default Redis address")
	flag.Parse()

	app := newApplication(*redisAddress)
	server := &http.Server{Addr: *listenAddress, Handler: app.routes(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		if sim, err := app.activeSimulation(); err == nil {
			sim.stop()
		}
		shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()

	log.Printf("Fazpi Qbit load tester available at http://%s", *listenAddress)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
