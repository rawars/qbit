// Package qbit provides Redis-backed grouped job queues for Go applications.
//
// This is Qbit's stable public import path. Jobs in the same group are handled
// in order, while independent groups can be processed concurrently across
// worker replicas.
package qbit

import (
	"context"
	"time"

	core "github.com/rawars/qbit/packages/go/qbit"
	"github.com/redis/go-redis/v9"
)

type (
	RedisOptions       = core.RedisOptions
	RedisPoolStats     = core.RedisPoolStats
	ClientOptions      = core.ClientOptions
	Client             = core.Client
	Queue              = core.Queue
	QueueOption        = core.QueueOption
	Job                = core.Job
	AddOption          = core.AddOption
	PublishOption      = core.PublishOption
	ReserveOption      = core.ReserveOption
	WorkerOption       = core.WorkerOption
	WorkerInfo         = core.WorkerInfo
	WorkerRegistration = core.WorkerRegistration
	Handler            = core.Handler
	Backoff            = core.Backoff
	RetryPolicy        = core.RetryPolicy
	WorkerOptions      = core.WorkerOptions
	Worker             = core.Worker
	LifecycleCounts    = core.LifecycleCounts
	LifecycleRates     = core.LifecycleRates
	Stats              = core.Stats
	Event              = core.Event
)

var (
	ErrNoJob                  = core.ErrNoJob
	ErrReservationLost        = core.ErrReservationLost
	ErrQueuePaused            = core.ErrQueuePaused
	ErrWorkerRegistrationLost = core.ErrWorkerRegistrationLost
	ErrWorkerAlreadyRunning   = core.ErrWorkerAlreadyRunning
	ErrWorkerShutdownTimeout  = core.ErrWorkerShutdownTimeout
)

// NewClient creates a client that owns its Redis connection pool.
func NewClient(options ClientOptions) (*Client, error) {
	return core.NewClient(options)
}

// NewQueue constructs a queue from an existing go-redis client. Most
// applications should create a Client and call Client.Queue instead.
func NewQueue(name string, client redis.UniversalClient, options ...QueueOption) (*Queue, error) {
	return core.NewQueue(name, client, options...)
}

// NewWorker creates a managed concurrent worker.
func NewWorker(queue *Queue, handler Handler, options WorkerOptions) (*Worker, error) {
	return core.NewWorker(queue, handler, options)
}

// WithGroup keeps related jobs ordered while allowing different groups to run
// concurrently.
func WithGroup(group string) AddOption { return core.WithGroup(group) }

// WithJobID makes publishing idempotent for the supplied ID.
func WithJobID(id string) AddOption { return core.WithJobID(id) }

// WithRetention configures retention of completed and failed jobs.
func WithRetention(completed, failed time.Duration) QueueOption {
	return core.WithRetention(completed, failed)
}

// WithLockTTL configures a low-level reservation lock.
func WithLockTTL(ttl time.Duration) ReserveOption { return core.WithLockTTL(ttl) }

// WithWorkerID configures a low-level worker registration ID.
func WithWorkerID(id string) WorkerOption { return core.WithWorkerID(id) }

// WithWorkerInstance configures the human-readable instance name shown in
// observability data.
func WithWorkerInstance(instance string) WorkerOption {
	return core.WithWorkerInstance(instance)
}

// WithWorkerTTL configures a low-level worker registration lease.
func WithWorkerTTL(ttl time.Duration) WorkerOption { return core.WithWorkerTTL(ttl) }

// Permanent marks a handler error as non-retryable.
func Permanent(err error) error { return core.Permanent(err) }

// IsPermanent reports whether an error was marked with Permanent.
func IsPermanent(err error) bool { return core.IsPermanent(err) }

// FixedBackoff returns a fixed retry delay.
func FixedBackoff(delay time.Duration) Backoff { return core.FixedBackoff(delay) }

// ExponentialBackoff returns a capped exponential retry delay.
func ExponentialBackoff(initial, maximum time.Duration) Backoff {
	return core.ExponentialBackoff(initial, maximum)
}

// HandlerFunc adapts a function to Handler. It exists mainly to make API
// intent explicit in generated documentation.
func HandlerFunc(handler func(context.Context, *Job) error) Handler { return Handler(handler) }
