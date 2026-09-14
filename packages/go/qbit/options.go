package qbit

import "time"

type addOptions struct {
	group    string
	jobID    string
	hasJobID bool
}

// AddOption configures a job when it is added to a queue.
type AddOption func(*addOptions)

// PublishOption customizes a published job. It aliases AddOption so callers
// can migrate from Add to Publish without changing existing options.
type PublishOption = AddOption

// WithGroup assigns a job to a FIFO processing group.
func WithGroup(group string) AddOption {
	return func(options *addOptions) { options.group = group }
}

// WithJobID makes Add idempotent. Reusing an ID returns the original job
// instead of creating another one.
func WithJobID(id string) AddOption {
	return func(options *addOptions) {
		options.jobID = id
		options.hasJobID = true
	}
}

type queueOptions struct {
	completedRetention time.Duration
	failedRetention    time.Duration
}

// QueueOption configures queue-wide behavior.
type QueueOption func(*queueOptions)

// WithRetention controls how long terminal job records remain available.
func WithRetention(completed, failed time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.completedRetention = completed
		options.failedRetention = failed
	}
}

type reserveOptions struct{ lockTTL time.Duration }

// ReserveOption configures a reservation.
type ReserveOption func(*reserveOptions)

// WithLockTTL sets how long a reservation is owned by a consumer.
func WithLockTTL(ttl time.Duration) ReserveOption {
	return func(options *reserveOptions) { options.lockTTL = ttl }
}

type workerOptions struct {
	id       string
	instance string
	ttl      time.Duration
}

// WorkerOption configures a worker registration.
type WorkerOption func(*workerOptions)

// WithWorkerID provides a stable identifier for a worker process. Qbit creates
// a random identifier when this option is omitted.
func WithWorkerID(id string) WorkerOption {
	return func(options *workerOptions) { options.id = id }
}

// WithWorkerInstance sets a human-readable instance name, normally a pod or
// host name.
func WithWorkerInstance(instance string) WorkerOption {
	return func(options *workerOptions) { options.instance = instance }
}

// WithWorkerTTL controls how quickly a worker disappears from telemetry when
// its heartbeat stops.
func WithWorkerTTL(ttl time.Duration) WorkerOption {
	return func(options *workerOptions) { options.ttl = ttl }
}
