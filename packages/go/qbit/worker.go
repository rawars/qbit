package qbit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	// ErrWorkerAlreadyRunning is returned when Run is called concurrently on
	// the same Worker.
	ErrWorkerAlreadyRunning = errors.New("qbit: worker is already running")

	// ErrWorkerShutdownTimeout is returned when handlers do not stop before the
	// configured graceful shutdown deadline.
	ErrWorkerShutdownTimeout = errors.New("qbit: worker graceful shutdown timed out")
)

// Handler processes one reserved job. Returning nil completes it, returning a
// regular error retries it according to RetryPolicy, and returning Permanent
// marks it as failed without another retry.
type Handler func(context.Context, *Job) error

// Backoff calculates the delay before a failed attempt is retried. attempt is
// one-based: the first failed attempt is 1.
type Backoff func(attempt int) time.Duration

// RetryPolicy controls handler retries.
type RetryPolicy struct {
	// MaxAttempts includes the first execution. Zero uses the default of 3.
	MaxAttempts int
	Backoff     Backoff
}

// WorkerOptions configures a high-level worker replica.
type WorkerOptions struct {
	// Concurrency is the number of jobs this process can handle simultaneously.
	// Zero uses one slot.
	Concurrency int
	Retry       RetryPolicy

	// LockTTL controls reservation ownership. RenewInterval must be shorter
	// than LockTTL. Zero values use 30s and one third of LockTTL.
	LockTTL       time.Duration
	RenewInterval time.Duration
	ReserveWait   time.Duration

	// ShutdownTimeout is how long Run waits for in-flight handlers after its
	// context is cancelled. Handlers should observe their context.
	ShutdownTimeout time.Duration

	// ID and Instance identify this replica in Prometheus/Grafana. Empty values
	// are generated automatically.
	ID              string
	Instance        string
	RegistrationTTL time.Duration
}

// Worker reserves, renews, processes, retries, and completes jobs.
type Worker struct {
	queue   *Queue
	handler Handler
	options WorkerOptions

	lifecycleMu sync.Mutex
	cancelRun   context.CancelFunc
	runDone     chan struct{}
}

type permanentFailure struct {
	err error
}

func (e permanentFailure) Error() string { return e.err.Error() }
func (e permanentFailure) Unwrap() error { return e.err }
func (e permanentFailure) permanent()    {}

// Permanent marks a handler error as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentFailure{err: err}
}

// IsPermanent reports whether err, or an error in its chain, was marked with
// Permanent.
func IsPermanent(err error) bool {
	var target interface{ permanent() }
	return errors.As(err, &target)
}

// FixedBackoff returns a retry policy that always waits delay.
func FixedBackoff(delay time.Duration) Backoff {
	if delay < 0 {
		delay = 0
	}
	return func(int) time.Duration { return delay }
}

// ExponentialBackoff doubles initial after each failed attempt and caps it at
// maximum. Negative values are treated as zero.
func ExponentialBackoff(initial, maximum time.Duration) Backoff {
	if initial < 0 {
		initial = 0
	}
	if maximum < initial {
		maximum = initial
	}
	return func(attempt int) time.Duration {
		if attempt <= 1 || initial == 0 {
			return initial
		}
		shift := attempt - 1
		if shift >= 63 || initial > time.Duration(math.MaxInt64>>shift) {
			return maximum
		}
		delay := initial << shift
		if delay > maximum {
			return maximum
		}
		return delay
	}
}

// NewWorker creates a worker for queue. Call Run to begin processing.
func NewWorker(queue *Queue, handler Handler, options WorkerOptions) (*Worker, error) {
	if queue == nil {
		return nil, errors.New("qbit: worker queue is required")
	}
	if handler == nil {
		return nil, errors.New("qbit: worker handler is required")
	}

	normalized, err := normalizeWorkerOptions(options)
	if err != nil {
		return nil, err
	}
	return &Worker{queue: queue, handler: handler, options: normalized}, nil
}

func normalizeWorkerOptions(options WorkerOptions) (WorkerOptions, error) {
	if options.Concurrency < 0 {
		return options, errors.New("qbit: worker concurrency cannot be negative")
	}
	if options.Concurrency == 0 {
		options.Concurrency = 1
	}
	if options.Retry.MaxAttempts < 0 {
		return options, errors.New("qbit: maximum attempts cannot be negative")
	}
	if options.Retry.MaxAttempts == 0 {
		options.Retry.MaxAttempts = 3
	}
	if options.Retry.Backoff == nil {
		options.Retry.Backoff = FixedBackoff(0)
	}
	if options.LockTTL < 0 {
		return options, errors.New("qbit: lock TTL cannot be negative")
	}
	if options.LockTTL == 0 {
		options.LockTTL = 30 * time.Second
	}
	if options.RenewInterval < 0 {
		return options, errors.New("qbit: renewal interval cannot be negative")
	}
	if options.RenewInterval == 0 {
		options.RenewInterval = options.LockTTL / 3
	}
	if options.RenewInterval <= 0 || options.RenewInterval >= options.LockTTL {
		return options, errors.New("qbit: renewal interval must be greater than zero and shorter than lock TTL")
	}
	if options.ReserveWait < 0 {
		return options, errors.New("qbit: reserve wait cannot be negative")
	}
	if options.ReserveWait == 0 {
		options.ReserveWait = time.Second
	}
	if options.ShutdownTimeout < 0 {
		return options, errors.New("qbit: shutdown timeout cannot be negative")
	}
	if options.ShutdownTimeout == 0 {
		options.ShutdownTimeout = 30 * time.Second
	}
	if options.RegistrationTTL < 0 {
		return options, errors.New("qbit: registration TTL cannot be negative")
	}
	if options.RegistrationTTL == 0 {
		options.RegistrationTTL = defaultWorkerTTL
	}
	if options.RegistrationTTL < 3*time.Second {
		return options, errors.New("qbit: registration TTL must be at least 3 seconds")
	}
	return options, nil
}

// Run blocks while the worker consumes jobs. Cancelling ctx stops new
// reservations and waits for in-flight handlers up to ShutdownTimeout. A clean
// cancellation returns nil.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("qbit: worker context is required")
	}
	runCtx, cancelRun, err := w.beginRun(ctx)
	if err != nil {
		return err
	}
	defer cancelRun()
	defer w.finishRun()

	registrationOptions := []WorkerOption{WithWorkerTTL(w.options.RegistrationTTL)}
	if w.options.ID != "" {
		registrationOptions = append(registrationOptions, WithWorkerID(w.options.ID))
	}
	if w.options.Instance != "" {
		registrationOptions = append(registrationOptions, WithWorkerInstance(w.options.Instance))
	}
	registration, err := w.queue.RegisterWorker(runCtx, w.options.Concurrency, registrationOptions...)
	if err != nil {
		if runCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("qbit: register worker: %w", err)
	}

	processingCtx, cancelProcessing := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelProcessing()
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	var firstErr error
	var firstErrOnce sync.Once
	reportFatal := func(fatalErr error) {
		if fatalErr == nil {
			return
		}
		firstErrOnce.Do(func() {
			firstErr = fatalErr
			cancelRun()
		})
	}

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		if maintainErr := registration.Maintain(lifecycleCtx); maintainErr != nil && lifecycleCtx.Err() == nil {
			reportFatal(fmt.Errorf("qbit: maintain worker registration: %w", maintainErr))
		}
	}()

	var slots sync.WaitGroup
	slots.Add(w.options.Concurrency)
	for range w.options.Concurrency {
		go func() {
			defer slots.Done()
			w.runSlot(runCtx, processingCtx, reportFatal)
		}()
	}

	slotsDone := make(chan struct{})
	go func() {
		slots.Wait()
		close(slotsDone)
	}()

	<-runCtx.Done()

	timer := time.NewTimer(w.options.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-slotsDone:
	case <-timer.C:
		cancelProcessing()
		cancelLifecycle()
		return ErrWorkerShutdownTimeout
	}

	cancelProcessing()
	cancelLifecycle()
	<-heartbeatDone
	return firstErr
}

// Shutdown stops new reservations and waits for Run to finish. Passing a
// context with a deadline lets an application bound how long it waits. It is
// safe to call when the worker is not running.
func (w *Worker) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("qbit: shutdown context is required")
	}
	w.lifecycleMu.Lock()
	cancelRun := w.cancelRun
	runDone := w.runDone
	w.lifecycleMu.Unlock()
	if cancelRun == nil || runDone == nil {
		return nil
	}

	cancelRun()
	select {
	case <-runDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("qbit: wait for worker shutdown: %w", ctx.Err())
	}
}

func (w *Worker) beginRun(ctx context.Context) (context.Context, context.CancelFunc, error) {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if w.runDone != nil {
		return nil, nil, ErrWorkerAlreadyRunning
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	w.cancelRun = cancelRun
	w.runDone = make(chan struct{})
	return runCtx, cancelRun, nil
}

func (w *Worker) finishRun() {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	close(w.runDone)
	w.cancelRun = nil
	w.runDone = nil
}

func (w *Worker) runSlot(reserveCtx, processingCtx context.Context, reportFatal func(error)) {
	for {
		job, err := w.queue.ReserveBlocking(
			reserveCtx,
			w.options.ReserveWait,
			WithLockTTL(w.options.LockTTL),
		)
		if err != nil {
			if reserveCtx.Err() != nil {
				return
			}
			if errors.Is(err, ErrNoJob) || errors.Is(err, ErrQueuePaused) {
				continue
			}
			reportFatal(fmt.Errorf("qbit: reserve job: %w", err))
			return
		}

		if err := w.processJob(processingCtx, job); err != nil {
			reportFatal(err)
			return
		}
	}
}

func (w *Worker) processJob(processingCtx context.Context, job *Job) error {
	jobCtx, cancelJob := context.WithCancel(processingCtx)
	defer cancelJob()

	stopRenewal := make(chan struct{})
	renewalDone := make(chan error, 1)
	go w.renewReservation(jobCtx, cancelJob, job, stopRenewal, renewalDone)

	handlerErr := callHandler(w.handler, jobCtx, job)
	renewalFinished := false
	var renewalErr error
	if handlerErr != nil && !IsPermanent(handlerErr) && job.Attempts < w.options.Retry.MaxAttempts {
		delay := w.options.Retry.Backoff(job.Attempts)
		if delay < 0 {
			delay = 0
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case renewalErr = <-renewalDone:
				timer.Stop()
				renewalFinished = true
			case <-processingCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
			}
		}
	}
	if !renewalFinished {
		close(stopRenewal)
		renewalErr = <-renewalDone
	}
	if renewalErr != nil {
		return fmt.Errorf("qbit: renew job %q: %w", job.ID, renewalErr)
	}

	finishCtx, cancelFinish := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFinish()
	if handlerErr == nil {
		if err := w.queue.Complete(finishCtx, job); err != nil {
			return fmt.Errorf("qbit: complete job %q: %w", job.ID, err)
		}
		return nil
	}

	if IsPermanent(handlerErr) || job.Attempts >= w.options.Retry.MaxAttempts {
		if err := w.queue.Fail(finishCtx, job, handlerErr); err != nil {
			return fmt.Errorf("qbit: fail job %q: %w", job.ID, err)
		}
		return nil
	}

	if err := w.queue.Retry(finishCtx, job, handlerErr); err != nil {
		return fmt.Errorf("qbit: retry job %q: %w", job.ID, err)
	}
	return nil
}

func (w *Worker) renewReservation(
	ctx context.Context,
	cancel context.CancelFunc,
	job *Job,
	stop <-chan struct{},
	done chan<- error,
) {
	ticker := time.NewTicker(w.options.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			done <- nil
			return
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			renewCtx, cancelRenew := context.WithTimeout(context.Background(), w.options.RenewInterval)
			err := w.queue.Renew(renewCtx, job, w.options.LockTTL)
			cancelRenew()
			if err != nil {
				cancel()
				done <- err
				return
			}
		}
	}
}

func callHandler(handler Handler, ctx context.Context, job *Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, job)
}
