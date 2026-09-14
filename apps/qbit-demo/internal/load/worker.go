package load

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rawars/qbit/packages/go/qbit"
)

// WorkerQueue is the queue surface used by the demo worker.
type WorkerQueue interface {
	ReserveBlocking(context.Context, time.Duration, ...qbit.ReserveOption) (*qbit.Job, error)
	Complete(context.Context, *qbit.Job) error
	Retry(context.Context, *qbit.Job, error) error
}

// WorkerConfig describes one worker process. Concurrency is the number of
// jobs this process can handle at once.
type WorkerConfig struct {
	Concurrency    int
	ProcessingTime time.Duration
	// FailurePercent deterministically fails this percentage of jobs on their
	// first attempt. Failed jobs are retried once and then complete normally.
	FailurePercent float64
}

// WorkerResult summarizes what this demo process observed. Failed counts
// failed attempts; Recovered counts those jobs when the retry later succeeds.
type WorkerResult struct {
	Completed int64
	Failed    int64
	Recovered int64
}

// RunWorkers consumes until ctx is cancelled and returns the number of jobs
// completed by this process.
func RunWorkers(ctx context.Context, queue WorkerQueue, config WorkerConfig) (WorkerResult, error) {
	if queue == nil {
		return WorkerResult{}, fmt.Errorf("qbit demo: queue is required")
	}
	if config.Concurrency <= 0 {
		return WorkerResult{}, fmt.Errorf("qbit demo: worker concurrency must be positive")
	}
	if config.ProcessingTime < 0 {
		return WorkerResult{}, fmt.Errorf("qbit demo: processing time cannot be negative")
	}
	if config.FailurePercent < 0 || config.FailurePercent > 100 {
		return WorkerResult{}, fmt.Errorf("qbit demo: failure rate must be between 0 and 100 percent")
	}

	runContext, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var completed atomic.Int64
	var failed atomic.Int64
	var recovered atomic.Int64
	var workers sync.WaitGroup
	var firstError error
	var errorLock sync.Mutex
	for range config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for runContext.Err() == nil {
				job, err := queue.ReserveBlocking(runContext, time.Second, qbit.WithLockTTL(workerLockTTL(config.ProcessingTime)))
				if errors.Is(err, qbit.ErrNoJob) {
					continue
				}
				if err != nil {
					if runContext.Err() == nil {
						recordWorkerError(&errorLock, &firstError, fmt.Errorf("qbit demo: reserve: %w", err))
						cancelRun()
					}
					return
				}
				if config.ProcessingTime > 0 {
					time.Sleep(config.ProcessingTime)
				}
				finishContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if shouldInjectFailure(job, config.FailurePercent) {
					err = queue.Retry(finishContext, job, errors.New("qbit demo: injected transient failure"))
					cancel()
					if err != nil {
						recordWorkerError(&errorLock, &firstError, fmt.Errorf("qbit demo: retry: %w", err))
						cancelRun()
						return
					}
					failed.Add(1)
					continue
				}
				err = queue.Complete(finishContext, job)
				cancel()
				if err != nil {
					recordWorkerError(&errorLock, &firstError, fmt.Errorf("qbit demo: complete: %w", err))
					cancelRun()
					return
				}
				completed.Add(1)
				if job.Attempts > 1 {
					recovered.Add(1)
				}
			}
		}()
	}
	workers.Wait()
	errorLock.Lock()
	err := firstError
	errorLock.Unlock()
	return WorkerResult{
		Completed: completed.Load(),
		Failed:    failed.Load(),
		Recovered: recovered.Load(),
	}, err
}

func shouldInjectFailure(job *qbit.Job, failurePercent float64) bool {
	if job == nil || job.Attempts != 1 || failurePercent <= 0 {
		return false
	}
	if failurePercent >= 100 {
		return true
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(job.ID))
	return hash.Sum32()%10_000 < uint32(failurePercent*100)
}

func recordWorkerError(lock *sync.Mutex, target *error, err error) {
	lock.Lock()
	defer lock.Unlock()
	if *target == nil {
		*target = err
	}
}

func workerLockTTL(processingTime time.Duration) time.Duration {
	ttl := processingTime * 3
	if ttl < 30*time.Second {
		return 30 * time.Second
	}
	return ttl
}
