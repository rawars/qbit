// Package load implements the producer and worker loops used by qbit-demo.
package load

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rawars/qbit/packages/go/qbit"
)

// ProducerQueue is the queue surface used by the demo producer.
type ProducerQueue interface {
	Add(context.Context, string, []byte, ...qbit.AddOption) (*qbit.Job, error)
}

// ProducerConfig describes one bounded load run.
type ProducerConfig struct {
	Messages          int64
	MessagesPerSecond float64
	Groups            int
	Concurrency       int
	PayloadBytes      int
}

// ProducerResult contains measured client-side publishing throughput.
type ProducerResult struct {
	Published     int64
	Elapsed       time.Duration
	RatePerSecond float64
}

// RunProducer publishes a bounded number of jobs at a target rate. A zero rate
// sends as fast as Redis and the configured concurrency allow.
func RunProducer(ctx context.Context, queue ProducerQueue, config ProducerConfig) (ProducerResult, error) {
	if queue == nil {
		return ProducerResult{}, fmt.Errorf("qbit demo: queue is required")
	}
	if err := config.Validate(); err != nil {
		return ProducerResult{}, err
	}
	started := time.Now()
	runID := strconv.FormatInt(started.UnixNano(), 36)
	prefix := "load-" + runID + "-"
	payload := make([]byte, config.PayloadBytes)
	for index := range payload {
		payload[index] = 'q'
	}

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(chan int64, config.Concurrency*2)
	var published atomic.Int64
	var workers sync.WaitGroup
	var firstError error
	var errorLock sync.Mutex
	for range config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range tasks {
				_, err := queue.Add(runContext, "load", payload,
					qbit.WithGroup("load-group-"+strconv.FormatInt(index%int64(config.Groups), 10)),
					qbit.WithJobID(prefix+strconv.FormatInt(index, 10)),
				)
				if err != nil {
					errorLock.Lock()
					if firstError == nil && runContext.Err() == nil {
						firstError = fmt.Errorf("qbit demo: publish: %w", err)
						cancel()
					}
					errorLock.Unlock()
					return
				}
				published.Add(1)
			}
		}()
	}

publishLoop:
	for index := int64(0); index < config.Messages; index++ {
		if err := waitForRate(runContext, started, index, config.MessagesPerSecond); err != nil {
			break
		}
		select {
		case tasks <- index:
		case <-runContext.Done():
			break publishLoop
		}
	}
	close(tasks)
	workers.Wait()
	errorLock.Lock()
	err := firstError
	errorLock.Unlock()
	if err != nil {
		return ProducerResult{}, err
	}
	if ctx.Err() != nil {
		return ProducerResult{}, ctx.Err()
	}

	elapsed := time.Since(started)
	result := ProducerResult{Published: published.Load(), Elapsed: elapsed}
	if elapsed > 0 {
		result.RatePerSecond = float64(result.Published) / elapsed.Seconds()
	}
	return result, nil
}

// Validate checks the producer load shape.
func (config ProducerConfig) Validate() error {
	if config.Messages <= 0 {
		return fmt.Errorf("qbit demo: messages must be positive")
	}
	if config.MessagesPerSecond < 0 {
		return fmt.Errorf("qbit demo: rate cannot be negative")
	}
	if config.Groups <= 0 {
		return fmt.Errorf("qbit demo: groups must be positive")
	}
	if config.Concurrency <= 0 {
		return fmt.Errorf("qbit demo: producer concurrency must be positive")
	}
	if config.PayloadBytes < 0 || config.PayloadBytes > 1<<20 {
		return fmt.Errorf("qbit demo: payload bytes must be between 0 and 1048576")
	}
	return nil
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
