package load

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rawars/qbit/packages/go/qbit"
)

type fakeProducerQueue struct {
	mu    sync.Mutex
	count int64
}

func (queue *fakeProducerQueue) Add(context.Context, string, []byte, ...qbit.AddOption) (*qbit.Job, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.count++
	return &qbit.Job{}, nil
}

func TestRunProducerPublishesRequestedMessages(t *testing.T) {
	queue := &fakeProducerQueue{}
	result, err := RunProducer(context.Background(), queue, ProducerConfig{
		Messages:     100,
		Groups:       10,
		Concurrency:  4,
		PayloadBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Published != 100 || queue.count != 100 || result.RatePerSecond <= 0 {
		t.Fatalf("result = %+v, queue count = %d", result, queue.count)
	}
}

func TestProducerValidation(t *testing.T) {
	tests := []ProducerConfig{
		{},
		{Messages: 1, MessagesPerSecond: -1, Groups: 1, Concurrency: 1},
		{Messages: 1, Groups: 0, Concurrency: 1},
		{Messages: 1, Groups: 1, Concurrency: 0},
		{Messages: 1, Groups: 1, Concurrency: 1, PayloadBytes: 1<<20 + 1},
	}
	for _, config := range tests {
		if err := config.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil", config)
		}
	}
}

type fakeWorkerQueue struct {
	jobs   chan *qbit.Job
	mu     sync.Mutex
	done   int
	failed int
}

func (queue *fakeWorkerQueue) ReserveBlocking(ctx context.Context, wait time.Duration, _ ...qbit.ReserveOption) (*qbit.Job, error) {
	select {
	case job := <-queue.jobs:
		job.Attempts++
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(wait):
		return nil, qbit.ErrNoJob
	}
}

func (queue *fakeWorkerQueue) Retry(ctx context.Context, job *qbit.Job, _ error) error {
	queue.mu.Lock()
	queue.failed++
	queue.mu.Unlock()
	select {
	case queue.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (queue *fakeWorkerQueue) Complete(context.Context, *qbit.Job) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.done++
	return nil
}

func TestRunWorkersConsumesUntilCancelled(t *testing.T) {
	queue := &fakeWorkerQueue{jobs: make(chan *qbit.Job, 2)}
	queue.jobs <- &qbit.Job{ID: "1"}
	queue.jobs <- &qbit.Job{ID: "2"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := RunWorkers(ctx, queue, WorkerConfig{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 2 || queue.done != 2 {
		t.Fatalf("result = %+v, queue done = %d", result, queue.done)
	}
}

func TestRunWorkersRetriesAndRecoversInjectedFailures(t *testing.T) {
	queue := &fakeWorkerQueue{jobs: make(chan *qbit.Job, 2)}
	queue.jobs <- &qbit.Job{ID: "retry-me"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := RunWorkers(ctx, queue, WorkerConfig{Concurrency: 1, FailurePercent: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 1 || result.Failed != 1 || result.Recovered != 1 || queue.done != 1 || queue.failed != 1 {
		t.Fatalf("result = %+v, queue completed = %d, failed = %d", result, queue.done, queue.failed)
	}
}

type failingWorkerQueue struct{}

func (failingWorkerQueue) ReserveBlocking(context.Context, time.Duration, ...qbit.ReserveOption) (*qbit.Job, error) {
	return nil, errors.New("boom")
}

func (failingWorkerQueue) Complete(context.Context, *qbit.Job) error     { return nil }
func (failingWorkerQueue) Retry(context.Context, *qbit.Job, error) error { return nil }

func TestRunWorkersStopsAllWorkersOnError(t *testing.T) {
	if _, err := RunWorkers(context.Background(), failingWorkerQueue{}, WorkerConfig{Concurrency: 3}); err == nil {
		t.Fatal("RunWorkers error = nil")
	}
}

func TestWorkerValidation(t *testing.T) {
	if _, err := RunWorkers(context.Background(), nil, WorkerConfig{Concurrency: 1}); err == nil {
		t.Fatal("nil queue error = nil")
	}
	queue := &fakeWorkerQueue{jobs: make(chan *qbit.Job)}
	if _, err := RunWorkers(context.Background(), queue, WorkerConfig{}); err == nil {
		t.Fatal("zero concurrency error = nil")
	}
	if _, err := RunWorkers(context.Background(), queue, WorkerConfig{Concurrency: 1, ProcessingTime: -1}); err == nil {
		t.Fatal("negative processing time error = nil")
	}
	if _, err := RunWorkers(context.Background(), queue, WorkerConfig{Concurrency: 1, FailurePercent: -1}); err == nil {
		t.Fatal("negative failure rate error = nil")
	}
	if _, err := RunWorkers(context.Background(), queue, WorkerConfig{Concurrency: 1, FailurePercent: 101}); err == nil {
		t.Fatal("failure rate above 100 error = nil")
	}
}

func TestWorkerLockTTL(t *testing.T) {
	if got := workerLockTTL(time.Second); got != 30*time.Second {
		t.Fatalf("workerLockTTL = %v", got)
	}
	if got := workerLockTTL(20 * time.Second); got != time.Minute {
		t.Fatalf("workerLockTTL = %v", got)
	}
}
