package qbit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNewWorkerValidationAndDefaults(t *testing.T) {
	queue := &Queue{}
	handler := func(context.Context, *Job) error { return nil }

	if _, err := NewWorker(nil, handler, WorkerOptions{}); err == nil {
		t.Fatal("nil queue error = nil")
	}
	if _, err := NewWorker(queue, nil, WorkerOptions{}); err == nil {
		t.Fatal("nil handler error = nil")
	}
	if _, err := NewWorker(queue, handler, WorkerOptions{Concurrency: -1}); err == nil {
		t.Fatal("negative concurrency error = nil")
	}
	if _, err := NewWorker(queue, handler, WorkerOptions{LockTTL: time.Second, RenewInterval: time.Second}); err == nil {
		t.Fatal("renew interval equal to lock TTL error = nil")
	}
	if _, err := NewWorker(queue, handler, WorkerOptions{RegistrationTTL: time.Second}); err == nil {
		t.Fatal("short registration TTL error = nil")
	}

	worker, err := NewWorker(queue, handler, WorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if worker.options.Concurrency != 1 || worker.options.Retry.MaxAttempts != 3 {
		t.Fatalf("defaults = concurrency %d, attempts %d", worker.options.Concurrency, worker.options.Retry.MaxAttempts)
	}
}

func TestPermanentErrors(t *testing.T) {
	cause := errors.New("invalid destination")
	err := Permanent(cause)
	if !IsPermanent(err) {
		t.Fatal("Permanent error was not detected")
	}
	if !errors.Is(err, cause) {
		t.Fatal("Permanent error does not unwrap to cause")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	if IsPermanent(fmt.Errorf("wrapped: %w", cause)) {
		t.Fatal("regular wrapped error detected as permanent")
	}
}

func TestBackoffs(t *testing.T) {
	fixed := FixedBackoff(250 * time.Millisecond)
	if got := fixed(8); got != 250*time.Millisecond {
		t.Fatalf("FixedBackoff = %s", got)
	}

	exponential := ExponentialBackoff(time.Second, 5*time.Second)
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second} {
		if got := exponential(attempt + 1); got != want {
			t.Fatalf("ExponentialBackoff(%d) = %s, want %s", attempt+1, got, want)
		}
	}
}

func TestManagedWorkerRetriesAndPermanentFailures(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	for _, input := range []struct {
		name  string
		group string
	}{
		{name: "success", group: "conversation-1"},
		{name: "transient", group: "conversation-1"},
		{name: "permanent", group: "conversation-1"},
	} {
		if _, err := queue.Publish(ctx, input.name, nil, WithGroup(input.group)); err != nil {
			t.Fatal(err)
		}
	}

	worker, err := NewWorker(queue, func(_ context.Context, job *Job) error {
		switch job.Name {
		case "transient":
			if job.Attempts == 1 {
				return errors.New("temporary upstream failure")
			}
		case "permanent":
			return Permanent(errors.New("invalid payload"))
		}
		return nil
	}, WorkerOptions{
		Concurrency:     4,
		LockTTL:         300 * time.Millisecond,
		RenewInterval:   75 * time.Millisecond,
		ReserveWait:     25 * time.Millisecond,
		ShutdownTimeout: time.Second,
		Retry: RetryPolicy{
			MaxAttempts: 3,
			// Deliberately longer than LockTTL to verify that Qbit renews the
			// reservation while waiting to retry.
			Backoff: FixedBackoff(450 * time.Millisecond),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(workerCtx) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		stats, statsErr := queue.Stats(ctx, time.Minute)
		if statsErr != nil {
			t.Fatal(statsErr)
		}
		if stats.Totals.Completed == 2 && stats.Totals.Failed == 2 {
			if stats.Totals.Retried != 1 || stats.Totals.Recovered != 1 {
				t.Fatalf("retry totals = retried %d, recovered %d", stats.Totals.Retried, stats.Totals.Recovered)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker totals did not converge: %+v", stats.Totals)
		}
		time.Sleep(10 * time.Millisecond)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("Run after cancellation: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestManagedWorkerRenewsLongRunningJob(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Publish(ctx, "slow", nil, WithGroup("conversation")); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	worker, err := NewWorker(queue, func(context.Context, *Job) error {
		close(started)
		time.Sleep(220 * time.Millisecond)
		return nil
	}, WorkerOptions{
		Concurrency:     1,
		LockTTL:         75 * time.Millisecond,
		RenewInterval:   20 * time.Millisecond,
		ReserveWait:     20 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(workerCtx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWorkerAlreadyRunning) {
		t.Fatalf("concurrent Run error = %v, want ErrWorkerAlreadyRunning", err)
	}

	time.Sleep(130 * time.Millisecond)
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("competing Reserve error = %v, want ErrNoJob", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		stats, statsErr := queue.Stats(ctx, time.Minute)
		if statsErr != nil {
			t.Fatal(statsErr)
		}
		if stats.Totals.Completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("long-running job was not completed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if runErr := <-runDone; runErr != nil {
		t.Fatalf("Run after cancellation: %v", runErr)
	}
}
