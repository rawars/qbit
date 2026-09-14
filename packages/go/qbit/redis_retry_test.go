package qbit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type testTimeoutError struct{}

func (testTimeoutError) Error() string   { return "temporary Redis timeout" }
func (testTimeoutError) Timeout() bool   { return true }
func (testTimeoutError) Temporary() bool { return true }

func TestTransientRedisErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "EOF", err: io.EOF, want: true},
		{name: "network timeout", err: testTimeoutError{}, want: true},
		{name: "temporary DNS", err: &net.DNSError{Err: "server misbehaving", Name: "redis.local", IsTemporary: true}, want: true},
		{name: "unknown DNS host", err: &net.DNSError{Err: "no such host", Name: "missing.invalid", IsNotFound: true}, want: false},
		{name: "pool timeout", err: redis.ErrPoolTimeout, want: true},
		{name: "loading", err: errors.New("LOADING Redis is loading the dataset"), want: true},
		{name: "read only during failover", err: errors.New("READONLY replica"), want: true},
		{name: "cluster unavailable", err: errors.New("CLUSTERDOWN cluster unavailable"), want: true},
		{name: "try again", err: errors.New("TRYAGAIN temporary condition"), want: true},
		{name: "maximum clients", err: errors.New("ERR max number of clients reached"), want: true},
		{name: "cancelled", err: context.Canceled, want: false},
		{name: "authentication", err: errors.New("WRONGPASS invalid credentials"), want: false},
		{name: "permission", err: errors.New("NOPERM this user has no permissions"), want: false},
		{name: "out of memory", err: errors.New("OOM command not allowed"), want: false},
		{name: "script defect", err: errors.New("ERR Error running script"), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wrapped := fmt.Errorf("qbit operation: %w", test.err)
			if got := isTransientRedisError(wrapped); got != test.want {
				t.Fatalf("isTransientRedisError(%v) = %t, want %t", wrapped, got, test.want)
			}
		})
	}
}

func TestRetryRedisValueRecoversAndDoesNotRetryPermanentErrors(t *testing.T) {
	attempts := 0
	value, err := retryRedisValue(context.Background(), FixedBackoff(0), func(context.Context) (string, error) {
		attempts++
		if attempts < 3 {
			return "", testTimeoutError{}
		}
		return "recovered", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value != "recovered" || attempts != 3 {
		t.Fatalf("result = %q after %d attempts", value, attempts)
	}

	permanent := errors.New("WRONGPASS invalid credentials")
	attempts = 0
	_, err = retryRedisValue(context.Background(), FixedBackoff(0), func(context.Context) (string, error) {
		attempts++
		return "", permanent
	})
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatalf("permanent error = %v after %d attempts", err, attempts)
	}
}

func TestRetryRedisValueBackoffHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := retryRedisValue(ctx, FixedBackoff(time.Hour), func(context.Context) (struct{}, error) {
			close(called)
			return struct{}{}, testTimeoutError{}
		})
		done <- err
	}()

	<-called
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.As(err, new(testTimeoutError)) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Redis retry did not stop after cancellation")
	}
}

type scriptFailureHook struct {
	mu        sync.Mutex
	remaining map[string]int
	failure   error
}

func (hook *scriptFailureHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (hook *scriptFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		arguments := command.Args()
		if command.Name() == "evalsha" && len(arguments) > 1 && hook.take(fmt.Sprint(arguments[1])) {
			if hook.failure != nil {
				return hook.failure
			}
			return &net.OpError{Op: "read", Net: "tcp", Err: testTimeoutError{}}
		}
		return next(ctx, command)
	}
}

func (hook *scriptFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (hook *scriptFailureHook) take(hash string) bool {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	if hook.remaining[hash] <= 0 {
		return false
	}
	hook.remaining[hash]--
	return true
}

func (hook *scriptFailureHook) failuresLeft() int {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	total := 0
	for _, remaining := range hook.remaining {
		total += remaining
	}
	return total
}

func TestManagedWorkerReturnsPermanentRedisFailure(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Publish(ctx, "must-not-run", nil, WithGroup("conversation")); err != nil {
		t.Fatal(err)
	}

	permanent := errors.New("ERR Error running script: invalid protocol response")
	queue.client.AddHook(&scriptFailureHook{
		remaining: map[string]int{reserveScript.Hash(): 1},
		failure:   permanent,
	})

	handlerCalled := make(chan struct{}, 1)
	worker, err := NewWorker(queue, func(context.Context, *Job) error {
		handlerCalled <- struct{}{}
		return nil
	}, WorkerOptions{
		Concurrency:       1,
		ReserveWait:       20 * time.Millisecond,
		ShutdownTimeout:   time.Second,
		RedisRetryBackoff: FixedBackoff(5 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(context.Background()) }()
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, permanent) {
			t.Fatalf("Run error = %v, want permanent Redis error", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker hid or retried a permanent Redis error")
	}
	select {
	case <-handlerCalled:
		t.Fatal("handler ran despite permanent reserve failure")
	default:
	}
}

func TestManagedWorkerSurvivesTransientRedisFailures(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Publish(ctx, "temporary-redis-outage", nil, WithGroup("conversation")); err != nil {
		t.Fatal(err)
	}

	hook := &scriptFailureHook{remaining: map[string]int{
		reserveScript.Hash(): 2,
		renewScript.Hash():   2,
		finishScript.Hash():  2,
	}}
	queue.client.AddHook(hook)

	handlerStarted := make(chan struct{})
	worker, err := NewWorker(queue, func(context.Context, *Job) error {
		close(handlerStarted)
		time.Sleep(250 * time.Millisecond)
		return nil
	}, WorkerOptions{
		Concurrency:       1,
		LockTTL:           500 * time.Millisecond,
		RenewInterval:     75 * time.Millisecond,
		ReserveWait:       20 * time.Millisecond,
		ShutdownTimeout:   time.Second,
		RedisRetryBackoff: FixedBackoff(5 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(workerCtx) }()

	select {
	case <-handlerStarted:
	case runErr := <-runDone:
		t.Fatalf("worker stopped during transient reserve failures: %v", runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not recover from transient reserve failures")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, statsErr := queue.Stats(ctx, time.Minute)
		if statsErr != nil {
			t.Fatal(statsErr)
		}
		if stats.Totals.Completed == 1 {
			break
		}
		select {
		case runErr := <-runDone:
			t.Fatalf("worker stopped during transient renew/finalize failures: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete after Redis recovered: %+v", stats.Totals)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if remaining := hook.failuresLeft(); remaining != 0 {
		t.Fatalf("not every injected transient failure was exercised: %d left", remaining)
	}

	cancel()
	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("Run after cancellation: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}
