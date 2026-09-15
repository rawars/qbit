package qbit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestGroupedRoundRobin(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)

	for _, input := range []struct{ group, payload string }{
		{"a", "a1"}, {"a", "a2"}, {"b", "b1"}, {"b", "b2"},
	} {
		if _, err := queue.Add(ctx, "work", []byte(input.payload), WithGroup(input.group)); err != nil {
			t.Fatal(err)
		}
	}

	var order []string
	for range 4 {
		job, err := queue.Reserve(ctx)
		if err != nil {
			t.Fatal(err)
		}
		order = append(order, string(job.Payload))
		if err := queue.Complete(ctx, job); err != nil {
			t.Fatal(err)
		}
	}

	want := []string{"a1", "b1", "a2", "b2"}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("processing order = %v, want %v", order, want)
		}
	}
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("empty Reserve error = %v, want ErrNoJob", err)
	}
}

func TestGroupAllowsOnlyOneActiveJob(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	for range 2 {
		if _, err := queue.Add(ctx, "work", nil, WithGroup("tenant")); err != nil {
			t.Fatal(err)
		}
	}

	first, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("second Reserve error = %v, want ErrNoJob", err)
	}
	if err := queue.Complete(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Reserve(ctx); err != nil {
		t.Fatalf("Reserve after completion: %v", err)
	}
}

func TestPauseStopsNewReservationsAndResumeWakesWorkers(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Publish(ctx, "work", nil, WithGroup("tenant")); err != nil {
		t.Fatal(err)
	}
	if err := queue.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	paused, err := queue.Paused(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !paused {
		t.Fatal("Paused = false after Pause")
	}
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrQueuePaused) {
		t.Fatalf("Reserve while paused error = %v, want ErrQueuePaused", err)
	}
	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Paused || stats.Waiting != 1 {
		t.Fatalf("paused stats = paused %v, waiting %d", stats.Paused, stats.Waiting)
	}

	reserved := make(chan *Job, 1)
	reserveErrors := make(chan error, 1)
	go func() {
		job, reserveErr := queue.ReserveBlocking(ctx, time.Second)
		if reserveErr != nil {
			reserveErrors <- reserveErr
			return
		}
		reserved <- job
	}()
	time.Sleep(50 * time.Millisecond)
	if err := queue.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-reserved:
		if err := queue.Complete(ctx, job); err != nil {
			t.Fatal(err)
		}
	case reserveErr := <-reserveErrors:
		t.Fatalf("ReserveBlocking after Resume: %v", reserveErr)
	case <-time.After(time.Second):
		t.Fatal("Resume did not wake the blocked reservation")
	}
	paused, err = queue.Paused(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if paused {
		t.Fatal("Paused = true after Resume")
	}
}

func TestExpiredReservationIsRecovered(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	first, err := queue.Add(ctx, "work", []byte("first"), WithGroup("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Add(ctx, "work", []byte("second"), WithGroup("tenant")); err != nil {
		t.Fatal(err)
	}

	reserved, err := queue.Reserve(ctx, WithLockTTL(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	recovered, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != first.ID || recovered.ID != reserved.ID {
		t.Fatalf("recovered ID = %q, want %q", recovered.ID, first.ID)
	}
	if recovered.Token == reserved.Token {
		t.Fatal("recovered reservation reused the expired token")
	}
	if err := queue.Complete(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	next, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(next.Payload) != "second" {
		t.Fatalf("next payload = %q, want second", next.Payload)
	}
}

func TestStatsSeparatesLiveAndExpiredReservations(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Add(ctx, "work", nil, WithGroup("expires")); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Add(ctx, "work", nil, WithGroup("live")); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Reserve(ctx, WithLockTTL(30*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Reserve(ctx, WithLockTTL(time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Active != 1 || stats.ExpiredReservations != 1 || stats.Waiting != 0 {
		t.Fatalf("reservation stats = active %d, expired %d, waiting %d; want 1, 1, 0",
			stats.Active, stats.ExpiredReservations, stats.Waiting)
	}
}

func TestRenewPreventsRecovery(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Add(ctx, "work", nil, WithGroup("tenant")); err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx, WithLockTTL(80*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := queue.Renew(ctx, job, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Reserve during renewed lock = %v, want ErrNoJob", err)
	}
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
}

func TestAddWithJobIDIsIdempotent(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	first, err := queue.Add(ctx, "work", []byte("original"), WithGroup("a"), WithJobID("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := queue.Add(ctx, "other", []byte("replacement"), WithGroup("b"), WithJobID("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || !duplicate.Duplicate {
		t.Fatalf("duplicate flags = %v, %v", first.Duplicate, duplicate.Duplicate)
	}
	if duplicate.Group != "a" || duplicate.Name != "work" || string(duplicate.Payload) != "original" {
		t.Fatalf("duplicate returned %+v, want original job", duplicate)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("duplicate created extra work: %v", err)
	}
}

func TestFinishedJobHasRetention(t *testing.T) {
	queue, ctx := newIntegrationQueue(t, WithRetention(100*time.Millisecond, 200*time.Millisecond))
	added, err := queue.Add(ctx, "work", nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	ttl, err := queue.client.PTTL(ctx, queue.keys.job(added.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 100*time.Millisecond {
		t.Fatalf("completed job TTL = %v", ttl)
	}
}

func TestFailedJobStoresCauseAndRetention(t *testing.T) {
	queue, ctx := newIntegrationQueue(t, WithRetention(100*time.Millisecond, 250*time.Millisecond))
	added, err := queue.Add(ctx, "work", nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Fail(ctx, job, errors.New("processor unavailable")); err != nil {
		t.Fatal(err)
	}
	values, err := queue.client.HMGet(ctx, queue.keys.job(added.ID), "state", "error").Result()
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(values[0]) != "failed" || stringValue(values[1]) != "processor unavailable" {
		t.Fatalf("failed job fields = %v", values)
	}
	ttl, err := queue.client.PTTL(ctx, queue.keys.job(added.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 250*time.Millisecond {
		t.Fatalf("failed job TTL = %v", ttl)
	}
}

func TestRetryPreservesGroupOrderAndReportsRecovery(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	first, err := queue.Add(ctx, "work", []byte("first"), WithGroup("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Add(ctx, "work", []byte("second"), WithGroup("tenant")); err != nil {
		t.Fatal(err)
	}

	failedAttempt, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failedAttempt.ID != first.ID || failedAttempt.Attempts != 1 {
		t.Fatalf("first attempt = %+v", failedAttempt)
	}
	if err := queue.Retry(ctx, failedAttempt, errors.New("temporary outage")); err != nil {
		t.Fatal(err)
	}

	retry, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retry.ID != first.ID || retry.Attempts != 2 {
		t.Fatalf("retry = %+v, want first job on attempt 2", retry)
	}
	if err := queue.Complete(ctx, retry); err != nil {
		t.Fatal(err)
	}
	next, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(next.Payload) != "second" {
		t.Fatalf("next payload = %q, want second", next.Payload)
	}
	if err := queue.Complete(ctx, next); err != nil {
		t.Fatal(err)
	}

	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Totals.Published != 2 || stats.Totals.Failed != 1 ||
		stats.Totals.Retried != 1 || stats.Totals.Recovered != 1 ||
		stats.Totals.Completed != 2 || stats.Waiting != 0 || stats.Active != 0 {
		t.Fatalf("retry stats = %+v", stats)
	}
	if stats.Rates.Failed <= 0 || stats.Rates.Retried <= 0 || stats.Rates.Recovered <= 0 {
		t.Fatalf("retry rates = %+v", stats.Rates)
	}
}

func TestRetryRejectsLostReservation(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if err := queue.Retry(ctx, nil, errors.New("temporary")); err == nil {
		t.Fatal("Retry(nil) error = nil")
	}
	if _, err := queue.Add(ctx, "work", nil); err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job.Token = "not-the-owner"
	if err := queue.Retry(ctx, job, errors.New("temporary")); !errors.Is(err, ErrReservationLost) {
		t.Fatalf("Retry with lost reservation error = %v", err)
	}
}

func TestTerminalAcknowledgementsAreIdempotent(t *testing.T) {
	for _, test := range []struct {
		name         string
		terminalType string
		finish       func(context.Context, *Queue, *Job) error
		conflict     func(context.Context, *Queue, *Job) error
	}{
		{
			name:         "complete",
			terminalType: "completed",
			finish: func(ctx context.Context, queue *Queue, job *Job) error {
				return queue.Complete(ctx, job)
			},
			conflict: func(ctx context.Context, queue *Queue, job *Job) error {
				return queue.Fail(ctx, job, errors.New("conflicting terminal state"))
			},
		},
		{
			name:         "fail",
			terminalType: "failed",
			finish: func(ctx context.Context, queue *Queue, job *Job) error {
				return queue.Fail(ctx, job, errors.New("permanent"))
			},
			conflict: func(ctx context.Context, queue *Queue, job *Job) error {
				return queue.Complete(ctx, job)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, ctx := newIntegrationQueue(t)
			if _, err := queue.Add(ctx, "work", nil); err != nil {
				t.Fatal(err)
			}
			job, err := queue.Reserve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.finish(ctx, queue, job); err != nil {
				t.Fatalf("first acknowledgement: %v", err)
			}
			if err := test.finish(ctx, queue, job); err != nil {
				t.Fatalf("replayed acknowledgement: %v", err)
			}
			if err := test.conflict(ctx, queue, job); !errors.Is(err, ErrReservationLost) {
				t.Fatalf("conflicting terminal state error = %v, want ErrReservationLost", err)
			}

			stats, err := queue.Stats(ctx, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if test.name == "complete" && stats.Totals.Completed != 1 {
				t.Fatalf("completed total = %d, want 1", stats.Totals.Completed)
			}
			if test.name == "fail" && stats.Totals.Failed != 1 {
				t.Fatalf("failed total = %d, want 1", stats.Totals.Failed)
			}
			events, err := queue.RecentEvents(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			terminalEvents := 0
			for _, event := range events {
				if event.Type == test.terminalType {
					terminalEvents++
				}
			}
			if terminalEvents != 1 {
				t.Fatalf("terminal event count = %d, want 1; events = %+v", terminalEvents, events)
			}
		})
	}
}

func TestTerminalAcknowledgementRejectsAnotherToken(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Add(ctx, "work", nil); err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}

	stale := *job
	stale.Token = "another-reservation-token"
	if err := queue.Complete(ctx, &stale); !errors.Is(err, ErrReservationLost) {
		t.Fatalf("Complete with another token error = %v, want ErrReservationLost", err)
	}
}

func TestReserveBlockingWakesForJob(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = queue.Add(ctx, "work", []byte("ready"))
	}()
	job, err := queue.ReserveBlocking(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "ready" {
		t.Fatalf("payload = %q, want ready", job.Payload)
	}
}

func TestReserveBlockingHonorsTimeout(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	started := time.Now()
	_, err := queue.ReserveBlocking(ctx, 50*time.Millisecond)
	if !errors.Is(err, ErrNoJob) {
		t.Fatalf("ReserveBlocking error = %v, want ErrNoJob", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("ReserveBlocking took %v, expected bounded wait", elapsed)
	}
}

func TestQueueStatsAggregateLifecycle(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Add(ctx, "work", nil, WithGroup("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Add(ctx, "work", nil, WithGroup("b")); err != nil {
		t.Fatal(err)
	}
	completed, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Complete(ctx, completed); err != nil {
		t.Fatal(err)
	}
	failed, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Fail(ctx, failed, errors.New("test failure")); err != nil {
		t.Fatal(err)
	}

	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Totals.Published != 2 || stats.Totals.Reserved != 2 ||
		stats.Totals.Completed != 1 || stats.Totals.Failed != 1 {
		t.Fatalf("lifecycle totals = %+v", stats.Totals)
	}
	if stats.Waiting != 0 || stats.Active != 0 {
		t.Fatalf("waiting = %d, active = %d", stats.Waiting, stats.Active)
	}
	if stats.Rates.Published <= 0 || stats.Rates.Completed <= 0 || stats.Rates.Failed <= 0 {
		t.Fatalf("lifecycle rates = %+v", stats.Rates)
	}
	events, err := queue.RecentEvents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 6 || events[0].Type != "failed" {
		t.Fatalf("recent events = %+v", events)
	}
}

func TestStatsUsesAggregatesWhenEventStreamIsUnavailable(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.Add(ctx, "work", nil, WithGroup("group")); err != nil {
		t.Fatal(err)
	}
	job, err := queue.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := queue.client.Del(ctx, queue.keys.events()).Err(); err != nil {
		t.Fatal(err)
	}

	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rates.Published <= 0 || stats.Rates.Reserved <= 0 || stats.Rates.Completed <= 0 {
		t.Fatalf("aggregate rates = %+v", stats.Rates)
	}
	if stats.AverageProcessingMillis <= 0 {
		t.Fatalf("average processing = %v, want positive", stats.AverageProcessingMillis)
	}
}

func TestStatsPreservesLegacyQueueTotalsWithoutScanningEvents(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	now := time.Now()
	if err := queue.client.HSet(ctx, queue.keys.metrics(),
		"initialized_at", now.Add(-time.Hour).UnixMilli(),
		"published", 3,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.client.ZAdd(ctx, queue.keys.activeJobs(),
		redis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: "expired"},
		redis.Z{Score: float64(now.Add(time.Minute).UnixMilli()), Member: "live"},
	).Err(); err != nil {
		t.Fatal(err)
	}

	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Totals.Published != 3 || stats.Waiting != 1 || stats.Active != 1 || stats.ExpiredReservations != 1 {
		t.Fatalf("legacy stats = %+v", stats)
	}
	if stats.Rates != (LifecycleRates{}) || !stats.WindowMayBeEventTruncated {
		t.Fatalf("legacy rolling metrics = rates %+v, truncated %v", stats.Rates, stats.WindowMayBeEventTruncated)
	}
}

func TestWorkerRegistrationAppearsInStats(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	registration, err := queue.RegisterWorker(ctx, 12,
		WithWorkerID("worker-1"),
		WithWorkerInstance("pod-a"),
		WithWorkerTTL(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := registration.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err := queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkerReplicas != 1 || stats.WorkerConcurrency != 12 || len(stats.Workers) != 1 {
		t.Fatalf("worker stats = %+v", stats)
	}
	if stats.Workers[0].ID != "worker-1" || stats.Workers[0].Instance != "pod-a" {
		t.Fatalf("worker info = %+v", stats.Workers[0])
	}
	if err := registration.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err = queue.Stats(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkerReplicas != 0 || stats.WorkerConcurrency != 0 {
		t.Fatalf("worker remained registered: %+v", stats)
	}
}

func TestRegisterWorkerValidatesOptions(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	if _, err := queue.RegisterWorker(ctx, 0); err == nil {
		t.Fatal("zero concurrency error = nil")
	}
	if _, err := queue.RegisterWorker(ctx, 1, WithWorkerTTL(time.Second)); err == nil {
		t.Fatal("short TTL error = nil")
	}
	if _, err := queue.RegisterWorker(ctx, 1, WithWorkerID("bad:id")); err == nil {
		t.Fatal("invalid worker ID error = nil")
	}
}

func newIntegrationQueue(t *testing.T, options ...QueueOption) (*Queue, context.Context) {
	t.Helper()
	address := os.Getenv("QBIT_REDIS_ADDR")
	if address == "" {
		t.Skip("set QBIT_REDIS_ADDR to run Redis integration tests")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	queue, err := NewQueue(fmt.Sprintf("test-%d", time.Now().UnixNano()), client, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, _ := client.Keys(ctx, queue.keys.base+":*").Result()
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		_ = client.SRem(ctx, queueRegistryKey, queue.name).Err()
	})
	return queue, ctx
}
