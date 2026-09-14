package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	qbit "github.com/rawars/qbit/packages/go"
	"github.com/redis/go-redis/v9"
)

func TestSimulationFailureKeepsTheOriginalError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sim := &simulation{ctx: ctx, cancel: cancel}
	original := errors.New("redis: OOM command not allowed when used memory > 'maxmemory'")
	sim.fail(original)
	sim.fail(errors.New("context canceled"))

	sim.stateMu.RLock()
	message := sim.errorMessage
	sim.stateMu.RUnlock()
	if message != original.Error() {
		t.Fatalf("failure = %q, want original Redis error %q", message, original.Error())
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("failure should cancel the simulation")
	}
}

func TestPublishFailureIsExposedBeforeCancellation(t *testing.T) {
	client, err := qbit.NewClient(qbit.ClientOptions{Redis: qbit.RedisOptions{Address: "127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := client.Queue("fazpi-publisher-error")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	config := defaultConfig("127.0.0.1:1")
	config.TrafficProfiles = []TrafficProfile{{Account: "Casur", Agent: "Kata", PeoplePerHour: 1, MessagesPerPerson: 1}}
	config.ArrivalWeights = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	config.TestDurationSeconds = 1
	config.PublisherConcurrency = 1
	sim := &simulation{
		config:         config,
		runID:          "publisher-error",
		publisherQueue: queue,
		ctx:            ctx,
		cancel:         cancel,
		profileCounts:  make([]profileCounters, 1),
	}

	publishErr := sim.publishAll()
	if publishErr == nil {
		t.Fatal("expected publication to fail with the closed Redis client")
	}
	sim.stateMu.RLock()
	message := sim.errorMessage
	sim.stateMu.RUnlock()
	if message != publishErr.Error() || !strings.Contains(message, "closed") {
		t.Fatalf("visible error = %q, publication error = %q", message, publishErr)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("simulation context error = %v, want canceled", ctx.Err())
	}
}

func TestRedisScanLiteralKeepsCleanupOnOneQueue(t *testing.T) {
	got := redisScanLiteral(`fazpi*[test]?\\queue`)
	want := `fazpi\*\[test\]\?\\\\queue`
	if got != want {
		t.Fatalf("escaped queue = %q, want %q", got, want)
	}
}

func TestCleanupOnlyAcceptsLoopbackRedis(t *testing.T) {
	for _, address := range []string{"127.0.0.1:6379", "localhost:6379", "[::1]:6379", "host.docker.internal:6379"} {
		if !isLoopbackRedisAddress(address) {
			t.Fatalf("expected %q to be accepted", address)
		}
	}
	for _, address := range []string{"redis.internal:6379", "10.0.0.8:6379", "127.0.0.1"} {
		if isLoopbackRedisAddress(address) {
			t.Fatalf("expected %q to be rejected", address)
		}
	}
}

func TestPurgeQueueDataOnlyDeletesRequestedQueue(t *testing.T) {
	address := os.Getenv("QBIT_REDIS_ADDR")
	if address == "" {
		t.Skip("set QBIT_REDIS_ADDR to run the Redis cleanup test")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	target := "fazpi-cleanup-target-" + suffix
	neighbor := "fazpi-cleanup-neighbor-" + suffix
	targetKey := "qbit:{" + target + "}:job:1"
	neighborKey := "qbit:{" + neighbor + "}:job:1"
	client := redis.NewClient(&redis.Options{Addr: address})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Unlink(cleanupCtx, targetKey, neighborKey).Err()
		_ = client.SRem(cleanupCtx, "qbit:registry:{queues}:names", target, neighbor).Err()
		_ = client.Close()
	})
	if err := client.Set(ctx, targetKey, "target", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, neighborKey, "neighbor", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, "qbit:registry:{queues}:names", target, neighbor).Err(); err != nil {
		t.Fatal(err)
	}

	removed, err := purgeQueueData(ctx, address, target)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed keys = %d, want 1", removed)
	}
	if exists := client.Exists(ctx, targetKey).Val(); exists != 0 {
		t.Fatal("target queue key still exists")
	}
	if exists := client.Exists(ctx, neighborKey).Val(); exists != 1 {
		t.Fatal("neighbor queue key was removed")
	}
	if registered := client.SIsMember(ctx, "qbit:registry:{queues}:names", target).Val(); registered {
		t.Fatal("target queue is still registered")
	}
	if registered := client.SIsMember(ctx, "qbit:registry:{queues}:names", neighbor).Val(); !registered {
		t.Fatal("neighbor queue registration was removed")
	}
}

func TestDefaultConfigIsValid(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	if err := config.validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
	if got, want := config.expectedUnique(), int64(100_700); got != want {
		t.Fatalf("default expected messages = %d, want %d", got, want)
	}
}

func TestMixedTrafficProfilesScaleVirtualTime(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	config.VirtualDurationMinutes = 30
	config.TrafficProfiles = []TrafficProfile{
		{Account: "heavy", Agent: "kata", PeoplePerHour: 100_000, MessagesPerPerson: 2},
		{Account: "small", Agent: "pascual", PeoplePerHour: 500, MessagesPerPerson: 2},
	}
	if got, want := config.conversationsFor(config.TrafficProfiles[0]), int64(50_000); got != want {
		t.Fatalf("heavy conversations = %d, want %d", got, want)
	}
	if got, want := config.expectedUnique(), int64(100_500); got != want {
		t.Fatalf("expected messages = %d, want %d", got, want)
	}
}

func TestArrivalCurveIsMonotonicAndCreatesPeak(t *testing.T) {
	weights := []float64{0.25, 0.25, 0.5, 1, 2, 4, 2, 1, 0.5, 0.25, 0.25, 0.25}
	previous := float64(-1)
	buckets := make([]int, len(weights))
	for index := int64(0); index < 10_000; index++ {
		fraction := arrivalFraction(index, 10_000, weights)
		if fraction < previous || fraction < 0 || fraction > 1 {
			t.Fatalf("arrival fraction is invalid or not monotonic: previous=%f current=%f", previous, fraction)
		}
		previous = fraction
		bucket := min(len(weights)-1, int(fraction*float64(len(weights))))
		buckets[bucket]++
	}
	if buckets[5] <= buckets[0]*10 {
		t.Fatalf("expected a visible peak, buckets=%v", buckets)
	}
}

func TestCapacityPlanUsesAcceleratedDemand(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	config.TestDurationSeconds = 100
	config.ProcessingMillis = 50
	config.ProcessingJitterMillis = 0
	config.WorkerReplicas = 2
	config.WorkerConcurrency = 25
	sim := &simulation{config: config, expectedUnique: 100_000}
	plan := sim.capacityPlan()
	if plan.TargetMessagesPerSecond != 1_000 || plan.EstimatedWorkerCapacity != 1_000 {
		t.Fatalf("unexpected capacity plan: %#v", plan)
	}
	if plan.EstimatedRequiredSlots != 50 || plan.EstimatedCapacityMargin != 1 {
		t.Fatalf("unexpected slots or margin: %#v", plan)
	}
}

func TestConfigRejectsImpossibleRetryPolicy(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	config.TransientFailurePercent = 10
	config.MaxAttempts = 1
	if err := config.validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestConfigRejectsPoolsThatCanBeExhaustedByConcurrency(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	config.PublisherRedisPoolSize = config.PublisherConcurrency - 1
	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "publisher Redis pool") {
		t.Fatalf("expected publisher pool validation error, got %v", err)
	}

	config = defaultConfig("127.0.0.1:6379")
	config.WorkerRedisPoolSize = config.WorkerConcurrency + 1
	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "worker Redis pool") {
		t.Fatalf("expected worker pool validation error, got %v", err)
	}
}

func TestRedisPoolPlanAccountsForEveryIsolatedClient(t *testing.T) {
	config := defaultConfig("127.0.0.1:6379")
	config.WorkerReplicas = 10
	plan := config.redisPoolPlan()
	if plan.Publisher != 64 || plan.WorkerPerReplica != 35 || plan.WorkersTotal != 350 || plan.Monitor != 16 {
		t.Fatalf("unexpected Redis pool plan: %#v", plan)
	}
	if plan.MaximumConnections != 430 {
		t.Fatalf("maximum connections = %d, want 430", plan.MaximumConnections)
	}
}

func TestPublicationTimingDetectsSchedulerLag(t *testing.T) {
	now := time.Now()
	sim := &simulation{
		config:             Config{TestDurationSeconds: 60},
		startedAt:          now.Add(-61 * time.Second),
		producerFinishedAt: now,
	}
	sim.publishScheduleSamples.Store(2)
	sim.publishScheduleDelayNanos.Store(int64(1500 * time.Millisecond))
	sim.maximumPublishDelayNanos.Store(int64(1 * time.Second))
	onTime := sim.publicationTiming(true)
	if !onTime.PublishedWithinTolerance || onTime.ToleranceSeconds != 2 || onTime.AverageScheduleDelayMS != 750 {
		t.Fatalf("unexpected on-time publication result: %#v", onTime)
	}

	sim.maximumPublishDelayNanos.Store(int64(3 * time.Second))
	late := sim.publicationTiming(true)
	if late.PublishedWithinTolerance {
		t.Fatalf("late publication should fail: %#v", late)
	}
}

func TestFailureSelectionIsDeterministicAndDisjoint(t *testing.T) {
	for index := 0; index < 10_000; index++ {
		id := "message-" + strings.Repeat("x", index%17) + string(rune(index))
		permanent := selected(id, 10, 0)
		transient := selected(id, 20, 10)
		if permanent && transient {
			t.Fatalf("message %q selected for both failure classes", id)
		}
		if permanent != selected(id, 10, 0) || transient != selected(id, 20, 10) {
			t.Fatalf("classification for %q is not deterministic", id)
		}
	}
}

func TestPayloadCanBePaddedAndDecoded(t *testing.T) {
	original := simulatedMessage{RunID: "run", AgentID: "agent-1", ThreadID: "thread-1", Message: "message-1", Sequence: 7}
	payload, err := makePayload(original, 1_024)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 1_024 {
		t.Fatalf("payload length = %d, want 1024", len(payload))
	}
	var decoded simulatedMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != original {
		t.Fatalf("decoded payload = %#v, want %#v", decoded, original)
	}
}

func TestValidationStates(t *testing.T) {
	sim := &simulation{expectedUnique: 10, config: Config{QueueWaitSLAms: 2_000}}
	pending := sim.validations("running", false, CounterSnapshot{}, qbit.Stats{Waiting: 10})
	if pending[4].Status != "pending" || pending[5].Status != "pending" {
		t.Fatalf("completion checks should be pending while running: %#v", pending)
	}
	failed := sim.validations("running", false, CounterSnapshot{OrderingViolations: 1}, qbit.Stats{})
	if failed[0].Status != "failed" {
		t.Fatalf("ordering violation should fail immediately: %#v", failed[0])
	}
	passed := sim.validations("completed", true, CounterSnapshot{Completed: 10, DuplicateAttempts: 2, DuplicatesConfirmed: 2}, qbit.Stats{})
	for _, validation := range passed {
		if validation.Status != "passed" {
			t.Fatalf("validation should pass: %#v", validation)
		}
	}
}

func TestQueueWaitSLAFailsForSlowAgent(t *testing.T) {
	sim := &simulation{
		config: Config{
			QueueWaitSLAms:  100,
			TrafficProfiles: []TrafficProfile{{Account: "Pascual", Agent: "Principal", PeoplePerHour: 1, MessagesPerPerson: 1}},
		},
		profileCounts: make([]profileCounters, 1),
	}
	sim.profileCounts[0].maxWaitNanos.Store(int64(150 * time.Millisecond))
	passed, explanation := sim.queueWaitSLA()
	if passed || !strings.Contains(explanation, "Pascual / Principal") {
		t.Fatalf("expected SLA failure identifying the agent, got passed=%v explanation=%q", passed, explanation)
	}
}

func TestApplicationIsolatesRedisPoolsUnderWorkerLoad(t *testing.T) {
	address := os.Getenv("QBIT_REDIS_ADDR")
	if address == "" {
		t.Skip("set QBIT_REDIS_ADDR to run the Redis pool isolation test")
	}
	queueName := fmt.Sprintf("fazpi-pool-isolation-%d", time.Now().UnixNano())
	redisClient := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := redisClient.Scan(ctx, cursor, "qbit:{"+queueName+"}:*", 1000).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				_ = redisClient.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = redisClient.SRem(ctx, "qbit:registry:{queues}:names", queueName).Err()
		_ = redisClient.Close()
	})

	config := defaultConfig(address)
	config.Queue = queueName
	config.TrafficProfiles = []TrafficProfile{{Account: "Casur", Agent: "Kata", PeoplePerHour: 1_000, MessagesPerPerson: 1}}
	config.TestDurationSeconds = 1
	config.WorkerReplicas = 10
	config.WorkerConcurrency = 25
	config.WorkerRedisPoolSize = 35
	config.ProcessingMillis = 0
	config.ProcessingJitterMillis = 0
	config.QueueWaitSLAms = 10_000
	config.TransientFailurePercent = 0
	config.PermanentFailurePercent = 0
	config.DuplicatePublishPercent = 0

	app := newApplication(address)
	sim, err := app.start(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if snapshot := sim.snapshot(); snapshot.Status == "starting" || snapshot.Status == "running" || snapshot.Status == "stopping" {
			sim.stop()
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				status := sim.snapshot().Status
				if status != "starting" && status != "running" && status != "stopping" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := sim.snapshot()
		switch snapshot.Status {
		case "completed":
			if snapshot.Counters.PublishedUnique != 1_000 || snapshot.Counters.Completed != 1_000 {
				t.Fatalf("unexpected terminal counters: %+v", snapshot.Counters)
			}
			if !snapshot.Publication.PublishedWithinTolerance {
				t.Fatalf("publisher missed its isolated-pool window: %+v", snapshot.Publication)
			}
			if snapshot.RedisPools.Plan.MaximumConnections != 430 {
				t.Fatalf("unexpected pool plan: %+v", snapshot.RedisPools.Plan)
			}
			if snapshot.RedisPools.Publisher.Timeouts != 0 {
				t.Fatalf("publisher pool timed out: %+v", snapshot.RedisPools.Publisher)
			}
			return
		case "failed":
			t.Fatalf("simulation failed: %s", snapshot.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
	sim.stop()
	t.Fatalf("simulation did not finish before the deadline: %+v", sim.snapshot())
}
