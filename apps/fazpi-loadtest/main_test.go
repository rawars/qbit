package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	qbit "github.com/rawars/qbit/packages/go"
)

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
