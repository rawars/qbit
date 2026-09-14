package qbit

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewClientValidatesRedisOptions(t *testing.T) {
	if _, err := NewClient(ClientOptions{}); err == nil {
		t.Fatal("NewClient without address error = nil")
	}
	if _, err := NewClient(ClientOptions{Redis: RedisOptions{Address: "localhost:6379", DB: -1}}); err == nil {
		t.Fatal("NewClient with negative DB error = nil")
	}
	if _, err := NewClient(ClientOptions{Redis: RedisOptions{Address: "localhost:6379", PoolSize: -1}}); err == nil {
		t.Fatal("NewClient with negative pool size error = nil")
	}
}

func TestClientCreatesQueueAndClosesIdempotently(t *testing.T) {
	client, err := NewClient(ClientOptions{Redis: RedisOptions{Address: "localhost:6379"}})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := client.Queue("emails")
	if err != nil {
		t.Fatal(err)
	}
	if queue == nil {
		t.Fatal("Queue returned nil")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNilClientQueueAndClose(t *testing.T) {
	var client *Client
	if _, err := client.Queue("emails"); err == nil {
		t.Fatal("nil Client.Queue error = nil")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("nil Client.Close: %v", err)
	}
}

func TestQueueNameFromMetricsKey(t *testing.T) {
	for _, test := range []struct {
		key  string
		name string
		ok   bool
	}{
		{key: "qbit:{whatsapp}:metrics", name: "whatsapp", ok: true},
		{key: "qbit:{team:email}:metrics", name: "team:email", ok: true},
		{key: "qbit:{whatsapp}:workers", ok: false},
		{key: "other:{whatsapp}:metrics", ok: false},
	} {
		name, ok := queueNameFromMetricsKey(test.key)
		if name != test.name || ok != test.ok {
			t.Fatalf("queueNameFromMetricsKey(%q) = %q, %v", test.key, name, ok)
		}
	}
}

func TestClientRegistersQueuesAndFindsLegacyMetrics(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	client := &Client{redis: queue.client}

	registered, err := queue.client.SIsMember(ctx, queueRegistryKey, queue.name).Result()
	if err != nil {
		t.Fatal(err)
	}
	if registered {
		t.Fatal("new queue was registered before its first operation")
	}
	if _, err := queue.Publish(ctx, "work", nil); err != nil {
		t.Fatal(err)
	}
	registered, err = queue.client.SIsMember(ctx, queueRegistryKey, queue.name).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !registered {
		t.Fatal("queue was not registered by Publish")
	}

	secondName := fmt.Sprintf("second-%d", time.Now().UnixNano())
	second, err := NewQueue(secondName, queue.client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Publish(ctx, "work", nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, _ := queue.client.Keys(context.Background(), second.keys.base+":*").Result()
		if len(keys) > 0 {
			_ = queue.client.Del(context.Background(), keys...).Err()
		}
		_ = queue.client.SRem(context.Background(), queueRegistryKey, secondName).Err()
	})

	stats, err := client.QueueStats(ctx, time.Minute, secondName, queue.name, secondName)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || !containsStatsQueue(stats, queue.name) || !containsStatsQueue(stats, secondName) {
		t.Fatalf("QueueStats queues = %+v", stats)
	}

	legacyName := fmt.Sprintf("legacy-%d", time.Now().UnixNano())
	legacyKey := "qbit:{" + legacyName + "}:metrics"
	if err := queue.client.HSet(ctx, legacyKey, "published", 7).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = queue.client.Del(context.Background(), legacyKey).Err()
		_ = queue.client.SRem(context.Background(), queueRegistryKey, legacyName).Err()
	})
	discovered, err := client.legacyQueueNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !containsQueue(discovered, legacyName) {
		t.Fatalf("legacy queue missing from discovery: %v", discovered)
	}
}

func containsQueue(names []string, wanted string) bool {
	for _, name := range names {
		if name == wanted {
			return true
		}
	}
	return false
}

func containsStatsQueue(stats []Stats, wanted string) bool {
	for _, queueStats := range stats {
		if queueStats.Queue == wanted {
			return true
		}
	}
	return false
}
