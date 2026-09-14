package qbit

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkAdd(b *testing.B) {
	queue, ctx := newBenchmarkQueue(b)
	payload := []byte(`{"event":"benchmark"}`)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := queue.Add(ctx, "benchmark", payload,
			WithGroup("group-"+strconv.Itoa(index%128)),
			WithJobID("benchmark-add-"+strconv.Itoa(index)),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupedLifecycle(b *testing.B) {
	queue, ctx := newBenchmarkQueue(b)
	payload := []byte(`{"event":"benchmark"}`)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := queue.Add(ctx, "benchmark", payload, WithGroup("group-"+strconv.Itoa(index%128)))
		if err != nil {
			b.Fatal(err)
		}
		job, err := queue.Reserve(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := queue.Complete(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkQueue(b *testing.B) (*Queue, context.Context) {
	b.Helper()
	address := os.Getenv("QBIT_REDIS_ADDR")
	if address == "" {
		b.Skip("set QBIT_REDIS_ADDR to run Redis benchmarks")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address})
	queue, err := NewQueue(fmt.Sprintf("benchmark-%d", time.Now().UnixNano()), client)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		keys, _ := client.Keys(ctx, queue.keys.base+":*").Result()
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		_ = client.Close()
	})
	return queue, ctx
}
