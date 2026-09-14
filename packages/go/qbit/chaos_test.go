//go:build chaos

package qbit

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestChaosConcurrentWorkerCrashes(t *testing.T) {
	queue, setupContext := newIntegrationQueue(t)
	const (
		groupCount   = 8
		jobsPerGroup = 3
		totalJobs    = groupCount * jobsPerGroup
	)
	for group := 0; group < groupCount; group++ {
		for job := 0; job < jobsPerGroup; job++ {
			_, err := queue.Add(setupContext, "chaos", nil, WithGroup(string(rune('a'+group))))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var (
		completed  atomic.Int64
		crashCount atomic.Int64
		crashed    sync.Map
		finished   sync.Map
		workers    sync.WaitGroup
	)
	for range groupCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for completed.Load() < totalJobs {
				job, err := queue.ReserveBlocking(ctx, 250*time.Millisecond, WithLockTTL(40*time.Millisecond))
				if errors.Is(err, ErrNoJob) {
					continue
				}
				if err != nil {
					return
				}
				if _, seen := crashed.LoadOrStore(job.ID, struct{}{}); !seen && crashCount.Add(1) <= 4 {
					continue // Simulate a process dying without ACK or lock renewal.
				}
				if err := queue.Complete(ctx, job); err != nil {
					if errors.Is(err, ErrReservationLost) {
						continue
					}
					return
				}
				if _, duplicate := finished.LoadOrStore(job.ID, struct{}{}); !duplicate {
					completed.Add(1)
				}
			}
		}()
	}
	workers.Wait()
	if got := completed.Load(); got != totalJobs {
		t.Fatalf("completed %d/%d jobs after simulated crashes: %v", got, totalJobs, ctx.Err())
	}
}

func TestChaosRedisDropsConnections(t *testing.T) {
	queue, ctx := newIntegrationQueue(t)
	added, err := queue.Add(ctx, "chaos", []byte("survives"), WithJobID("connection-drop"))
	if err != nil {
		t.Fatal(err)
	}

	admin := redis.NewClient(&redis.Options{Addr: os.Getenv("QBIT_REDIS_ADDR")})
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err(); err != nil {
		t.Fatal(err)
	}

	job, err := queue.ReserveBlocking(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != added.ID {
		t.Fatalf("reserved %q after reconnect, want %q", job.ID, added.ID)
	}
	if err := queue.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
}
