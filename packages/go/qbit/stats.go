package qbit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxRecentEvents = 10_000

// LifecycleCounts contains queue transitions observed since metrics were
// initialized. Counters are shared by every client using the same queue.
type LifecycleCounts struct {
	Published int64 `json:"published"`
	Reserved  int64 `json:"reserved"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Stalled   int64 `json:"stalled"`
	Retried   int64 `json:"retried"`
	Recovered int64 `json:"recovered"`
}

// LifecycleRates contains average transitions per second during a window.
type LifecycleRates struct {
	Published float64 `json:"published"`
	Reserved  float64 `json:"reserved"`
	Completed float64 `json:"completed"`
	Failed    float64 `json:"failed"`
	Stalled   float64 `json:"stalled"`
	Retried   float64 `json:"retried"`
	Recovered float64 `json:"recovered"`
}

// Stats is a point-in-time operational view of a queue.
type Stats struct {
	Queue                     string          `json:"queue"`
	CapturedAt                time.Time       `json:"captured_at"`
	WindowSeconds             float64         `json:"window_seconds"`
	MetricsInitializedAt      time.Time       `json:"metrics_initialized_at,omitempty"`
	Totals                    LifecycleCounts `json:"totals"`
	Rates                     LifecycleRates  `json:"rates_per_second"`
	Waiting                   int64           `json:"waiting"`
	Active                    int64           `json:"active"`
	ReadyGroups               int64           `json:"ready_groups"`
	Paused                    bool            `json:"paused"`
	WorkerReplicas            int             `json:"worker_replicas"`
	WorkerConcurrency         int             `json:"worker_concurrency"`
	Workers                   []WorkerInfo    `json:"workers"`
	AverageQueueWaitMillis    float64         `json:"average_queue_wait_ms"`
	AverageProcessingMillis   float64         `json:"average_processing_ms"`
	OldestEventAvailableAt    time.Time       `json:"oldest_event_available_at,omitempty"`
	WindowMayBeEventTruncated bool            `json:"window_may_be_event_truncated"`
}

// Event is one queue lifecycle transition.
type Event struct {
	ID               string    `json:"id"`
	Type             string    `json:"type"`
	JobID            string    `json:"job_id"`
	Group            string    `json:"group"`
	At               time.Time `json:"at"`
	QueueWaitMillis  int64     `json:"queue_wait_ms,omitempty"`
	ProcessingMillis int64     `json:"processing_ms,omitempty"`
}

// Stats returns aggregate counters, current pressure, and rolling rates. The
// event window must be positive.
func (queue *Queue) Stats(ctx context.Context, window time.Duration) (Stats, error) {
	if window <= 0 {
		return Stats{}, errors.New("qbit: stats window must be positive")
	}
	now := time.Now()
	pipe := queue.client.Pipeline()
	metricsCommand := pipe.HGetAll(ctx, queue.keys.metrics())
	activeCommand := pipe.HLen(ctx, queue.keys.active())
	readyGroupsCommand := pipe.LLen(ctx, queue.keys.ready())
	pausedCommand := pipe.Exists(ctx, queue.keys.paused())
	pipe.ZRemRangeByScore(ctx, queue.keys.workers(), "-inf", strconv.FormatInt(now.UnixMilli(), 10))
	workersCommand := pipe.ZRangeByScoreWithScores(ctx, queue.keys.workers(), &redis.ZRangeBy{
		Min: strconv.FormatInt(now.UnixMilli()+1, 10), Max: "+inf",
	})
	eventsCommand := pipe.XRangeN(ctx, queue.keys.events(), streamID(now.Add(-window)), "+", maxRecentEvents)
	oldestCommand := pipe.XRangeN(ctx, queue.keys.events(), "-", "+", 1)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return Stats{}, fmt.Errorf("qbit: read queue stats: %w", err)
	}

	metrics, err := metricsCommand.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Stats{}, fmt.Errorf("qbit: read metric counters: %w", err)
	}
	active, err := activeCommand.Result()
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: read active jobs: %w", err)
	}
	readyGroups, err := readyGroupsCommand.Result()
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: read ready groups: %w", err)
	}
	paused, err := pausedCommand.Result()
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: read queue pause state: %w", err)
	}
	events, err := eventsCommand.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Stats{}, fmt.Errorf("qbit: read metric window: %w", err)
	}
	workerMembers, err := workersCommand.Result()
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: read worker registrations: %w", err)
	}

	stats := Stats{
		Queue:         queueName(queue.keys.base),
		CapturedAt:    now.UTC(),
		WindowSeconds: window.Seconds(),
		Totals: LifecycleCounts{
			Published: metricInt(metrics, "published"),
			Reserved:  metricInt(metrics, "reserved"),
			Completed: metricInt(metrics, "completed"),
			Failed:    metricInt(metrics, "failed"),
			Stalled:   metricInt(metrics, "stalled"),
			Retried:   metricInt(metrics, "retried"),
			Recovered: metricInt(metrics, "recovered"),
		},
		Active:      active,
		ReadyGroups: readyGroups,
		Paused:      paused == 1,
		Workers:     make([]WorkerInfo, 0, len(workerMembers)),
	}
	if len(workerMembers) > 0 {
		workerPipe := queue.client.Pipeline()
		workerCommands := make([]*redis.MapStringStringCmd, 0, len(workerMembers))
		workerExpirations := make([]int64, 0, len(workerMembers))
		for _, member := range workerMembers {
			workerID := stringValue(member.Member)
			workerCommands = append(workerCommands, workerPipe.HGetAll(ctx, queue.keys.worker(workerID)))
			workerExpirations = append(workerExpirations, int64(member.Score))
		}
		if _, workerErr := workerPipe.Exec(ctx); workerErr != nil && !errors.Is(workerErr, redis.Nil) {
			return Stats{}, fmt.Errorf("qbit: read worker details: %w", workerErr)
		}
		for index, command := range workerCommands {
			values, workerErr := command.Result()
			if workerErr != nil && !errors.Is(workerErr, redis.Nil) {
				return Stats{}, fmt.Errorf("qbit: read worker detail: %w", workerErr)
			}
			info, valid := workerInfo(values, workerExpirations[index])
			if !valid {
				continue
			}
			stats.Workers = append(stats.Workers, info)
			stats.WorkerConcurrency += info.Concurrency
		}
		stats.WorkerReplicas = len(stats.Workers)
	}
	if initializedAt := metricInt(metrics, "initialized_at"); initializedAt > 0 {
		stats.MetricsInitializedAt = time.UnixMilli(initializedAt).UTC()
	}
	// A retry records a failed attempt and creates another waiting attempt for
	// the same logical job. Including it here keeps the backlog exact for both
	// terminal failures and transient failures.
	stats.Waiting = stats.Totals.Published + stats.Totals.Retried - stats.Totals.Completed - stats.Totals.Failed - active
	if stats.Waiting < 0 {
		stats.Waiting = 0
	}

	var waitTotal, processingTotal int64
	var waitSamples, processingSamples int64
	windowCounts := LifecycleCounts{}
	for _, message := range events {
		event := eventFromMessage(message)
		switch event.Type {
		case "waiting":
			windowCounts.Published++
		case "active":
			windowCounts.Reserved++
			waitTotal += event.QueueWaitMillis
			waitSamples++
		case "completed":
			windowCounts.Completed++
			processingTotal += event.ProcessingMillis
			processingSamples++
		case "failed":
			windowCounts.Failed++
			processingTotal += event.ProcessingMillis
			processingSamples++
		case "stalled":
			windowCounts.Stalled++
		case "retried":
			windowCounts.Retried++
		case "recovered":
			windowCounts.Recovered++
		}
	}
	stats.Rates = rates(windowCounts, window.Seconds())
	if waitSamples > 0 {
		stats.AverageQueueWaitMillis = float64(waitTotal) / float64(waitSamples)
	}
	if processingSamples > 0 {
		stats.AverageProcessingMillis = float64(processingTotal) / float64(processingSamples)
	}
	if oldest, oldestErr := oldestCommand.Result(); oldestErr == nil && len(oldest) == 1 {
		stats.OldestEventAvailableAt = streamTime(oldest[0].ID)
		stats.WindowMayBeEventTruncated = len(events) == maxRecentEvents && stats.OldestEventAvailableAt.After(now.Add(-window))
	}
	return stats, nil
}

// RecentEvents returns newest queue events first.
func (queue *Queue) RecentEvents(ctx context.Context, count int64) ([]Event, error) {
	if count <= 0 || count > maxRecentEvents {
		return nil, fmt.Errorf("qbit: event count must be between 1 and %d", maxRecentEvents)
	}
	messages, err := queue.client.XRevRangeN(ctx, queue.keys.events(), "+", "-", count).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("qbit: read recent events: %w", err)
	}
	events := make([]Event, 0, len(messages))
	for _, message := range messages {
		events = append(events, eventFromMessage(message))
	}
	return events, nil
}

func rates(counts LifecycleCounts, seconds float64) LifecycleRates {
	return LifecycleRates{
		Published: float64(counts.Published) / seconds,
		Reserved:  float64(counts.Reserved) / seconds,
		Completed: float64(counts.Completed) / seconds,
		Failed:    float64(counts.Failed) / seconds,
		Stalled:   float64(counts.Stalled) / seconds,
		Retried:   float64(counts.Retried) / seconds,
		Recovered: float64(counts.Recovered) / seconds,
	}
}

func eventFromMessage(message redis.XMessage) Event {
	return Event{
		ID:               message.ID,
		Type:             stringValue(message.Values["event"]),
		JobID:            stringValue(message.Values["job_id"]),
		Group:            stringValue(message.Values["group"]),
		At:               streamTime(message.ID),
		QueueWaitMillis:  valueInt(message.Values["wait_ms"]),
		ProcessingMillis: valueInt(message.Values["processing_ms"]),
	}
}

func streamID(at time.Time) string { return strconv.FormatInt(at.UnixMilli(), 10) + "-0" }

func streamTime(id string) time.Time {
	milliseconds, _, _ := strings.Cut(id, "-")
	value, _ := strconv.ParseInt(milliseconds, 10, 64)
	return time.UnixMilli(value).UTC()
}

func metricInt(metrics map[string]string, key string) int64 {
	value, _ := strconv.ParseInt(metrics[key], 10, 64)
	return value
}

func valueInt(value any) int64 {
	parsed, _ := strconv.ParseInt(stringValue(value), 10, 64)
	return parsed
}

func queueName(base string) string {
	const prefix = "qbit:{"
	if len(base) > len(prefix)+1 {
		return base[len(prefix) : len(base)-1]
	}
	return base
}
