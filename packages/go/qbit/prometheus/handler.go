// Package prometheus exposes Qbit queue telemetry in the Prometheus text
// exposition format without requiring an additional metrics dependency.
package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rawars/qbit/packages/go/qbit"
)

const statsWindow = time.Minute

// StatsReader is implemented by qbit.Queue.
type StatsReader interface {
	Stats(context.Context, time.Duration) (qbit.Stats, error)
}

// MultiStatsReader discovers and reads several queues. qbit.Client implements
// this interface.
type MultiStatsReader interface {
	QueueStats(context.Context, time.Duration, ...string) ([]qbit.Stats, error)
}

// Handler returns a read-only Prometheus metrics endpoint.
func Handler(queue StatsReader) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if queue == nil {
			http.Error(writer, "queue is not configured", http.StatusServiceUnavailable)
			return
		}
		stats, err := queue.Stats(request.Context(), statsWindow)
		if err != nil {
			http.Error(writer, "queue telemetry is unavailable", http.StatusServiceUnavailable)
			return
		}
		writeResponse(writer, []qbit.Stats{stats})
	})
}

// MultiQueueHandler discovers all registered queues on every Prometheus scrape.
// queueNames optionally limits collection to an explicit set.
func MultiQueueHandler(reader MultiStatsReader, queueNames ...string) http.Handler {
	filter := append([]string(nil), queueNames...)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if reader == nil {
			http.Error(writer, "queue discovery is not configured", http.StatusServiceUnavailable)
			return
		}
		stats, err := reader.QueueStats(request.Context(), statsWindow, filter...)
		if err != nil {
			http.Error(writer, "queue telemetry is unavailable", http.StatusServiceUnavailable)
			return
		}
		writeResponse(writer, stats)
	})
}

type queueMetric struct {
	name       string
	help       string
	metricType string
	value      func(qbit.Stats) any
}

var queueMetrics = []queueMetric{
	{"qbit_jobs_published_total", "Jobs published to the queue.", "counter", func(stats qbit.Stats) any { return stats.Totals.Published }},
	{"qbit_jobs_reserved_total", "Job reservations created by workers.", "counter", func(stats qbit.Stats) any { return stats.Totals.Reserved }},
	{"qbit_jobs_completed_total", "Jobs completed successfully.", "counter", func(stats qbit.Stats) any { return stats.Totals.Completed }},
	{"qbit_jobs_failed_total", "Processing attempts that ended with a failure.", "counter", func(stats qbit.Stats) any { return stats.Totals.Failed }},
	{"qbit_jobs_stalled_total", "Expired reservations recovered by Qbit.", "counter", func(stats qbit.Stats) any { return stats.Totals.Stalled }},
	{"qbit_jobs_retried_total", "Failed processing attempts returned to the queue.", "counter", func(stats qbit.Stats) any { return stats.Totals.Retried }},
	{"qbit_jobs_recovered_total", "Retried jobs later completed successfully.", "counter", func(stats qbit.Stats) any { return stats.Totals.Recovered }},
	{"qbit_jobs_waiting", "Jobs currently waiting to be reserved.", "gauge", func(stats qbit.Stats) any { return stats.Waiting }},
	{"qbit_jobs_active", "Jobs currently reserved by workers.", "gauge", func(stats qbit.Stats) any { return stats.Active }},
	{"qbit_ready_groups", "Groups currently available for reservation.", "gauge", func(stats qbit.Stats) any { return stats.ReadyGroups }},
	{"qbit_queue_paused", "Whether new reservations are paused for the queue.", "gauge", func(stats qbit.Stats) any { return boolValue(stats.Paused) }},
	{"qbit_queue_wait_seconds", "Mean queue wait in the rolling one-minute window.", "gauge", func(stats qbit.Stats) any { return stats.AverageQueueWaitMillis / 1000 }},
	{"qbit_processing_duration_seconds", "Mean processing duration in the rolling one-minute window.", "gauge", func(stats qbit.Stats) any { return stats.AverageProcessingMillis / 1000 }},
	{"qbit_metrics_window_truncated", "Whether the rolling window exceeded the retained event sample.", "gauge", func(stats qbit.Stats) any { return boolValue(stats.WindowMayBeEventTruncated) }},
	{"qbit_worker_replicas_active", "Worker processes or pods with a live heartbeat.", "gauge", func(stats qbit.Stats) any { return stats.WorkerReplicas }},
	{"qbit_worker_concurrency_slots", "Total parallel worker slots across live replicas.", "gauge", func(stats qbit.Stats) any { return stats.WorkerConcurrency }},
}

func writeResponse(writer http.ResponseWriter, allStats []qbit.Stats) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintln(writer, "# HELP qbit_exporter_discovered_queues Queues included in the current scrape.")
	_, _ = fmt.Fprintln(writer, "# TYPE qbit_exporter_discovered_queues gauge")
	_, _ = fmt.Fprintf(writer, "qbit_exporter_discovered_queues %d\n", len(allStats))
	for _, metric := range queueMetrics {
		_, _ = fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, metric.metricType)
		for _, stats := range allStats {
			label := `queue="` + escapeLabel(stats.Queue) + `"`
			_, _ = fmt.Fprintf(writer, "%s{%s} %v\n", metric.name, label, metric.value(stats))
		}
	}

	_, _ = fmt.Fprintln(writer, "# HELP qbit_worker_info Metadata for a worker process with a live heartbeat.")
	_, _ = fmt.Fprintln(writer, "# TYPE qbit_worker_info gauge")
	_, _ = fmt.Fprintln(writer, "# HELP qbit_worker_concurrency Configured concurrency for one worker process.")
	_, _ = fmt.Fprintln(writer, "# TYPE qbit_worker_concurrency gauge")
	_, _ = fmt.Fprintln(writer, "# HELP qbit_worker_last_heartbeat_timestamp_seconds Unix timestamp of a worker's latest heartbeat.")
	_, _ = fmt.Fprintln(writer, "# TYPE qbit_worker_last_heartbeat_timestamp_seconds gauge")
	for _, stats := range allStats {
		label := `queue="` + escapeLabel(stats.Queue) + `"`
		for _, worker := range stats.Workers {
			workerLabels := label + `,worker_id="` + escapeLabel(worker.ID) + `",worker_instance="` + escapeLabel(worker.Instance) + `"`
			_, _ = fmt.Fprintf(writer, "qbit_worker_info{%s} 1\n", workerLabels)
			_, _ = fmt.Fprintf(writer, "qbit_worker_concurrency{%s} %d\n", workerLabels, worker.Concurrency)
			_, _ = fmt.Fprintf(writer, "qbit_worker_last_heartbeat_timestamp_seconds{%s} %v\n", workerLabels, float64(worker.LastHeartbeatAt.UnixMilli())/1000)
		}
	}
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func boolValue(value bool) int {
	if value {
		return 1
	}
	return 0
}
