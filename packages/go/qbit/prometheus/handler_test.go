package prometheus

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rawars/qbit/packages/go/qbit"
)

type fakeStatsReader struct {
	stats qbit.Stats
	err   error
}

type fakeMultiStatsReader struct {
	stats     []qbit.Stats
	err       error
	requested []string
}

func (reader *fakeMultiStatsReader) QueueStats(_ context.Context, _ time.Duration, names ...string) ([]qbit.Stats, error) {
	reader.requested = append([]string(nil), names...)
	return reader.stats, reader.err
}

func (reader fakeStatsReader) Stats(context.Context, time.Duration) (qbit.Stats, error) {
	return reader.stats, reader.err
}

func TestHandlerExportsMetrics(t *testing.T) {
	handler := Handler(fakeStatsReader{stats: qbit.Stats{
		Queue:               `email"queue`,
		Waiting:             3,
		Active:              4,
		ExpiredReservations: 2,
		Paused:              true,
		WorkerReplicas:      1,
		WorkerConcurrency:   12,
		Workers: []qbit.WorkerInfo{{
			ID: "worker-1", Instance: "pod-a", Concurrency: 12,
			LastHeartbeatAt: time.Unix(100, 0),
		}},
		Totals: qbit.LifecycleCounts{Published: 10, Completed: 7, Failed: 2, Retried: 2, Recovered: 2},
	}})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	metrics := string(body)
	if !strings.Contains(metrics, `qbit_jobs_published_total{queue="email\"queue"} 10`) ||
		!strings.Contains(metrics, `qbit_jobs_waiting{queue="email\"queue"} 3`) ||
		!strings.Contains(metrics, `qbit_jobs_active{queue="email\"queue"} 4`) ||
		!strings.Contains(metrics, `qbit_reservations_expired{queue="email\"queue"} 2`) ||
		!strings.Contains(metrics, `qbit_jobs_retried_total{queue="email\"queue"} 2`) ||
		!strings.Contains(metrics, `qbit_jobs_recovered_total{queue="email\"queue"} 2`) ||
		!strings.Contains(metrics, `qbit_queue_paused{queue="email\"queue"} 1`) ||
		!strings.Contains(metrics, `qbit_worker_concurrency_slots{queue="email\"queue"} 12`) ||
		!strings.Contains(metrics, `qbit_worker_concurrency{queue="email\"queue",worker_id="worker-1",worker_instance="pod-a"} 12`) {
		t.Fatalf("metrics = %s", metrics)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("Content-Type = %q", contentType)
	}
}

func TestHandlerReturnsUnavailable(t *testing.T) {
	handler := Handler(fakeStatsReader{err: errors.New("redis unavailable")})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlerRejectsNilReader(t *testing.T) {
	response := httptest.NewRecorder()
	Handler(nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestMultiQueueHandlerExportsEveryQueue(t *testing.T) {
	reader := &fakeMultiStatsReader{stats: []qbit.Stats{
		{Queue: "emails", Waiting: 3, Totals: qbit.LifecycleCounts{Published: 10}},
		{Queue: "whatsapp", Waiting: 8, Totals: qbit.LifecycleCounts{Published: 20}},
	}}
	response := httptest.NewRecorder()
	MultiQueueHandler(reader).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metrics := response.Body.String()
	for _, sample := range []string{
		`qbit_exporter_discovered_queues 2`,
		`qbit_jobs_published_total{queue="emails"} 10`,
		`qbit_jobs_published_total{queue="whatsapp"} 20`,
		`qbit_jobs_waiting{queue="emails"} 3`,
		`qbit_jobs_waiting{queue="whatsapp"} 8`,
	} {
		if !strings.Contains(metrics, sample) {
			t.Fatalf("missing %q in metrics: %s", sample, metrics)
		}
	}
	if count := strings.Count(metrics, "# HELP qbit_jobs_published_total "); count != 1 {
		t.Fatalf("published HELP declarations = %d, want 1", count)
	}
}

func TestMultiQueueHandlerPassesFilter(t *testing.T) {
	reader := &fakeMultiStatsReader{}
	response := httptest.NewRecorder()
	MultiQueueHandler(reader, "emails", "whatsapp").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Join(reader.requested, ",") != "emails,whatsapp" {
		t.Fatalf("requested queues = %v", reader.requested)
	}
	if !strings.Contains(response.Body.String(), "qbit_exporter_discovered_queues 0") {
		t.Fatalf("metrics = %s", response.Body.String())
	}
}

func TestMultiQueueHandlerReturnsUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	MultiQueueHandler(&fakeMultiStatsReader{err: errors.New("redis unavailable")}).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/metrics", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	response = httptest.NewRecorder()
	MultiQueueHandler(nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil reader status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}
