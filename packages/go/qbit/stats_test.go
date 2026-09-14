package qbit

import (
	"testing"
	"time"
)

func TestMetricsBucketsAreBounded(t *testing.T) {
	keys, err := newQueueKeys("metrics-test")
	if err != nil {
		t.Fatal(err)
	}
	queue := &Queue{keys: keys}
	now := time.Date(2026, 9, 14, 15, 0, 2, 0, time.UTC)

	buckets, start, truncated := queue.metricsBuckets(now, 24*time.Hour)
	if !truncated {
		t.Fatal("24-hour metric window was not marked truncated")
	}
	if len(buckets) > int(metricsAggregateWindow/metricsBucketWidth)+1 {
		t.Fatalf("bucket count = %d, exceeds bounded maximum", len(buckets))
	}
	if sampled := now.Sub(start); sampled < metricsAggregateWindow || sampled >= metricsAggregateWindow+metricsBucketWidth {
		t.Fatalf("sampled window = %v, want [%v, %v)", sampled, metricsAggregateWindow, metricsAggregateWindow+metricsBucketWidth)
	}
}

func TestLegacyMetricsMarkRollingWindowIncomplete(t *testing.T) {
	now := time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)
	legacy := map[string]string{"initialized_at": "1000", "published": "42"}
	if !aggregateWindowStartsAfterRequested(legacy, now.Add(-time.Minute)) {
		t.Fatal("legacy metrics without aggregates were reported as complete")
	}

	current := map[string]string{
		"initialized_at":            "1000",
		"aggregates_initialized_at": "1000",
	}
	if aggregateWindowStartsAfterRequested(current, now.Add(-time.Minute)) {
		t.Fatal("current metrics were reported as incomplete")
	}
}
