// Package prometheus exposes Qbit telemetry in the Prometheus text exposition
// format. Most deployments use the qbit-metrics executable instead of
// embedding this handler in an application.
package prometheus

import (
	"net/http"

	core "github.com/rawars/qbit/packages/go/qbit/prometheus"
)

type (
	StatsReader      = core.StatsReader
	MultiStatsReader = core.MultiStatsReader
)

// Handler exposes one queue. Prefer MultiQueueHandler for new deployments.
func Handler(queue StatsReader) http.Handler { return core.Handler(queue) }

// MultiQueueHandler discovers every registered queue. queueNames optionally
// limits the exported queues.
func MultiQueueHandler(reader MultiStatsReader, queueNames ...string) http.Handler {
	return core.MultiQueueHandler(reader, queueNames...)
}
