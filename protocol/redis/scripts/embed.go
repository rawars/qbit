// Package scripts exposes the canonical Qbit Redis protocol scripts.
package scripts

import _ "embed"

var (
	// Add atomically creates or resolves an idempotent job.
	//go:embed add.lua
	Add string
	// Reserve recovers expired work and reserves one job from the next group.
	//go:embed reserve.lua
	Reserve string
	// Finish acknowledges a job and releases its group.
	//go:embed finish.lua
	Finish string
	// Retry records a failed attempt and returns its job to the head of the group.
	//go:embed retry.lua
	Retry string
	// Renew extends a reservation owned by its token.
	//go:embed renew.lua
	Renew string
	// RegisterWorker creates a leased worker registration.
	//go:embed register_worker.lua
	RegisterWorker string
	// HeartbeatWorker refreshes a worker registration lease.
	//go:embed heartbeat_worker.lua
	HeartbeatWorker string
	// UnregisterWorker removes a worker registration.
	//go:embed unregister_worker.lua
	UnregisterWorker string
	// Pause prevents new reservations while preserving queued and active jobs.
	//go:embed pause.lua
	Pause string
	// Resume allows reservations and wakes blocked workers.
	//go:embed resume.lua
	Resume string
)
