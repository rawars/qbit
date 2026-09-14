# Redis protocol v1

Qbit uses Redis Cluster hash tags so every key belonging to one queue occupies
the same slot. For queue `emails`, the base key is `qbit:{emails}`.

## Keys

| Key | Type | Purpose |
| --- | --- | --- |
| `<base>:id` | string | Monotonic job identifier |
| `<base>:job:<id>` | hash | Job payload, state, name and group |
| `<base>:group:<group>:wait` | list | FIFO jobs waiting within one group |
| `<base>:groups:ready` | list | Round-robin ring of runnable groups |
| `<base>:groups:ready:set` | set | Prevents duplicate ring entries |
| `<base>:groups:active` | hash | Active group to job-ID ownership map |
| `<base>:jobs:active` | sorted set | Active jobs scored by lock expiration |
| `<base>:job:<id>:lock` | string | Reservation token with a TTL |
| `<base>:marker` | sorted set | Wakes consumers waiting for runnable work |
| `<base>:events` | stream | Bounded lifecycle event log |
| `<base>:metrics` | hash | Monotonic lifecycle counters for observability |
| `<base>:paused` | string | Distributed switch that prevents new reservations |
| `<base>:workers` | sorted set | Worker IDs scored by heartbeat lease expiry |
| `<base>:worker:<id>` | hash | Worker instance, concurrency and heartbeat data |

The deployment also contains a low-traffic global catalog:

| Key | Type | Purpose |
| --- | --- | --- |
| `qbit:registry:{queues}:names` | set | Queue names discovered by the metrics exporter |

Each queue handle adds its validated name to this set on its first publish,
reservation or worker registration. This operation happens once per queue
handle rather than once per job. The catalog deliberately uses its own Redis
Cluster hash tag and is not part of queue state-transition scripts.

## Invariants

1. A group occurs at most once in the ready ring.
2. An active group cannot be selected for another job.
3. Only the owner of a lock token can complete or fail its job.
4. Releasing a group appends it to the end of the ready ring when more work
   exists. This is what provides round-robin fairness.
5. All state transitions involving multiple keys execute in Lua.
6. A paused queue accepts publications but creates no new reservations; active
   reservations may complete, fail, retry or renew normally.

Before reserving new work, consumers reclaim expired entries from the active
job index. The abandoned job is returned to the front of its group so FIFO is
preserved. `Renew` atomically extends both the lock TTL and its active-index
deadline.

`Retry` records a failed processing attempt and atomically returns the same job
to the front of its group. A later successful completion is counted as a
recovery. `Fail` remains terminal and releases the group without requeueing the
job.

`Pause` and `Resume` are idempotent administrative operations. Their state is
stored in the queue hash slot and is therefore observed by every worker replica.
Resuming wakes blocked consumers when the ready ring contains work.

Producer retries are made idempotent with a caller-supplied job ID. Automatic
IDs use the reserved `auto-` prefix.

## Delivery semantics

Delivery is at least once: an expired reservation is recovered and may execute
again. Consumers must therefore be idempotent. Completed job hashes expire
after 24 hours and failed job hashes after 7 days by default.

## Canonical scripts

The Lua files in `protocol/redis/scripts` define the cross-language protocol.
SDKs embed these files instead of maintaining independent implementations.

## Observability

The metrics hash stores monotonic `published`, `reserved`, `completed`,
`failed`, `retried`, `recovered`, and `stalled` counters. Rates and latency are
derived from the bounded event stream over a caller-selected time window. An `active` event
includes `wait_ms`; terminal events include `processing_ms`. This makes one
queue observable across every producer and worker replica without requiring
the processes to communicate directly.

Worker processes register a leased heartbeat. The registration disappears
after its TTL when a process crashes or loses connectivity, allowing
Prometheus to distinguish idle workers from active jobs. One registration
represents one process or pod; its concurrency reports the number of parallel
worker slots inside that replica.

The `qbit-metrics` server reads the global queue catalog on each Prometheus
scrape and exports every queue with a `queue` label. When it starts with an
empty catalog, it performs one compatibility scan for metrics hashes created
before the catalog existed and imports those queue names.
