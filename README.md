# Qbit

Qbit is a Redis-backed messaging monorepo. The first SDK is written in Go and
focuses on durable work queues with fair, group-aware processing.

## Group semantics

- Jobs are FIFO within a group.
- Ready groups are selected in round-robin order.
- At most one job per group is active at a time.
- A job without an explicit group receives its own group, so unrelated jobs can
  still run concurrently.
- Completing or failing a job releases its group and schedules the next job in
  that group.

These rules are part of the `qbit:v1` Redis protocol, rather than a feature to
be added after the basic queue has shipped.

## Quick start

```bash
docker compose up -d
QBIT_REDIS_ADDR=localhost:6379 go test ./...
```

Install the SDK in another Go module:

```bash
go get github.com/rawars/qbit/packages/go@latest
```

```go
import qbit "github.com/rawars/qbit/packages/go"

client, err := qbit.NewClient(qbit.ClientOptions{
    Redis: qbit.RedisOptions{Address: "localhost:6379"},
})
if err != nil {
    panic(err)
}
defer client.Close()

queue, err := client.Queue("messages")
if err != nil {
    panic(err)
}

job, err := queue.Publish(ctx, "send-message", []byte(`{"text":"hello"}`),
    qbit.WithGroup("conversation-42"),
    qbit.WithJobID("message-123"),
)
```

Consume with a managed worker:

```go
worker, err := qbit.NewWorker(queue, func(ctx context.Context, job *qbit.Job) error {
    return deliver(ctx, job.Payload)
}, qbit.WorkerOptions{
    Concurrency: 50,
    Retry: qbit.RetryPolicy{
        MaxAttempts: 5,
        Backoff: qbit.ExponentialBackoff(time.Second, time.Minute),
    },
})
if err != nil {
    panic(err)
}

if err := worker.Run(ctx); err != nil {
    panic(err)
}
```

The worker owns reservations, automatic lock renewal, retries, acknowledgments,
heartbeats and graceful shutdown. Return `qbit.Permanent(err)` from a handler
when retrying cannot succeed. `WithJobID` makes producer retries idempotent.
Completed jobs are retained for 24 hours and failed jobs for 7 days by default;
configure both with `WithRetention`.

See [the Go SDK guide](packages/go/README.md) for installation, producer and
worker examples, local development and release versioning.

## Installable components

Qbit ships three independently consumable components:

- `github.com/rawars/qbit/packages/go`: the Go SDK embedded in producers and
  workers;
- `qbit-metrics`: one central Prometheus exporter that discovers every Qbit
  queue in Redis;
- `qbitctl`: the administrator CLI for health, queues, statistics, workers,
  events and distributed pause/resume.

Install the binaries from the same release as the SDK:

```bash
go install github.com/rawars/qbit/cmd/qbitctl@v0.1.0
go install github.com/rawars/qbit/cmd/qbit-metrics@v0.1.0
```

The exporter is also published as
`ghcr.io/rawars/qbit-metrics:<version>`. See the
[qbitctl guide](cmd/qbitctl/README.md) and
[qbit-metrics guide](cmd/qbit-metrics/README.md).

## Repository layout

```text
docs/protocol/       Redis protocol documentation
docs/observability/  Prometheus and Grafana development stack
docs/incidents/      Reproducible incidents and known limits
apps/qbit-demo/      Load producer and worker demo application
apps/fazpi-loadtest/ Fazpi-oriented load laboratory with a web dashboard
packages/go/         Stable Go SDK import
packages/go/qbit/    Go SDK implementation
protocol/redis/      Canonical cross-SDK Redis scripts
cmd/qbitctl/          Installable administration CLI
cmd/qbit-metrics/     Installable exporter and production Docker image
```

BullMQ is used as an architectural reference only. Qbit has its own keyspace,
wire format and public API.

## Fazpi load laboratory

The local laboratory simulates simultaneous traffic from multiple Fazpi
accounts and agents using the real Qbit Go SDK. Each agent has an independent
people-per-hour rate and messages-per-person value while every conversation
retains its own FIFO group. A configurable 12-window arrival curve creates
peaks and valleys throughout the simulated hour. Its web dashboard configures
time acceleration, publishers, worker replicas,
concurrency, processing time, duplicate messages, failures, retry policy and a
per-agent queue-wait SLA. It validates FIFO processing, per-thread mutual
exclusion, idempotency, agent isolation and complete queue drainage.

With Redis running on `127.0.0.1:6379`:

```powershell
$env:QBIT_REDIS_ADDR = "127.0.0.1:6379"
go run ./apps/fazpi-loadtest
```

Open `http://127.0.0.1:8080`. See the
[Fazpi load laboratory guide](apps/fazpi-loadtest/README.md) for the available
checks and configuration.

## Quality gates

With Redis running locally, the same checks used by CI can be executed with:

```bash
make integration   # Redis integration tests with the race detector
make audit         # staticcheck, govulncheck and go vet
make coverage      # SDK coverage profile and minimum threshold
make fuzz          # coverage-guided native Go fuzzing
make benchmark     # enqueue and grouped-lifecycle throughput
make chaos         # worker crashes, lock recovery and connection drops
```

`REDIS_ADDR` selects Redis and defaults to `127.0.0.1:6379`. `FUZZ_TIME` and
`COVERAGE_MIN` control fuzz duration and the required SDK coverage percentage.
CI runs `make integration` across the complete repository before calculating
coverage for the installable packages under `packages/go`. This keeps demos and
binary entry points from diluting the library quality signal.

## Observability

Qbit records shared lifecycle counters and a bounded event stream in Redis.
They aggregate producers and workers across every application replica using
the same queue. The Prometheus exporter provides published, reserved,
completed, failed, retried, recovered and stalled totals, plus waiting jobs,
active jobs, ready groups, queue wait and processing duration.

Start the complete local stack:

```bash
docker compose up -d
make observability
```

El Redis de este Compose está perfilado para transporte Qbit con AOF
`everysec`, `noeviction`, snapshots RDB automáticos desactivados y límites de
memoria configurables. Consulta la
[guía de Redis dedicado](docs/redis-dedicated-transport.md) antes de trasladar
estos valores a un entorno real.

Grafana is available at `http://127.0.0.1:3000` and Prometheus at
`http://127.0.0.1:9090`. The local Grafana credentials default to
`admin` / `qbit`; override `GRAFANA_ADMIN_USER` and
`GRAFANA_ADMIN_PASSWORD` outside development. Both services use named Docker
volumes, so their data survives container recreation. The provisioned **Qbit
Queue Overview** dashboard compares incoming and completed throughput,
backlog, failures, latency, group parallelism, reservation stalls and live
worker heartbeats. Qbit registers queues automatically, so one `qbit-metrics`
server discovers and exports every queue in the Redis deployment. The dashboard
queue selector can show one, several or all queues and provides decision signals
for worker utilization, backlog growth, attempt success and retry recovery.

To expose the endpoint without the Docker observability stack:

```bash
make metrics
```

Prometheus can then scrape `http://127.0.0.1:9091/metrics`. The exporter binds
to localhost by default; configure `QBIT_METRICS_ADDR` when it must listen on a
different interface and protect that endpoint at the network boundary. Set
`QBIT_QUEUES=emails,whatsapp` only when the exporter should intentionally limit
its automatic discovery.

See [the Prometheus query guide](docs/observability/prometheus-queries.md) for
queue state, throughput, latency, capacity, multi-queue and alert expressions.
For applications with blocking workers, see the
[Redis connection-pool guide](docs/redis-connection-pools.md) before choosing
worker concurrency or scaling the number of processes.

## Load demo

The demo application uses Qbit as an external application would. Start one
worker process, choosing its internal concurrency:

```bash
QBIT_QUEUE=emails make demo-worker
```

Then start a producer in another terminal:

```bash
QBIT_QUEUE=emails make demo-producer
```

Both commands are interactive. The producer asks for total messages, target
messages per second and independent groups. The worker asks for workers in
that replica, simulated processing time and a transient failure percentage.
Run `make demo-worker` in more terminals to add replicas. See
`apps/qbit-demo/README.md` for repeatable flag examples.

For large local tests, the worker accepts `-completed-retention` and
`-failed-retention`. Production code configures the same policy with
`qbit.WithRetention`; the defaults are 24 hours and 7 days respectively.

Each demo worker process registers a leased heartbeat. Prometheus exposes the
number of live replicas, their total concurrency and one labeled series per
instance. Registrations disappear automatically after a crash or disconnect.
