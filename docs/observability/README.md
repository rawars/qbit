# Qbit observability stack

This development stack starts one multi-queue Qbit exporter, Prometheus and
Grafana with persistent local volumes.

From the repository root:

```bash
make observability
```

Or directly:

```bash
docker compose -f docs/observability/docker-compose.yml up -d --build
```

The Compose stack builds `cmd/qbit-metrics/Dockerfile` and tags it locally as
`qbit-metrics:dev`. To test a released image without modifying the file:

```bash
QBIT_METRICS_IMAGE=ghcr.io/rawars/qbit-metrics:0.1.0 \
docker compose -f docs/observability/docker-compose.yml up -d --no-build
```

Open:

- Grafana: `http://127.0.0.1:3000` (`admin` / `qbit` by default).
- Prometheus: `http://127.0.0.1:9090`.

The exporter connects to `127.0.0.1:6379` on the host by default and discovers
all Qbit queues stored there. No queue name is required. The Grafana **Queue**
selector updates from the `queue` label and can display one, several or all
queues.

Override Redis when necessary:

```bash
QBIT_REDIS_ADDR=redis.example.internal:6379 make observability
```

Redis credentials can be supplied as `QBIT_REDIS_USERNAME` and
`QBIT_REDIS_PASSWORD`. `QBIT_REDIS_DB` selects the database. For Sentinel,
provide all Sentinel addresses in `QBIT_REDIS_ADDR` and set
`QBIT_REDIS_MASTER_NAME`.

Restrict the exporter only when intentionally needed:

```bash
QBIT_QUEUES=emails,whatsapp make observability
```

Verify discovery in Prometheus:

```promql
qbit_exporter_discovered_queues
```

List the backlog for every queue:

```promql
qbit_jobs_waiting
```

See [the Prometheus query guide](prometheus-queries.md) for throughput,
latency, workers, retries, scaling signals and alert expressions.
