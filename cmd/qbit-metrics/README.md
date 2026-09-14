# qbit-metrics

`qbit-metrics` is Qbit's central Prometheus exporter. It runs one HTTP server
for a Redis deployment and discovers every Qbit queue automatically. It never
reserves or processes jobs.

```text
Qbit producers/workers -> Redis <- qbit-metrics <- Prometheus <- Grafana
```

## Install

After a Qbit version is published:

```bash
go install github.com/rawars/qbit/cmd/qbit-metrics@v0.1.0
```

Run it against Redis:

```bash
qbit-metrics \
  -redis 127.0.0.1:6379 \
  -listen 0.0.0.0:9091
```

## Docker image

Released tags publish a minimal, non-root, multi-architecture image to GitHub
Container Registry:

```bash
docker pull ghcr.io/rawars/qbit-metrics:0.1.0
docker run --rm --name qbit-metrics \
  -p 9091:9091 \
  --add-host host.docker.internal:host-gateway \
  -e QBIT_METRICS_ADDR=0.0.0.0:9091 \
  -e QBIT_REDIS_ADDR=host.docker.internal:6379 \
  ghcr.io/rawars/qbit-metrics:0.1.0
```

Build the same production image locally from the repository root:

```bash
make image-metrics QBIT_METRICS_IMAGE=qbit-metrics:dev
```

The image contains only the statically linked exporter binary, runs as a
non-root user and supports `linux/amd64` and `linux/arm64` release manifests.

Endpoints:

- `GET /metrics` exposes every discovered queue for Prometheus.
- `GET /healthz` checks the Redis connection.

The equivalent environment variables are:

```bash
QBIT_REDIS_ADDR=127.0.0.1:6379
QBIT_METRICS_ADDR=0.0.0.0:9091
```

For authenticated, Sentinel or Cluster deployments:

```bash
QBIT_REDIS_ADDR=redis-0:6379,redis-1:6379,redis-2:6379
QBIT_REDIS_USERNAME=qbit
QBIT_REDIS_PASSWORD=secret
QBIT_REDIS_DB=0
QBIT_REDIS_MASTER_NAME=qbit-primary
```

`QBIT_REDIS_MASTER_NAME` is only set for Sentinel. Multiple addresses without
a master name select Redis Cluster. The corresponding CLI flags are `-redis`,
`-username`, `-password`, `-db` and `-master-name`.

No queue setting is required. Producers and workers add their queue name to a
Redis catalog on their first operation. The exporter reads that catalog during
every scrape, so a new queue appears without restarting the server. On startup,
when the catalog is empty, the exporter also imports queues created by Qbit
versions that predate the catalog.

To expose only selected queues:

```bash
QBIT_QUEUES=whatsapp,emails qbit-metrics
```

The equivalent CLI flag is `-queues whatsapp,emails`. The old `-queue` flag and
`QBIT_QUEUE` variable remain accepted as a single-queue compatibility filter.

## Prometheus

Prometheus needs one scrape target regardless of the number of queues:

```yaml
scrape_configs:
  - job_name: qbit
    static_configs:
      - targets: [qbit-metrics:9091]
```

Every queue metric includes its name:

```text
qbit_jobs_waiting{queue="emails"} 12
qbit_jobs_waiting{queue="whatsapp"} 380
```

`qbit_exporter_discovered_queues` reports how many queues were included in the
current scrape.
