# qbitctl

`qbitctl` is the operational CLI for a Qbit Redis deployment. It discovers
queues, reads their current state, lists live worker replicas and can pause or
resume consumption without restarting applications.

## Install

Pin the same Qbit release used by the applications:

```bash
go install github.com/rawars/qbit/cmd/qbitctl@v0.1.0
qbitctl --version
```

For local development from this repository:

```bash
make qbitctl ARGS="queues"
```

The default Redis address is `127.0.0.1:6379`. Configure it with a flag or an
environment variable:

```bash
qbitctl --redis redis.internal:6379 health
QBIT_REDIS_ADDR=redis.internal:6379 qbitctl queues
```

Authentication and topology options are available through `--username`,
`--password`, `--db` and `--master-name`. Global flags must be placed before
the command.

## Commands

```bash
qbitctl health
qbitctl queues
qbitctl stats
qbitctl stats whatsapp emails
qbitctl workers
qbitctl workers whatsapp
qbitctl events -limit 50 whatsapp
qbitctl pause whatsapp
qbitctl resume whatsapp
```

`queues`, `stats` and `workers` discover all registered queues when no queue
name is supplied. Add `--json` before the command for scripts and automation:

```bash
qbitctl --json stats whatsapp
qbitctl --json workers
```

`pause` is distributed state stored in Redis. Every current and future worker
replica stops reserving new jobs from that queue, while jobs already active are
allowed to finish and keep renewing their locks. Publishing remains enabled,
so backlog may grow while paused. `resume` wakes workers and consumption
continues from the existing grouped FIFO queues. Both operations are
idempotent.
