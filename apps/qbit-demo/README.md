# Qbit demo

This application exercises Qbit through its public Go API. The producer and
worker are separate processes so load, worker concurrency and worker replicas
can be changed independently while Prometheus and Grafana observe the queue.

Start one worker replica interactively:

```bash
QBIT_QUEUE=emails go run ./apps/qbit-demo/worker
```

In another terminal, start the interactive producer:

```bash
QBIT_QUEUE=emails go run ./apps/qbit-demo/producer
```

The producer asks for total messages, target messages per second and number of
independent groups. The worker asks for concurrency and simulated processing
time. The metrics exporter discovers the demo queue automatically after its
first publish or worker registration.

Flags make repeatable tests possible:

```bash
go run ./apps/qbit-demo/worker \
  -queue emails \
  -workers 10 \
  -processing-time 25ms \
  -failure-rate 10 \
  -completed-retention 15m \
  -failed-retention 1h

go run ./apps/qbit-demo/producer \
  -queue emails -messages 1000000 -rate 5000 -groups 500
```

One worker command is one application replica. Run it in more terminals to
simulate multiple replicas; every replica competes safely for work in the same
Redis queue. Stop workers with Ctrl+C.

`-failure-rate 10` deterministically makes approximately 10 percent of jobs
fail on their first attempt. Qbit returns each failed job to the head of its
group, and its second attempt succeeds. Grafana then shows the failed attempt,
the retry, and the later recovery as separate rates. Leave the value at zero
for normal processing.

The library defaults to retaining completed job records for 24 hours and
terminal failed records for 7 days. The demo flags can shorten that window for
large load tests, preventing old result hashes from occupying Redis after the
test. Retention is assigned when a job reaches its terminal state, so changing
the flags requires restarting the demo worker and affects subsequent results.
