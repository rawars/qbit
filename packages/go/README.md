# Qbit Go SDK

Qbit is installed as a Go library. It is embedded in the producer and worker
applications; it is not a separate service that each application must call.
Redis is the shared coordinator between every replica.

Queues are registered automatically on their first publish, reservation or
worker registration. A single `qbit-metrics` process uses that catalog to
discover all queues; application code does not need to maintain an exporter
configuration whenever a new queue is added.

## Install

From an existing Go module:

```bash
go get github.com/rawars/qbit/packages/go@latest
```

Import the stable package path:

```go
import qbit "github.com/rawars/qbit/packages/go"
```

For reproducible production builds, pin a released version instead of
`latest`:

```bash
go get github.com/rawars/qbit/packages/go@v0.1.0
```

The repository must be public or the developer's Go environment must have
access to the private Git repository. A maintainer publishes a release by
tagging the root Go module, for example `v0.1.0`.

To use the local checkout from another project before publishing a version:

```bash
go mod edit -replace github.com/rawars/qbit=/Users/rawars/app/qbit
go get github.com/rawars/qbit/packages/go
```

The `replace` directive is only for local development and should not be
committed by applications that consume a released Qbit version.

## Create a client and queue

Create one client per application process and reuse it for all queues. The
client owns the Redis connection pool.

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    qbit "github.com/rawars/qbit/packages/go"
)

type Message struct {
    ID             string `json:"id"`
    ConversationID string `json:"conversation_id"`
    Text           string `json:"text"`
}

func main() {
    ctx := context.Background()
    client, err := qbit.NewClient(qbit.ClientOptions{
        Redis: qbit.RedisOptions{Address: "127.0.0.1:6379"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    messages, err := client.Queue("messages",
        qbit.WithRetention(24*time.Hour, 7*24*time.Hour),
    )
    if err != nil {
        log.Fatal(err)
    }

    message := Message{
        ID:             "msg-123",
        ConversationID: "conversation-42",
        Text:           "hello",
    }
    payload, err := json.Marshal(message)
    if err != nil {
        log.Fatal(err)
    }

    job, err := messages.Publish(ctx, "deliver-message", payload,
        qbit.WithGroup(message.ConversationID),
        qbit.WithJobID(message.ID),
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("published job %s", job.ID)
}
```

`WithGroup` carries domain information from the producer. In a messaging
application it is normally the conversation ID. Qbit processes only one job
from a group at a time, preserving its order, while different conversations
run concurrently. `WithJobID` makes a repeated publish of the same domain
message idempotent.

## Run a worker replica

```go
package main

import (
    "context"
    "encoding/json"
    "errors"
    "log"
    "os/signal"
    "syscall"
    "time"

    qbit "github.com/rawars/qbit/packages/go"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM,
    )
    defer stop()

    client, err := qbit.NewClient(qbit.ClientOptions{
        Redis: qbit.RedisOptions{Address: "127.0.0.1:6379"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    messages, err := client.Queue("messages")
    if err != nil {
        log.Fatal(err)
    }

    worker, err := qbit.NewWorker(messages,
        func(ctx context.Context, job *qbit.Job) error {
            var message Message
            if err := json.Unmarshal(job.Payload, &message); err != nil {
                return qbit.Permanent(err)
            }
            if err := deliver(ctx, message); err != nil {
                if errors.Is(err, ErrInvalidRecipient) {
                    return qbit.Permanent(err)
                }
                return err
            }
            return nil
        },
        qbit.WorkerOptions{
            Concurrency: 50,
            Retry: qbit.RetryPolicy{
                MaxAttempts: 5,
                Backoff: qbit.ExponentialBackoff(time.Second, time.Minute),
            },
            ShutdownTimeout: 30 * time.Second,
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    if err := worker.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

The handler contract is:

- return `nil` to complete the job;
- return a regular error to retry up to `MaxAttempts`;
- return `qbit.Permanent(err)` to fail immediately without retrying;
- observe `ctx.Done()` so Kubernetes can shut the pod down gracefully.

The managed worker registers its replica and concurrency for Prometheus,
reserves jobs, renews locks while handlers run, retries transient errors and
unregisters on clean shutdown. A process crash is recovered after the job lock
and worker heartbeat leases expire.

`Run` stops cleanly when its context is cancelled. Applications with a separate
lifecycle manager can alternatively call `worker.Shutdown(shutdownCtx)`; this
stops new reservations and waits for the running worker to finish its in-flight
handlers.

An administrator can pause reservations for the queue without stopping its
processes:

```go
if err := messages.Pause(ctx); err != nil {
    log.Fatal(err)
}

paused, err := messages.Paused(ctx)

if err := messages.Resume(ctx); err != nil {
    log.Fatal(err)
}
```

Publishing continues while paused and already active jobs finish normally.
Application code can call these methods, but operational use should normally
go through `qbitctl` so the control plane stays outside business services.

## Scaling model

Total worker slots equal application replicas multiplied by concurrency per
replica. Ten replicas with five slots each expose fifty concurrent slots.
Actual group-aware parallelism is bounded by the number of different groups
that currently have work, because Qbit deliberately serializes each group.

API replicas and worker replicas may share one executable initially. At larger
scale, run them as separate Kubernetes deployments so HTTP capacity and queue
capacity can scale independently.

## Redis topologies

Standalone Redis:

```go
Redis: qbit.RedisOptions{Address: "redis:6379"}
```

Redis Cluster:

```go
Redis: qbit.RedisOptions{
    Addresses: []string{"redis-0:6379", "redis-1:6379", "redis-2:6379"},
}
```

Redis Sentinel:

```go
Redis: qbit.RedisOptions{
    Addresses:  []string{"sentinel-0:26379", "sentinel-1:26379"},
    MasterName: "qbit-primary",
}
```

Cluster connectivity is supported by the client. Distributing one hot logical
queue over several Redis primaries still requires Qbit's planned physical
queue partitioning; Redis Cluster alone does not split one queue's hash-tagged
keys.
