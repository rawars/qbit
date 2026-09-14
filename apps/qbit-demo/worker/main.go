package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rawars/qbit/apps/qbit-demo/internal/load"
	"github.com/rawars/qbit/apps/qbit-demo/internal/prompt"
	qbit "github.com/rawars/qbit/packages/go"
)

type configuration struct {
	RedisAddress       string
	QueueName          string
	CompletedRetention time.Duration
	FailedRetention    time.Duration
	Workers            load.WorkerConfig
}

func main() {
	config := defaults()
	if len(os.Args) == 1 {
		var err error
		config, err = wizard(config)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	} else {
		parseFlags(&config)
	}
	client, err := qbit.NewClient(qbit.ClientOptions{
		Redis: qbit.RedisOptions{Address: config.RedisAddress},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer func() { _ = client.Close() }()
	queue, err := client.Queue(config.QueueName,
		qbit.WithRetention(config.CompletedRetention, config.FailedRetention),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalContext)
	defer cancel()
	registration, err := queue.RegisterWorker(ctx, config.Workers.Concurrency)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	heartbeatDone := make(chan error, 1)
	go func() {
		heartbeatErr := registration.Maintain(ctx)
		cancel()
		heartbeatDone <- heartbeatErr
	}()

	fmt.Printf("Worker replica ready: queue=%s concurrency=%d processing=%s instance=%s retention_completed=%s retention_failed=%s\n",
		config.QueueName, config.Workers.Concurrency, config.Workers.ProcessingTime,
		registration.Info().Instance, config.CompletedRetention, config.FailedRetention)
	if config.Workers.FailurePercent > 0 {
		fmt.Printf("Transient failure injection enabled: %.2f%% of jobs fail once and retry\n", config.Workers.FailurePercent)
	}
	result, err := load.RunWorkers(ctx, queue, config.Workers)
	cancel()
	heartbeatErr := <-heartbeatDone
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if heartbeatErr != nil {
		fmt.Fprintln(os.Stderr, heartbeatErr)
		os.Exit(1)
	}
	fmt.Printf("Worker stopped: completed=%d failed_attempts=%d recovered=%d\n",
		result.Completed, result.Failed, result.Recovered)
}

func defaults() configuration {
	return configuration{
		RedisAddress:       envOr("QBIT_REDIS_ADDR", "127.0.0.1:6379"),
		QueueName:          envOr("QBIT_QUEUE", "default"),
		CompletedRetention: 24 * time.Hour,
		FailedRetention:    7 * 24 * time.Hour,
		Workers: load.WorkerConfig{
			Concurrency:    10,
			ProcessingTime: 10 * time.Millisecond,
		},
	}
}

func parseFlags(config *configuration) {
	flag.StringVar(&config.RedisAddress, "redis", config.RedisAddress, "Redis address")
	flag.StringVar(&config.QueueName, "queue", config.QueueName, "queue to consume")
	flag.IntVar(&config.Workers.Concurrency, "workers", config.Workers.Concurrency, "parallel workers in this process")
	flag.DurationVar(&config.Workers.ProcessingTime, "processing-time", config.Workers.ProcessingTime, "simulated work per job")
	flag.Float64Var(&config.Workers.FailurePercent, "failure-rate", config.Workers.FailurePercent, "percentage of jobs that fail once and retry (0-100)")
	flag.DurationVar(&config.CompletedRetention, "completed-retention", config.CompletedRetention, "how long completed job records remain in Redis")
	flag.DurationVar(&config.FailedRetention, "failed-retention", config.FailedRetention, "how long terminal failed job records remain in Redis")
	flag.Parse()
}

func wizard(config configuration) (configuration, error) {
	reader := prompt.New(os.Stdin, os.Stdout)
	fmt.Fprintln(os.Stdout, "Qbit demo worker")
	var err error
	if config.QueueName, err = reader.String("Queue name", config.QueueName); err != nil {
		return configuration{}, err
	}
	if config.Workers.Concurrency, err = reader.Int("Workers in this replica", config.Workers.Concurrency); err != nil {
		return configuration{}, err
	}
	processingMilliseconds, err := reader.Float("Processing time per message in milliseconds", float64(config.Workers.ProcessingTime)/float64(time.Millisecond))
	if err != nil {
		return configuration{}, err
	}
	config.Workers.ProcessingTime = time.Duration(processingMilliseconds * float64(time.Millisecond))
	if config.Workers.FailurePercent, err = reader.Float("Transient failure percentage", config.Workers.FailurePercent); err != nil {
		return configuration{}, err
	}
	return config, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
