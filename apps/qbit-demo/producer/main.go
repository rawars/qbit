package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rawars/qbit/apps/qbit-demo/internal/load"
	"github.com/rawars/qbit/apps/qbit-demo/internal/prompt"
	qbit "github.com/rawars/qbit/packages/go"
)

type configuration struct {
	RedisAddress string
	QueueName    string
	Load         load.ProducerConfig
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
	if err := config.Load.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	client, err := qbit.NewClient(qbit.ClientOptions{
		Redis: qbit.RedisOptions{Address: config.RedisAddress},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer func() { _ = client.Close() }()
	queue, err := client.Queue(config.QueueName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("Publishing %d jobs to %s at %.2f jobs/s across %d groups\n",
		config.Load.Messages, config.QueueName, config.Load.MessagesPerSecond, config.Load.Groups)
	result, err := load.RunProducer(ctx, queue, config.Load)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Published %d jobs in %s (%.2f jobs/s achieved)\n",
		result.Published, result.Elapsed.Round(1e6), result.RatePerSecond)
}

func defaults() configuration {
	return configuration{
		RedisAddress: envOr("QBIT_REDIS_ADDR", "127.0.0.1:6379"),
		QueueName:    envOr("QBIT_QUEUE", "default"),
		Load: load.ProducerConfig{
			Messages:          10_000,
			MessagesPerSecond: 1_000,
			Groups:            100,
			Concurrency:       4,
			PayloadBytes:      128,
		},
	}
}

func parseFlags(config *configuration) {
	flag.StringVar(&config.RedisAddress, "redis", config.RedisAddress, "Redis address")
	flag.StringVar(&config.QueueName, "queue", config.QueueName, "queue to load")
	flag.Int64Var(&config.Load.Messages, "messages", config.Load.Messages, "total jobs to publish")
	flag.Float64Var(&config.Load.MessagesPerSecond, "rate", config.Load.MessagesPerSecond, "target publish rate; zero is unlimited")
	flag.IntVar(&config.Load.Groups, "groups", config.Load.Groups, "independent FIFO groups")
	flag.IntVar(&config.Load.Concurrency, "concurrency", config.Load.Concurrency, "parallel Redis publishers")
	flag.IntVar(&config.Load.PayloadBytes, "payload-bytes", config.Load.PayloadBytes, "payload size per job")
	flag.Parse()
}

func wizard(config configuration) (configuration, error) {
	reader := prompt.New(os.Stdin, os.Stdout)
	fmt.Fprintln(os.Stdout, "Qbit demo producer")
	var err error
	if config.QueueName, err = reader.String("Queue name", config.QueueName); err != nil {
		return configuration{}, err
	}
	if config.Load.Messages, err = reader.Int64("Total messages", config.Load.Messages); err != nil {
		return configuration{}, err
	}
	if config.Load.MessagesPerSecond, err = reader.Float("Messages per second (0 = unlimited)", config.Load.MessagesPerSecond); err != nil {
		return configuration{}, err
	}
	if config.Load.Groups, err = reader.Int("Independent groups", config.Load.Groups); err != nil {
		return configuration{}, err
	}
	return config, config.Load.Validate()
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
