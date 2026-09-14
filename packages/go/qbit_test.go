package qbit_test

import (
	"context"
	"testing"

	qbit "github.com/rawars/qbit/packages/go"
	qbitprometheus "github.com/rawars/qbit/packages/go/prometheus"
)

func TestPublicPackageCompiles(t *testing.T) {
	client, err := qbit.NewClient(qbit.ClientOptions{
		Redis: qbit.RedisOptions{Address: "localhost:6379"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	queue, err := client.Queue("emails")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := qbit.NewWorker(queue, func(context.Context, *qbit.Job) error {
		return nil
	}, qbit.WorkerOptions{Concurrency: 5})
	if err != nil {
		t.Fatal(err)
	}
	if worker == nil {
		t.Fatal("NewWorker returned nil")
	}
	if handler := qbitprometheus.MultiQueueHandler(client); handler == nil {
		t.Fatal("MultiQueueHandler returned nil")
	}
}
