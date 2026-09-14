package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rawars/qbit/internal/buildinfo"
	qbit "github.com/rawars/qbit/packages/go"
	qbitprometheus "github.com/rawars/qbit/packages/go/prometheus"
)

func main() {
	listenAddress := flag.String("listen", envOr("QBIT_METRICS_ADDR", "127.0.0.1:9091"), "metrics listen address")
	redisAddresses := flag.String("redis", envOr("QBIT_REDIS_ADDR", "127.0.0.1:6379"), "comma-separated Redis addresses")
	redisUsername := flag.String("username", envOr("QBIT_REDIS_USERNAME", ""), "Redis username")
	redisPassword := flag.String("password", envOr("QBIT_REDIS_PASSWORD", ""), "Redis password")
	redisDB := flag.Int("db", envInt("QBIT_REDIS_DB", 0), "Redis database")
	redisMasterName := flag.String("master-name", envOr("QBIT_REDIS_MASTER_NAME", ""), "Redis Sentinel master name")
	queueNames := flag.String("queues", envOr("QBIT_QUEUES", ""), "optional comma-separated queue filter; empty discovers all queues")
	legacyQueueName := flag.String("queue", envOr("QBIT_QUEUE", ""), "deprecated single-queue filter")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("qbit-metrics", buildinfo.String())
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	client, err := qbit.NewClient(qbit.ClientOptions{
		Redis: qbit.RedisOptions{
			Addresses:  splitCSV(*redisAddresses),
			Username:   *redisUsername,
			Password:   *redisPassword,
			DB:         *redisDB,
			MasterName: *redisMasterName,
		},
	})
	if err != nil {
		logger.Error("configure Qbit client", "error", err)
		os.Exit(2)
	}
	defer func() { _ = client.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	discovered, err := client.DiscoverQueues(ctx)
	if err != nil {
		logger.Error("discover Qbit queues", "error", err)
		os.Exit(1)
	}
	filter := parseQueueFilter(*queueNames, *legacyQueueName)
	for _, name := range filter {
		if _, err := client.Queue(name); err != nil {
			logger.Error("configure queue filter", "queue", name, "error", err)
			os.Exit(2)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", qbitprometheus.MultiQueueHandler(client, filter...))
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, request *http.Request) {
		if pingErr := client.Ping(request.Context()); pingErr != nil {
			http.Error(writer, "Redis is unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()

	logger.Info("Qbit metrics ready",
		"listen", *listenAddress,
		"discovered_queues", len(discovered),
		"queue_filter", strings.Join(filter, ","),
	)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serve metrics", "error", err)
		os.Exit(1)
	}
}

func parseQueueFilter(values ...string) []string {
	unique := make(map[string]struct{})
	for _, value := range values {
		for _, name := range strings.Split(value, ",") {
			name = strings.TrimSpace(name)
			if name != "" {
				unique[name] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(unique))
	for name := range unique {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
