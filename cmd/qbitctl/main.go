package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/rawars/qbit/internal/buildinfo"
	qbit "github.com/rawars/qbit/packages/go"
)

type configuration struct {
	redisAddresses []string
	username       string
	password       string
	db             int
	masterName     string
	timeout        time.Duration
	json           bool
}

type queueSummary struct {
	Name        string `json:"name"`
	Paused      bool   `json:"paused"`
	Waiting     int64  `json:"waiting"`
	Active      int64  `json:"active"`
	ReadyGroups int64  `json:"ready_groups"`
	Workers     int    `json:"worker_replicas"`
	Slots       int    `json:"worker_slots"`
}

type workerSummary struct {
	Queue           string    `json:"queue"`
	ID              string    `json:"id"`
	Instance        string    `json:"instance"`
	Concurrency     int       `json:"concurrency"`
	StartedAt       time.Time `json:"started_at"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "qbitctl:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, arguments []string, output, errorOutput io.Writer) error {
	flags := flag.NewFlagSet("qbitctl", flag.ContinueOnError)
	flags.SetOutput(errorOutput)
	redisAddresses := flags.String("redis", envOr("QBIT_REDIS_ADDR", "127.0.0.1:6379"), "comma-separated Redis addresses")
	username := flags.String("username", envOr("QBIT_REDIS_USERNAME", ""), "Redis username")
	password := flags.String("password", envOr("QBIT_REDIS_PASSWORD", ""), "Redis password")
	db := flags.Int("db", envInt("QBIT_REDIS_DB", 0), "Redis database")
	masterName := flags.String("master-name", envOr("QBIT_REDIS_MASTER_NAME", ""), "Redis Sentinel master name")
	timeout := flags.Duration("timeout", 10*time.Second, "command timeout")
	jsonOutput := flags.Bool("json", false, "write JSON output")
	showVersion := flags.Bool("version", false, "print version and exit")
	flags.Usage = func() { writeUsage(errorOutput) }
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *showVersion {
		_, err := fmt.Fprintln(output, "qbitctl", buildinfo.String())
		return err
	}
	remaining := flags.Args()
	if len(remaining) == 0 || remaining[0] == "help" {
		writeUsage(output)
		if len(remaining) == 0 {
			return errors.New("a command is required")
		}
		return nil
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	config := configuration{
		redisAddresses: splitNonEmpty(*redisAddresses),
		username:       *username,
		password:       *password,
		db:             *db,
		masterName:     *masterName,
		timeout:        *timeout,
		json:           *jsonOutput,
	}
	if len(config.redisAddresses) == 0 {
		return errors.New("at least one Redis address is required")
	}
	client, err := qbit.NewClient(qbit.ClientOptions{Redis: qbit.RedisOptions{
		Addresses:  config.redisAddresses,
		Username:   config.username,
		Password:   config.password,
		DB:         config.db,
		MasterName: config.masterName,
	}})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(parent, config.timeout)
	defer cancel()
	command := remaining[0]
	commandArguments := remaining[1:]
	switch command {
	case "health":
		if len(commandArguments) != 0 {
			return errors.New("health does not accept arguments")
		}
		if err := client.Ping(ctx); err != nil {
			return err
		}
		if config.json {
			return writeJSON(output, map[string]string{"status": "ok"})
		}
		_, err := fmt.Fprintln(output, "Redis: ok")
		return err
	case "queues":
		if len(commandArguments) != 0 {
			return errors.New("queues does not accept arguments")
		}
		stats, err := allStats(ctx, client, nil)
		if err != nil {
			return err
		}
		return writeQueues(output, stats, config.json)
	case "stats":
		stats, err := allStats(ctx, client, commandArguments)
		if err != nil {
			return err
		}
		return writeStats(output, stats, config.json)
	case "workers":
		stats, err := allStats(ctx, client, commandArguments)
		if err != nil {
			return err
		}
		return writeWorkers(output, stats, config.json)
	case "events":
		return runEvents(ctx, client, commandArguments, output, errorOutput, config.json)
	case "pause":
		return changePauseState(ctx, client, commandArguments, output, config.json, true)
	case "resume":
		return changePauseState(ctx, client, commandArguments, output, config.json, false)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func allStats(ctx context.Context, client *qbit.Client, requested []string) ([]qbit.Stats, error) {
	if len(requested) > 0 {
		return client.QueueStats(ctx, time.Minute, requested...)
	}
	names, err := client.DiscoverQueues(ctx)
	if err != nil {
		return nil, err
	}
	return client.QueueStats(ctx, time.Minute, names...)
}

func writeQueues(output io.Writer, stats []qbit.Stats, jsonOutput bool) error {
	queues := make([]queueSummary, 0, len(stats))
	for _, queueStats := range stats {
		queues = append(queues, queueSummary{
			Name:        queueStats.Queue,
			Paused:      queueStats.Paused,
			Waiting:     queueStats.Waiting,
			Active:      queueStats.Active,
			ReadyGroups: queueStats.ReadyGroups,
			Workers:     queueStats.WorkerReplicas,
			Slots:       queueStats.WorkerConcurrency,
		})
	}
	if jsonOutput {
		return writeJSON(output, queues)
	}
	table := newTable(output)
	_, _ = fmt.Fprintln(table, "QUEUE\tSTATE\tWAITING\tACTIVE\tREADY GROUPS\tWORKERS\tSLOTS")
	for _, queue := range queues {
		state := "running"
		if queue.Paused {
			state = "paused"
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			queue.Name, state, queue.Waiting, queue.Active, queue.ReadyGroups, queue.Workers, queue.Slots)
	}
	return table.Flush()
}

func writeStats(output io.Writer, stats []qbit.Stats, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(output, stats)
	}
	table := newTable(output)
	_, _ = fmt.Fprintln(table, "QUEUE\tSTATE\tWAITING\tACTIVE\tPUBLISHED/s\tCOMPLETED/s\tFAILED/s\tWAIT\tPROCESSING")
	for _, queueStats := range stats {
		state := "running"
		if queueStats.Paused {
			state = "paused"
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\t%s\n",
			queueStats.Queue,
			state,
			queueStats.Waiting,
			queueStats.Active,
			queueStats.Rates.Published,
			queueStats.Rates.Completed,
			queueStats.Rates.Failed,
			time.Duration(queueStats.AverageQueueWaitMillis*float64(time.Millisecond)).Round(time.Millisecond),
			time.Duration(queueStats.AverageProcessingMillis*float64(time.Millisecond)).Round(time.Millisecond),
		)
	}
	return table.Flush()
}

func writeWorkers(output io.Writer, stats []qbit.Stats, jsonOutput bool) error {
	workers := make([]workerSummary, 0)
	for _, queueStats := range stats {
		for _, worker := range queueStats.Workers {
			workers = append(workers, workerSummary{
				Queue:           queueStats.Queue,
				ID:              worker.ID,
				Instance:        worker.Instance,
				Concurrency:     worker.Concurrency,
				StartedAt:       worker.StartedAt,
				LastHeartbeatAt: worker.LastHeartbeatAt,
				ExpiresAt:       worker.ExpiresAt,
			})
		}
	}
	if jsonOutput {
		return writeJSON(output, workers)
	}
	table := newTable(output)
	_, _ = fmt.Fprintln(table, "QUEUE\tINSTANCE\tWORKER ID\tCONCURRENCY\tLAST HEARTBEAT\tEXPIRES")
	for _, worker := range workers {
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%s\n",
			worker.Queue,
			worker.Instance,
			worker.ID,
			worker.Concurrency,
			worker.LastHeartbeatAt.Format(time.RFC3339),
			worker.ExpiresAt.Format(time.RFC3339),
		)
	}
	return table.Flush()
}

func runEvents(
	ctx context.Context,
	client *qbit.Client,
	arguments []string,
	output io.Writer,
	errorOutput io.Writer,
	jsonOutput bool,
) error {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	flags.SetOutput(errorOutput)
	limit := flags.Int64("limit", 20, "maximum recent events")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("usage: qbitctl events [-limit N] QUEUE")
	}
	queue, err := client.Queue(flags.Args()[0])
	if err != nil {
		return err
	}
	events, err := queue.RecentEvents(ctx, *limit)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(output, events)
	}
	table := newTable(output)
	_, _ = fmt.Fprintln(table, "TIME\tTYPE\tJOB ID\tGROUP\tWAIT\tPROCESSING")
	for _, event := range events {
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			event.At.Format(time.RFC3339Nano),
			event.Type,
			event.JobID,
			event.Group,
			time.Duration(event.QueueWaitMillis)*time.Millisecond,
			time.Duration(event.ProcessingMillis)*time.Millisecond,
		)
	}
	return table.Flush()
}

func changePauseState(
	ctx context.Context,
	client *qbit.Client,
	arguments []string,
	output io.Writer,
	jsonOutput bool,
	pause bool,
) error {
	if len(arguments) != 1 {
		return errors.New("pause and resume require exactly one queue name")
	}
	queue, err := client.Queue(arguments[0])
	if err != nil {
		return err
	}
	state := "running"
	if pause {
		err = queue.Pause(ctx)
		state = "paused"
	} else {
		err = queue.Resume(ctx)
	}
	if err != nil {
		return err
	}
	result := map[string]string{"queue": queue.Name(), "state": state}
	if jsonOutput {
		return writeJSON(output, result)
	}
	_, err = fmt.Fprintf(output, "Queue %s is %s.\n", queue.Name(), state)
	return err
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func newTable(output io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
}

func splitNonEmpty(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
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

func writeUsage(output io.Writer) {
	_, _ = fmt.Fprint(output, `Usage: qbitctl [global options] COMMAND [arguments]

Commands:
  health                 Check Redis connectivity
  queues                 List discovered queues and current capacity
  stats [QUEUE ...]      Show queue throughput and latency
  workers [QUEUE ...]    List live worker replicas
  events [-limit N] QUEUE
                         Show recent queue lifecycle events
  pause QUEUE            Stop new reservations across all replicas
  resume QUEUE           Resume reservations

Global options must appear before the command. Common examples:
  qbitctl --redis 127.0.0.1:6379 queues
  qbitctl stats whatsapp
  qbitctl --json workers
  qbitctl pause whatsapp
`)
}
