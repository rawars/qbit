package qbit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const queueRegistryKey = "qbit:registry:{queues}:names"

// RedisOptions describes the Redis deployment used by Qbit.
//
// Address is convenient for a standalone Redis instance. Addresses can be
// used for Redis Cluster or Sentinel. When Addresses is set, Address is
// ignored.
type RedisOptions struct {
	Address      string
	Addresses    []string
	Username     string
	Password     string
	DB           int
	MasterName   string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	TLSConfig    *tls.Config
}

// ClientOptions configures a Qbit client.
type ClientOptions struct {
	Redis RedisOptions
}

// Client owns the Redis connection pool used by its queues.
type Client struct {
	redis     redis.UniversalClient
	closeOnce sync.Once
	closeErr  error
}

// RedisPoolStats is a point-in-time view of the connection pool owned by a
// Client. WaitCount and WaitDuration are cumulative and make pool starvation
// observable without exposing the underlying Redis client.
type RedisPoolStats struct {
	Hits             uint32
	Misses           uint32
	Timeouts         uint32
	WaitCount        uint32
	WaitDuration     time.Duration
	TotalConnections uint32
	IdleConnections  uint32
	StaleConnections uint32
}

// NewClient creates a Qbit client. It does not perform network I/O; Redis is
// contacted by the first queue operation.
func NewClient(options ClientOptions) (*Client, error) {
	addresses := append([]string(nil), options.Redis.Addresses...)
	if len(addresses) == 0 && options.Redis.Address != "" {
		addresses = []string{options.Redis.Address}
	}
	if len(addresses) == 0 {
		return nil, errors.New("qbit: at least one Redis address is required")
	}
	if options.Redis.DB < 0 {
		return nil, errors.New("qbit: Redis DB cannot be negative")
	}
	if options.Redis.PoolSize < 0 {
		return nil, errors.New("qbit: Redis pool size cannot be negative")
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        addresses,
		MasterName:   options.Redis.MasterName,
		Username:     options.Redis.Username,
		Password:     options.Redis.Password,
		DB:           options.Redis.DB,
		DialTimeout:  options.Redis.DialTimeout,
		ReadTimeout:  options.Redis.ReadTimeout,
		WriteTimeout: options.Redis.WriteTimeout,
		PoolSize:     options.Redis.PoolSize,
		TLSConfig:    options.Redis.TLSConfig,
	})

	return &Client{redis: client}, nil
}

// Queue returns a queue handle backed by this client's Redis connection.
func (c *Client) Queue(name string, options ...QueueOption) (*Queue, error) {
	if c == nil || c.redis == nil {
		return nil, errors.New("qbit: client is nil")
	}
	return NewQueue(name, c.redis, options...)
}

// Ping verifies that the configured Redis deployment is reachable.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.redis == nil {
		return errors.New("qbit: client is nil")
	}
	if err := c.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("qbit: ping Redis: %w", err)
	}
	return nil
}

// PoolStats returns connection-pool counters for this client. A nil client
// returns zero values. The configured PoolSize is intentionally not included
// because it belongs to application configuration; these counters describe
// actual pool use.
func (c *Client) PoolStats() RedisPoolStats {
	if c == nil || c.redis == nil {
		return RedisPoolStats{}
	}
	stats := c.redis.PoolStats()
	if stats == nil {
		return RedisPoolStats{}
	}
	return RedisPoolStats{
		Hits:             stats.Hits,
		Misses:           stats.Misses,
		Timeouts:         stats.Timeouts,
		WaitCount:        stats.WaitCount,
		WaitDuration:     time.Duration(stats.WaitDurationNs),
		TotalConnections: stats.TotalConns,
		IdleConnections:  stats.IdleConns,
		StaleConnections: stats.StaleConns,
	}
}

// QueueNames returns every queue registered by Qbit, sorted by name. Producers
// and workers register their queue on the first operation performed by each
// queue handle.
func (c *Client) QueueNames(ctx context.Context) ([]string, error) {
	if c == nil || c.redis == nil {
		return nil, errors.New("qbit: client is nil")
	}
	names, err := c.redis.SMembers(ctx, queueRegistryKey).Result()
	if err != nil {
		return nil, fmt.Errorf("qbit: list queues: %w", err)
	}
	for _, name := range names {
		if _, err := newQueueKeys(name); err != nil {
			return nil, fmt.Errorf("qbit: invalid queue registry member %q: %w", name, err)
		}
	}
	sort.Strings(names)
	return names, nil
}

// DiscoverQueues imports queues created by older Qbit versions into the queue
// registry and returns all known names. The compatibility scan runs only when
// the registry is empty; normal metrics scrapes use QueueNames.
func (c *Client) DiscoverQueues(ctx context.Context) ([]string, error) {
	registered, err := c.QueueNames(ctx)
	if err != nil {
		return nil, err
	}
	if len(registered) > 0 {
		return registered, nil
	}
	discovered, err := c.legacyQueueNames(ctx)
	if err != nil {
		return nil, err
	}
	if len(discovered) > 0 {
		members := make([]any, len(discovered))
		for index, name := range discovered {
			members[index] = name
		}
		if err := c.redis.SAdd(ctx, queueRegistryKey, members...).Err(); err != nil {
			return nil, fmt.Errorf("qbit: persist discovered queues: %w", err)
		}
	}
	return discovered, nil
}

func (c *Client) legacyQueueNames(ctx context.Context) ([]string, error) {
	names := make(map[string]struct{})
	var namesMu sync.Mutex
	scan := func(ctx context.Context, scanner interface {
		Scan(context.Context, uint64, string, int64) *redis.ScanCmd
	}) error {
		var cursor uint64
		for {
			keys, next, scanErr := scanner.Scan(ctx, cursor, "qbit:{*}:metrics", 1000).Result()
			if scanErr != nil {
				return scanErr
			}
			namesMu.Lock()
			for _, key := range keys {
				if name, ok := queueNameFromMetricsKey(key); ok {
					names[name] = struct{}{}
				}
			}
			namesMu.Unlock()
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}

	var err error
	if cluster, ok := c.redis.(*redis.ClusterClient); ok {
		err = cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			return scan(ctx, node)
		})
	} else {
		err = scan(ctx, c.redis)
	}
	if err != nil {
		return nil, fmt.Errorf("qbit: discover legacy queues: %w", err)
	}
	return sortedQueueNames(names), nil
}

// QueueStats reads operational statistics for several queues. With no names,
// it discovers all registered queues. Reads are bounded to eight concurrent
// queues so a large catalog cannot create unbounded Redis pressure.
func (c *Client) QueueStats(ctx context.Context, window time.Duration, queueNames ...string) ([]Stats, error) {
	if c == nil || c.redis == nil {
		return nil, errors.New("qbit: client is nil")
	}
	names, err := c.normalizeQueueNames(ctx, queueNames)
	if err != nil {
		return nil, err
	}
	stats := make([]Stats, len(names))
	if len(names) == 0 {
		return stats, nil
	}

	workerCount := min(8, len(names))
	indexes := make(chan int)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(workerCount)
	var firstErr error
	var firstErrOnce sync.Once
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range indexes {
				queue, queueErr := c.Queue(names[index])
				if queueErr == nil {
					stats[index], queueErr = queue.Stats(readCtx, window)
				}
				if queueErr != nil {
					firstErrOnce.Do(func() {
						firstErr = fmt.Errorf("qbit: read queue %q stats: %w", names[index], queueErr)
						cancel()
					})
					return
				}
			}
		}()
	}
sendIndexes:
	for index := range names {
		select {
		case indexes <- index:
		case <-readCtx.Done():
			break sendIndexes
		}
	}
	close(indexes)
	workers.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *Client) normalizeQueueNames(ctx context.Context, requested []string) ([]string, error) {
	if len(requested) == 0 {
		return c.QueueNames(ctx)
	}
	names := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		if _, err := newQueueKeys(name); err != nil {
			return nil, err
		}
		names[name] = struct{}{}
	}
	return sortedQueueNames(names), nil
}

func queueNameFromMetricsKey(key string) (string, bool) {
	const prefix = "qbit:{"
	const suffix = "}:metrics"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	if _, err := newQueueKeys(name); err != nil {
		return "", false
	}
	return name, true
}

func sortedQueueNames(names map[string]struct{}) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// Close releases the Redis connection pool. It is safe to call more than once.
func (c *Client) Close() error {
	if c == nil || c.redis == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.redis.Close()
	})
	return c.closeErr
}
