package qbit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisRetryInitial = 50 * time.Millisecond
	defaultRedisRetryMaximum = 2 * time.Second
	minimumRedisRetryDelay   = 10 * time.Millisecond
)

func defaultRedisRetryBackoff() Backoff {
	exponential := ExponentialBackoff(defaultRedisRetryInitial, defaultRedisRetryMaximum)
	return func(attempt int) time.Duration {
		delay := exponential(attempt)
		if delay <= time.Nanosecond {
			return delay
		}

		// Full synchronization between many worker slots would turn recovery
		// into another Redis spike. Jitter each retry into the upper half of
		// the exponential window while retaining a useful minimum delay.
		minimum := delay / 2
		return minimum + time.Duration(rand.Int64N(int64(delay-minimum)+1))
	}
}

// retryRedisValue repeats only errors that describe a temporary Redis or
// network condition. It stops immediately for protocol, authentication,
// permission, configuration, and data errors. Retries continue until the
// operation succeeds or ctx is cancelled.
func retryRedisValue[T any](
	ctx context.Context,
	backoff Backoff,
	operation func(context.Context) (T, error),
) (T, error) {
	var zero T
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, redisRetryContextError(err, lastErr)
		}

		value, err := operation(ctx)
		if err == nil {
			return value, nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, redisRetryContextError(ctxErr, lastErr)
		}
		if !isTransientRedisError(err) {
			return zero, err
		}

		delay := normalizeRedisRetryDelay(backoff(attempt))
		if err := waitForRetry(ctx, delay); err != nil {
			return zero, redisRetryContextError(err, lastErr)
		}
	}
}

func normalizeRedisRetryDelay(delay time.Duration) time.Duration {
	if delay < minimumRedisRetryDelay {
		return minimumRedisRetryDelay
	}
	return delay
}

func retryRedisOperation(
	ctx context.Context,
	backoff Backoff,
	operation func(context.Context) error,
) error {
	_, err := retryRedisValue(ctx, backoff, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, operation(ctx)
	})
	return err
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func redisRetryContextError(ctxErr, lastErr error) error {
	if lastErr == nil {
		return ctxErr
	}
	return fmt.Errorf("%w (last Redis error: %w)", ctxErr, lastErr)
}

// isTransientRedisError is intentionally conservative: retrying an unknown
// Redis server error forever can hide a broken script or configuration. The
// list mirrors recoverable connection, pool, failover, and loading states.
func isTransientRedisError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, redis.ErrPoolTimeout) ||
		errors.Is(err, redis.ErrPoolExhausted) {
		return true
	}

	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsTemporary {
		return true
	}
	if errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	// Qbit operations add contextual wrapping. Iterate the chain as well as
	// using the go-redis typed checks so older/plain Redis errors retain their
	// recognizable server prefix after wrapping.
	for current := err; current != nil; current = errors.Unwrap(current) {
		if redis.IsLoadingError(current) ||
			redis.IsReadOnlyError(current) ||
			redis.IsClusterDownError(current) ||
			redis.IsTryAgainError(current) ||
			redis.IsMasterDownError(current) ||
			redis.IsMaxClientsError(current) {
			return true
		}
		if _, moved := redis.IsMovedError(current); moved {
			return true
		}
		if _, ask := redis.IsAskError(current); ask {
			return true
		}
		if redis.HasErrorPrefix(current, "BUSY ") || strings.HasPrefix(current.Error(), "BUSY ") || current.Error() == "BUSY" {
			return true
		}
	}
	return false
}
