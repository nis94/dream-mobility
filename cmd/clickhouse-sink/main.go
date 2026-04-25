// Command clickhouse-sink consumes flight.telemetry from Kafka and batches
// writes into ClickHouse. The consumer loop owns the batch buffer, the
// flush timer, AND the Kafka offset commit — so a batch's offset is only
// committed AFTER the ClickHouse INSERT succeeds, preserving at-least-once
// with zero data loss.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nis94/dream-flight/internal/chsink"
	"github.com/nis94/dream-flight/internal/config"
	"github.com/nis94/dream-flight/internal/kafkametrics"
	otelinit "github.com/nis94/dream-flight/internal/otel"
	"github.com/nis94/dream-flight/internal/processor"
	"github.com/segmentio/kafka-go"
)

// Tuning knobs. Overridable via env (CLICKHOUSE_BATCH_SIZE,
// CLICKHOUSE_BATCH_TIMEOUT, CLICKHOUSE_BUFFER_MAX) so deploys can tune
// without a code change.
var (
	fetchBackoff    = 500 * time.Millisecond
	commitTimeout   = 5 * time.Second
	metricsInterval = 5 * time.Second
	flushOnClose    = 10 * time.Second
	serviceName     = "clickhouse-sink"
	defaultGroupID  = "flight-clickhouse"
)

const (
	defaultPromPort = "9467"
	promPortEnv     = "PROM_PORT"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.ParseLogLevel(os.Getenv("LOG_LEVEL")),
	}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if _, set := os.LookupEnv(promPortEnv); !set {
		_ = os.Setenv(promPortEnv, defaultPromPort)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownOtel, err := otelinit.Init(ctx, serviceName, logger)
	if err != nil {
		return fmt.Errorf("otel init: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			logger.Warn("otel shutdown failed", "err", err)
		}
	}()

	batchSize := envInt("CLICKHOUSE_BATCH_SIZE", chsink.DefaultBatchSize)
	batchTimeout := envDuration("CLICKHOUSE_BATCH_TIMEOUT", chsink.DefaultBatchTimeout)
	bufferMax := envInt("CLICKHOUSE_BUFFER_MAX", batchSize*4)

	sink, err := chsink.New(ctx, chsink.Config{
		Addr:         cfg.ClickHouseAddr,
		Database:     cfg.ClickHouseDB,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	}, logger)
	if err != nil {
		return fmt.Errorf("clickhouse sink: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), flushOnClose)
		defer cancel()
		if err := sink.Close(shutdownCtx); err != nil {
			logger.Error("clickhouse close failed", "err", err)
		}
	}()

	groupID := cfg.KafkaGroupID
	if groupID == "" || groupID == "flight-postgres" {
		// If the user didn't override, fall back to the sink's own default
		// rather than inheriting the Postgres group id by accident.
		groupID = defaultGroupID
	}

	logger.Info("clickhouse-sink starting",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", groupID,
		"clickhouse", cfg.ClickHouseAddr,
		"batch_size", batchSize,
		"batch_timeout", batchTimeout,
		"buffer_max", bufferMax,
	)

	return consumeLoop(ctx, cfg, groupID, sink, logger, consumeOpts{
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		bufferMax:    bufferMax,
	})
}

type consumeOpts struct {
	batchSize    int
	batchTimeout time.Duration
	bufferMax    int
}

// consumeLoop is single-goroutine. A helper goroutine fetches from Kafka
// and pipes messages into msgCh so the main select can race timer vs
// shutdown vs new-message. The main goroutine owns the buffer + sink and
// commits offsets only after sink.Flush() succeeds.
func consumeLoop(
	ctx context.Context,
	cfg config.Config,
	groupID string,
	sink *chsink.Sink,
	logger *slog.Logger,
	opts consumeOpts,
) error {
	reader := processor.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, groupID, logger)
	defer func() { _ = reader.Close() }()

	metrics := kafkametrics.New()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		metrics.Sample(ctx, reader, metricsInterval)
	}()

	msgCh := make(chan kafka.Message, opts.batchSize)
	errCh := make(chan error, 1)
	wg.Add(1)
	go runFetcher(ctx, &wg, reader, msgCh, errCh, logger)
	defer wg.Wait()

	buffered := make([]kafka.Message, 0, opts.batchSize)
	ticker := time.NewTicker(opts.batchTimeout)
	defer ticker.Stop()

	flushAndCommit := func() error {
		if len(buffered) == 0 {
			return nil
		}
		if err := sink.Flush(ctx); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err := reader.CommitMessages(commitCtx, buffered...)
		cancel()
		if err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		buffered = buffered[:0]
		return nil
	}

	for {
		// Backpressure: when the buffer is full, pause fetching by selecting
		// on a nil channel (which blocks forever) instead of msgCh.
		var msgCase <-chan kafka.Message = msgCh
		if len(buffered) >= opts.bufferMax {
			msgCase = nil
		}

		select {
		case <-ctx.Done():
			return drainAndExit(reader, sink, buffered, logger)

		case err := <-errCh:
			logger.Error("fetch message failed", "err", err)
			select {
			case <-ctx.Done():
				return drainAndExit(reader, sink, buffered, logger)
			case <-time.After(fetchBackoff):
			}
			// runFetcher returns after pushing one error, so the previous
			// goroutine is dead. Spawn a new one before we loop — otherwise
			// msgCh is never written to again and the sink silently stalls
			// while appearing healthy.
			wg.Add(1)
			go runFetcher(ctx, &wg, reader, msgCh, errCh, logger)
			continue

		case <-ticker.C:
			if err := flushAndCommit(); err != nil {
				logger.Error("periodic flush failed", "err", err)
			}

		case msg := <-msgCase:
			event, err := processor.DecodeFlightTelemetry(msg.Value)
			if err != nil {
				metrics.IncDecodeFailure()
				logger.Warn("decode failed, skipping",
					"partition", msg.Partition, "offset", msg.Offset,
					"payload_len", len(msg.Value), "err", err)
				// Flush any pending batch BEFORE committing past the poison
				// message, otherwise CommitMessages(msg) would skip the
				// un-committed buffered offsets for the same partition.
				if err := flushAndCommit(); err != nil {
					logger.Error("flush before poison commit failed", "err", err)
					continue
				}
				commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
				if err := reader.CommitMessages(commitCtx, msg); err != nil {
					logger.Error("commit past poison failed", "offset", msg.Offset, "err", err)
				}
				cancel()
				continue
			}

			sink.Add(ctx, event)
			buffered = append(buffered, msg)
			if len(buffered) >= opts.batchSize {
				if err := flushAndCommit(); err != nil {
					logger.Error("size-triggered flush failed", "err", err)
				}
			}
		}
	}
}

// drainAndExit attempts a final flush + commit on shutdown using a detached
// context so the in-flight batch is not lost to signal cancellation.
func drainAndExit(reader *kafka.Reader, sink *chsink.Sink, buffered []kafka.Message, logger *slog.Logger) error {
	if len(buffered) == 0 {
		logger.Info("clickhouse-sink stopped (context cancelled)")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushOnClose)
	defer cancel()
	if err := sink.Flush(ctx); err != nil {
		logger.Error("shutdown flush failed — events will redeliver from Kafka", "err", err)
		return nil
	}
	if err := reader.CommitMessages(ctx, buffered...); err != nil {
		logger.Error("shutdown commit failed — events will redeliver from Kafka", "err", err)
		return nil
	}
	logger.Info("clickhouse-sink stopped cleanly", "final_batch", len(buffered))
	return nil
}

func runFetcher(ctx context.Context, wg *sync.WaitGroup, reader *kafka.Reader, msgCh chan<- kafka.Message, errCh chan<- error, logger *slog.Logger) {
	defer wg.Done()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				logger.Info("fetcher stopped (context cancelled)")
				return
			}
			// Non-blocking send: if the consumer is busy flushing we prefer
			// to drop-into-retry rather than block the fetcher goroutine.
			select {
			case errCh <- err:
			case <-ctx.Done():
				return
			}
			return
		}
		select {
		case msgCh <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
