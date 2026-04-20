package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nis94/dream-mobility/internal/chsink"
	"github.com/nis94/dream-mobility/internal/config"
	"github.com/nis94/dream-mobility/internal/processor"
)

const fetchBackoff = 500 * time.Millisecond

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to ClickHouse and run migrations.
	sink, err := chsink.New(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDB, logger)
	if err != nil {
		return fmt.Errorf("clickhouse sink: %w", err)
	}
	defer func() {
		if err := sink.Close(context.Background()); err != nil {
			logger.Error("clickhouse close failed", "err", err)
		}
	}()

	// Start the periodic flusher in background.
	go sink.RunFlusher(ctx)

	// Reuse the processor's decode logic — we consume the same topic with
	// a different consumer group so each sink gets its own copy of the stream.
	groupID := cfg.KafkaGroupID + "-clickhouse"

	logger.Info("clickhouse-sink starting",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", groupID,
		"clickhouse", cfg.ClickHouseAddr,
	)

	return consumeLoop(ctx, cfg, groupID, sink, logger)
}

// consumeLoop reads from Kafka, decodes events, and adds them to the
// ClickHouse sink. It mirrors the processor's consume pattern but without
// the Postgres store.
func consumeLoop(ctx context.Context, cfg config.Config, groupID string, sink *chsink.Sink, logger *slog.Logger) error {
	// Create a lightweight consumer using the processor package's Kafka reader
	// config pattern. We only need the decode function from processor.
	reader := processor.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, groupID, logger)
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("clickhouse-sink stopped (context cancelled)")
				return nil
			}
			logger.Error("fetch message failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(fetchBackoff):
			}
			continue
		}

		event, err := processor.DecodeMovementEvent(msg.Value)
		if err != nil {
			logger.Warn("decode failed, skipping", "offset", msg.Offset, "err", err)
			commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := reader.CommitMessages(commitCtx, msg); err != nil {
				logger.Error("commit past corrupted message failed", "offset", msg.Offset, "err", err)
			}
			cancel()
			continue
		}

		if err := sink.Add(ctx, event); err != nil {
			logger.Error("clickhouse add failed", "event_id", event.EventID, "err", err)
			continue
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := reader.CommitMessages(commitCtx, msg); err != nil {
			logger.Error("commit offset failed", "offset", msg.Offset, "err", err)
		}
		cancel()
	}
}
