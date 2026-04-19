package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nis94/dream-mobility/internal/config"
	"github.com/nis94/dream-mobility/internal/processor"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to Postgres and run migrations.
	store, err := processor.NewStore(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("postgres connected, migrations applied")

	// Start the Kafka consumer loop.
	proc := processor.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, store, logger)
	defer func() {
		if err := proc.Close(); err != nil {
			logger.Error("processor close failed", "err", err)
		}
	}()

	logger.Info("stream-processor starting",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", cfg.KafkaGroupID,
		"postgres", cfg.PostgresDSN,
	)

	if err := proc.Run(ctx); err != nil {
		logger.Error("processor exited with error", "err", err)
		os.Exit(1)
	}
}
