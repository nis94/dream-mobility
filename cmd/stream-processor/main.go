package main

import (
	"context"
	"fmt"
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
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run wires config, signal-bound context, store, and processor, and blocks on
// proc.Run. Returning an error here (rather than calling os.Exit from deep in
// main) guarantees that both store.Close and proc.Close defers fire on every
// exit path.
func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := processor.NewStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer store.Close()
	logger.Info("postgres connected, migrations applied")

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
		"postgres", config.RedactDSN(cfg.PostgresDSN),
	)

	if err := proc.Run(ctx); err != nil {
		return fmt.Errorf("processor run: %w", err)
	}
	return nil
}
