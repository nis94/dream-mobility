package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nis94/dream-mobility/internal/config"
	"github.com/nis94/dream-mobility/internal/kafkametrics"
	otelinit "github.com/nis94/dream-mobility/internal/otel"
	"github.com/nis94/dream-mobility/internal/processor"
)

const (
	serviceName     = "stream-processor"
	defaultPromPort = "9466"
	promPortEnv     = "PROM_PORT"
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

	store, err := processor.NewStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer store.Close()
	logger.Info("postgres connected, migrations applied")

	metrics := kafkametrics.New()
	proc := processor.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, store, metrics, logger)
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
