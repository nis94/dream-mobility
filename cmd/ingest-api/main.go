package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nis94/dream-mobility/internal/api"
	"github.com/nis94/dream-mobility/internal/config"
	"github.com/nis94/dream-mobility/internal/producer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	// Graceful shutdown on SIGINT/SIGTERM. Created early so producer startup
	// (SR schema registration with retries) is bounded by the same signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create Kafka producer (registers schema with SR on startup).
	prod, err := producer.New(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.SchemaRegistryURL, logger)
	if err != nil {
		logger.Error("failed to create producer", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := prod.Close(); err != nil {
			logger.Error("producer close failed", "err", err)
		}
	}()

	h := api.NewHandler(prod, logger)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	r.Post("/events", h.Ingest)

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// The server goroutine reports only unexpected errors via srvErr.
	// http.ErrServerClosed is the normal return from Shutdown and is filtered.
	srvErr := make(chan error, 1)
	go func() {
		logger.Info("ingest-api starting", "addr", srv.Addr, "kafka", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
		close(srvErr)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down", "signal", ctx.Err())
	case err := <-srvErr:
		logger.Error("http server error", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "err", err)
	}
}
