package main

import (
	"context"
	"errors"
	"fmt"
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
	otelinit "github.com/nis94/dream-mobility/internal/otel"
	"github.com/nis94/dream-mobility/internal/producer"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	serviceName       = "ingest-api"
	defaultPromPort   = "9464"
	promPortEnv       = "PROM_PORT"
	shutdownTimeout   = 10 * time.Second
	otelFlushTimeout  = 5 * time.Second
	middlewareTimeout = 25 * time.Second
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), otelFlushTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			logger.Warn("otel shutdown failed", "err", err)
		}
	}()

	prod, err := producer.New(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.SchemaRegistryURL, logger)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
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
	r.Use(middleware.Timeout(middlewareTimeout))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	// otelhttp starts an HTTP server span per request and extracts W3C
	// TraceContext from incoming headers — useful if an upstream client
	// passes traceparent (curl, k6, another service). Wrapping only the
	// POST route keeps /health free of trace overhead.
	r.Method(http.MethodPost, "/events", otelhttp.NewHandler(http.HandlerFunc(h.Ingest), "POST /events"))

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

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
		return fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
