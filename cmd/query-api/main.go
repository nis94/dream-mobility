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
	"github.com/nis94/dream-mobility/internal/config"
	otelinit "github.com/nis94/dream-mobility/internal/otel"
	"github.com/nis94/dream-mobility/internal/query"
)

const (
	serviceName     = "query-api"
	defaultPromPort = "9465"
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

	store, err := query.NewStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer store.Close()
	logger.Info("query-api postgres connected", "postgres", config.RedactDSN(cfg.PostgresDSN))

	h := query.NewHandler(store, logger)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	// Set the per-request timeout slightly below the server's WriteTimeout
	// so chi produces a clean 503 rather than racing with the server closing
	// the connection.
	r.Use(middleware.Timeout(25 * time.Second))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})

	r.Route("/entities/{type}/{id}", func(r chi.Router) {
		r.Get("/position", h.GetPosition)
		r.Get("/events", h.ListEvents)
	})

	srv := &http.Server{
		Addr:         ":" + cfg.QueryHTTPPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("query-api starting", "addr", srv.Addr)
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
