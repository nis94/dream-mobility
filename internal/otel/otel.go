// Package otel provides shared OpenTelemetry SDK initialization for all
// Dream Mobility services. Call Init() at startup and defer Shutdown().
//
// Configuration is via OTEL_ environment variables (standard OTel env):
//   - OTEL_EXPORTER_OTLP_ENDPOINT  (default http://localhost:4318)
//   - OTEL_SERVICE_NAME            (override with serviceName param)
//   - OTEL_TRACES_SAMPLER          (default parentbased_always_on)
//
// Metrics are exported via Prometheus at :9464/metrics (PROM_PORT env).
package otel

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Init configures the global TracerProvider (OTLP/HTTP exporter) and
// MeterProvider (Prometheus exporter) and starts a Prometheus metrics
// server on PROM_PORT (default 9464). Returns a shutdown function that
// flushes pending spans and stops the metrics server.
func Init(ctx context.Context, serviceName string, logger *slog.Logger) (shutdown func(context.Context) error, err error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	// --- Traces: OTLP/HTTP → collector ---
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// --- Metrics: Prometheus scrape endpoint ---
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// Start Prometheus HTTP server.
	promPort := os.Getenv("PROM_PORT")
	if promPort == "" {
		promPort = "9464"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(
		promclient.DefaultGatherer,
		promhttp.HandlerOpts{EnableOpenMetrics: true},
	))
	promSrv := &http.Server{Addr: ":" + promPort, Handler: mux}
	go func() {
		logger.Info("prometheus metrics server starting", "addr", promSrv.Addr)
		if err := promSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("prometheus server error", "err", err)
		}
	}()

	shutdown = func(ctx context.Context) error {
		var firstErr error
		if err := tp.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := mp.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := promSrv.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return shutdown, nil
}
