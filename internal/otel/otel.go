// Package otel provides shared OpenTelemetry SDK initialization for all
// Dream Mobility services. Call Init at startup and defer the returned
// shutdown function on a short-lived context.
//
// Configuration is via OTel-standard env vars:
//
//   - OTEL_EXPORTER_OTLP_ENDPOINT   (default http://localhost:4318)
//   - OTEL_EXPORTER_OTLP_INSECURE   (default true in dev via otlptracehttp.WithInsecure)
//   - OTEL_SERVICE_NAME             (override via the serviceName param)
//   - OTEL_RESOURCE_ATTRIBUTES      (attached to every span)
//
// Metrics are exported via Prometheus at :PROM_PORT/metrics (default 9464).
// The sampler is the SDK default (ParentBased(AlwaysSample)) — changing it
// requires a code edit; OTEL_TRACES_SAMPLER is NOT read automatically by
// the Go SDK.
package otel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Init configures the global TracerProvider (OTLP/HTTP) and MeterProvider
// (Prometheus) and starts the Prometheus metrics server on PROM_PORT
// (default 9464). Returns a shutdown function that flushes pending spans
// and closes the metrics server in reverse-construction order.
//
// On partial-init failure the returned error is non-nil AND any resources
// already constructed are torn down before the call returns, so the caller
// can simply not defer the (nil) shutdown.
func Init(ctx context.Context, serviceName string, logger *slog.Logger) (shutdown func(context.Context) error, err error) {
	var teardown []func(context.Context) error
	// onError composes the teardown chain into a single shutdown to return
	// on any failure mid-init, preventing resource leaks.
	onError := func() {
		combined := composeShutdown(teardown)
		_ = combined(context.Background())
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	// --- Traces: OTLP/HTTP → collector ---
	// WithInsecure matches the comment at the top of the file: in local dev
	// the collector is plain HTTP on :4318. The Go OTLP/HTTP SDK defaults to
	// HTTPS when no scheme is given in OTEL_EXPORTER_OTLP_ENDPOINT, so
	// without this flag every export fails with "server gave HTTP response
	// to HTTPS client". Override by setting OTEL_EXPORTER_OTLP_ENDPOINT
	// explicitly to an https URL for TLS-terminated collectors.
	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// W3C TraceContext is load-bearing: without a global propagator,
	// otel.GetTextMapPropagator().Inject / Extract are no-ops and no
	// traceparent flows through Kafka headers or HTTP requests.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	teardown = append(teardown, tp.Shutdown)

	// --- Metrics: Prometheus scrape endpoint ---
	promExporter, err := prometheus.New()
	if err != nil {
		onError()
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	teardown = append(teardown, mp.Shutdown)

	// Bind the Prometheus listener synchronously so port-conflicts surface
	// as an Init error rather than a silent background log line.
	promPort := os.Getenv("PROM_PORT")
	if promPort == "" {
		promPort = "9464"
	}
	ln, err := net.Listen("tcp", ":"+promPort)
	if err != nil {
		onError()
		return nil, fmt.Errorf("prom listen :%s: %w", promPort, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(
		promclient.DefaultGatherer,
		promhttp.HandlerOpts{EnableOpenMetrics: true},
	))
	promSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Info("prometheus metrics server starting", "addr", ln.Addr().String())
		if err := promSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("prometheus server error", "err", err)
		}
	}()
	teardown = append(teardown, promSrv.Shutdown)

	return composeShutdown(teardown), nil
}

// composeShutdown returns a single func that invokes each teardown in
// reverse construction order and reports the first error encountered while
// still running every subsequent step.
func composeShutdown(steps []func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var firstErr error
		for i := len(steps) - 1; i >= 0; i-- {
			if err := steps[i](ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}
