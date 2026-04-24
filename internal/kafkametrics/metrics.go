// Package kafkametrics exports kafka-go consumer Reader.Stats() as
// Prometheus metrics. One authorized caller: Reader.Stats() resets its
// cumulative counters on every read, so running the sampler from two
// goroutines at once would silently split the counts.
//
// Metrics (uniform across every service that creates a Recorder):
//
//	kafka_consumer_lag              gauge   — messages behind the high-water mark
//	kafka_messages_total            counter — messages delivered by the reader
//	kafka_bytes_total               counter — bytes delivered by the reader
//	kafka_decode_failures_total     counter — wire/Avro decode failures
//
// The Prometheus `job` label (set at scrape time from the scrape config
// entry) distinguishes stream-processor from clickhouse-sink; no extra
// service-name label is needed on the metric itself.
package kafkametrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

// Recorder bundles the four per-consumer metrics. Create one per service at
// startup and share it between the Processor/Sink and any Sample goroutine.
type Recorder struct {
	lag            prometheus.Gauge
	messagesTotal  prometheus.Counter
	bytesTotal     prometheus.Counter
	decodeFailures prometheus.Counter
}

// New registers the metrics against the default Prometheus registry. Safe
// to call once per process. A second call panics because Prometheus refuses
// duplicate registrations — that's intentional; it forces tests to build
// their own registry if they need isolation.
func New() *Recorder {
	return &Recorder{
		lag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current consumer lag in messages for this reader.",
		}),
		messagesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "Total Kafka messages consumed by this reader.",
		}),
		bytesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_bytes_total",
			Help: "Total Kafka bytes consumed by this reader.",
		}),
		decodeFailures: promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_decode_failures_total",
			Help: "Total messages that failed wire-format or Avro decode and were committed past.",
		}),
	}
}

// IncDecodeFailure is the only write path for the decode_failures counter.
// Callers should invoke it from their decode-failure branch before returning
// nil (committing past the poison message).
func (r *Recorder) IncDecodeFailure() {
	r.decodeFailures.Inc()
}

// Sample periodically drains Reader.Stats() into the Prom metrics. Must be
// invoked from exactly one goroutine (kafka-go resets counters on read).
// Returns when ctx is done.
func (r *Recorder) Sample(ctx context.Context, reader *kafka.Reader, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := reader.Stats()
			r.lag.Set(float64(s.Lag))
			if s.Messages > 0 {
				r.messagesTotal.Add(float64(s.Messages))
			}
			if s.Bytes > 0 {
				r.bytesTotal.Add(float64(s.Bytes))
			}
		}
	}
}
