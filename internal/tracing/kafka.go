// Package tracing holds small OTel helpers that don't fit in internal/otel
// (which is reserved for SDK bootstrap).
//
// This file provides a TextMapCarrier over Kafka message headers so W3C
// TraceContext can propagate from producer to consumer. segmentio/kafka-go
// has no upstream OTel instrumentation, so we do it by hand.
package tracing

import "github.com/segmentio/kafka-go"

// KafkaHeaderCarrier adapts a kafka.Message's Headers slice to the
// propagation.TextMapCarrier interface. Get is O(n) but n is typically 1-2
// (traceparent + tracestate) so the scan is negligible against the cost of
// a Kafka round-trip.
type KafkaHeaderCarrier []kafka.Header

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces any existing header with the same key. New Set calls append.
// Producer-side we always start from an empty slice so the dup path never
// fires in practice, but guarding it keeps the carrier contract-compliant.
func (c *KafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, kafka.Header{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
