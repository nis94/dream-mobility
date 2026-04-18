package config

import (
	"fmt"
	"os"
	"strings"
)

// Config holds application configuration, loaded from environment variables.
type Config struct {
	HTTPPort          string
	KafkaBrokers      []string
	KafkaTopic        string
	SchemaRegistryURL string
}

// Load reads configuration from environment variables with sensible defaults
// for local development (matching deploy/docker-compose.yml).
//
// Returns an error if any env var parses to an empty/invalid value after
// normalization (e.g. KAFKA_BROKERS=" , ").
func Load() (Config, error) {
	brokers, err := parseBrokers(envOrDefault("KAFKA_BROKERS", "localhost:29092"))
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_BROKERS: %w", err)
	}
	return Config{
		HTTPPort:          envOrDefault("HTTP_PORT", "8080"),
		KafkaBrokers:      brokers,
		KafkaTopic:        envOrDefault("KAFKA_TOPIC", "movement.events"),
		SchemaRegistryURL: envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
	}, nil
}

// parseBrokers splits a comma-separated list of host:port pairs, trims
// surrounding whitespace on each entry, and drops empty entries. Returns an
// error if the resulting list is empty.
func parseBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no broker addresses")
	}
	return out, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
