package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Config holds application configuration, loaded from environment variables.
// Each service uses the subset of fields it needs; unused fields carry defaults.
type Config struct {
	HTTPPort          string
	QueryHTTPPort     string
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaGroupID      string
	SchemaRegistryURL string
	PostgresDSN       string
	ClickHouseAddr    string
	ClickHouseDB      string
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
		QueryHTTPPort:     envOrDefault("QUERY_HTTP_PORT", "8090"),
		KafkaBrokers:      brokers,
		KafkaTopic:        envOrDefault("KAFKA_TOPIC", "movement.events"),
		KafkaGroupID:      envOrDefault("KAFKA_GROUP_ID", "mobility-postgres"),
		SchemaRegistryURL: envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		PostgresDSN:       envOrDefault("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/mobility?sslmode=disable"),
		ClickHouseAddr:    envOrDefault("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:      envOrDefault("CLICKHOUSE_DB", "mobility"),
	}, nil
}

// RedactDSN strips the password from a URL-style DSN so it is safe to emit
// in startup logs. The username is preserved for debug clarity. Returns
// "<unparseable>" if the input does not parse as a URL.
func RedactDSN(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable>"
	}
	if u.User != nil {
		u.User = url.User(u.User.Username())
	}
	return u.String()
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
