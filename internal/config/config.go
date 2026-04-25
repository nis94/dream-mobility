package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
)

// ParseLogLevel reads the LOG_LEVEL env (DEBUG / INFO / WARN / ERROR,
// case-insensitive) and returns the matching slog.Level. Unset or
// unrecognized values fall back to Info — the safe production default.
// DEBUG in particular exposes per-event Processing lines from the
// stream-processor and per-batch flush lines from the clickhouse-sink.
func ParseLogLevel(raw string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

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
		KafkaTopic:        envOrDefault("KAFKA_TOPIC", "flight.telemetry"),
		KafkaGroupID:      envOrDefault("KAFKA_GROUP_ID", "flight-postgres"),
		SchemaRegistryURL: envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		PostgresDSN:       envOrDefault("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/flight?sslmode=disable"),
		ClickHouseAddr:    envOrDefault("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:      envOrDefault("CLICKHOUSE_DB", "flight"),
	}, nil
}

// RedactDSN strips the password from a URL-style DSN so it is safe to emit
// in startup logs. The username is preserved for debug clarity. Returns
// "<unparseable>" if the input does not parse as a URL.
//
// Also handles libpq key-value DSNs (`host=... password=...`): url.Parse is
// liberal and will happily "parse" those without extracting the password,
// leaving it in the returned string. We detect that shape and replace the
// password value with a sentinel.
func RedactDSN(raw string) string {
	if strings.Contains(strings.ToLower(raw), "password=") && !strings.HasPrefix(raw, "postgres://") && !strings.HasPrefix(raw, "postgresql://") {
		return redactKVDSN(raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable>"
	}
	if u.User != nil {
		u.User = url.User(u.User.Username())
	}
	return u.String()
}

// redactKVDSN replaces the value of any password= assignment in a libpq-style
// key-value DSN with "<redacted>". The parser is deliberately simple: splits
// on whitespace and on unquoted `=`. Handles the common case; pathological
// inputs (quoted passwords with spaces) still get redacted down to the point
// of the first whitespace, which is the safe failure mode.
func redactKVDSN(raw string) string {
	parts := strings.Fields(raw)
	for i, part := range parts {
		if idx := strings.Index(part, "="); idx > 0 {
			key := strings.ToLower(part[:idx])
			if key == "password" {
				parts[i] = "password=<redacted>"
			}
		}
	}
	return strings.Join(parts, " ")
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
