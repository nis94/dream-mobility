package config

import (
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
func Load() Config {
	return Config{
		HTTPPort:          envOrDefault("HTTP_PORT", "8080"),
		KafkaBrokers:      strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:29092"), ","),
		KafkaTopic:        envOrDefault("KAFKA_TOPIC", "movement.events"),
		SchemaRegistryURL: envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
