package config

import (
	"reflect"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	for _, k := range []string{"HTTP_PORT", "KAFKA_BROKERS", "KAFKA_TOPIC", "SCHEMA_REGISTRY_URL"} {
		t.Setenv(k, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPPort != "8080" {
		t.Errorf("HTTPPort = %q, want 8080", cfg.HTTPPort)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:29092"}) {
		t.Errorf("KafkaBrokers = %v, want [localhost:29092]", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "flight.telemetry" {
		t.Errorf("KafkaTopic = %q, want flight.telemetry", cfg.KafkaTopic)
	}
	if cfg.SchemaRegistryURL != "http://localhost:8081" {
		t.Errorf("SchemaRegistryURL = %q", cfg.SchemaRegistryURL)
	}
}

func TestLoad_TrimsAndDropsEmptyBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "  a:9092 , , b:9092  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a:9092", "b:9092"}
	if !reflect.DeepEqual(cfg.KafkaBrokers, want) {
		t.Errorf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, want)
	}
}

func TestLoad_ErrorsOnAllEmptyBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " , , ")

	_, err := Load()
	if err == nil {
		t.Error("expected error when KAFKA_BROKERS has no non-empty entries")
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("HTTP_PORT", "9000")
	t.Setenv("KAFKA_BROKERS", "broker-1:9092,broker-2:9092,broker-3:9092")
	t.Setenv("KAFKA_TOPIC", "custom.topic")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://sr:8081")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPPort != "9000" {
		t.Errorf("HTTPPort = %q", cfg.HTTPPort)
	}
	if len(cfg.KafkaBrokers) != 3 {
		t.Errorf("KafkaBrokers len = %d, want 3", len(cfg.KafkaBrokers))
	}
	if cfg.KafkaTopic != "custom.topic" {
		t.Errorf("KafkaTopic = %q", cfg.KafkaTopic)
	}
	if cfg.SchemaRegistryURL != "http://sr:8081" {
		t.Errorf("SchemaRegistryURL = %q", cfg.SchemaRegistryURL)
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "password stripped, username retained",
			in:   "postgres://user:pass@host:5432/db?sslmode=disable",
			want: "postgres://user@host:5432/db?sslmode=disable",
		},
		{
			name: "no password present",
			in:   "postgres://user@host:5432/db",
			want: "postgres://user@host:5432/db",
		},
		{
			name: "no userinfo",
			in:   "postgres://host:5432/db",
			want: "postgres://host:5432/db",
		},
		{
			name: "empty input",
			in:   "",
			want: "",
		},
		{
			name: "unparseable → sentinel",
			in:   "://broken",
			want: "<unparseable>",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactDSN(tc.in)
			if got != tc.want {
				t.Errorf("RedactDSN(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
