//go:build integration

package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	avroschema "github.com/nis94/dream-flight/internal/avro"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgreSQL SQLSTATE 23514 = check_violation.
const pgCheckViolation = "23514"

// testUUID generates a deterministic UUID-shaped string for test data.
func testUUID(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}

// hexIcao24 generates a deterministic 6-char lowercase hex icao24 for tests.
func hexIcao24(n int) string {
	return fmt.Sprintf("%06x", n)
}

// startPostgres spins up a Postgres container via testcontainers and returns
// a connected Store (with migrations applied) plus a cleanup function.
func startPostgres(t *testing.T) (*Store, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn := "postgres://test:test@" + host + ":" + port.Port() + "/testdb?sslmode=disable"
	store, err := NewStore(ctx, dsn)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	cleanup := func() {
		store.Close()
		container.Terminate(ctx) //nolint:errcheck
	}
	return store, cleanup
}

func ptr[T any](v T) *T { return &v }

// baseEvent constructs a minimal valid FlightTelemetry test fixture. Tests
// override fields they care about and leave the rest as defaults.
func baseEvent(id, icao24 string, ts time.Time, lat, lon float64) *avroschema.FlightTelemetry {
	return &avroschema.FlightTelemetry{
		EventID:        id,
		Icao24:         icao24,
		OriginCountry:  "GB",
		ObservedAt:     ts,
		PositionSource: "ADSB",
		Lat:            lat,
		Lon:            lon,
		OnGround:       false,
		Spi:            false,
	}
}

func TestInsertEvent_Deduplicate(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	ts := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	ev := baseEvent(testUUID(1), hexIcao24(1), ts, 52.52, 13.405)

	// First insert should succeed.
	r1, err := store.InsertEvent(ctx, ev)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !r1.RawInserted {
		t.Error("expected RawInserted=true on first insert")
	}
	if !r1.StateUpdated {
		t.Error("expected StateUpdated=true on first insert")
	}

	// Second insert with same event_id should be a no-op for flight_telemetry.
	r2, err := store.InsertEvent(ctx, ev)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if r2.RawInserted {
		t.Error("expected RawInserted=false for duplicate")
	}
	// State should NOT update either (same observed_at, not strictly greater).
	if r2.StateUpdated {
		t.Error("expected StateUpdated=false for duplicate")
	}

	// Verify flight_telemetry count.
	var count int
	err = store.pool.QueryRow(ctx, "SELECT count(*) FROM flight_telemetry WHERE event_id = $1", ev.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("flight_telemetry count = %d, want 1", count)
	}
}

func TestInsertEvent_OutOfOrder(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	t1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 10, 5, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 1, 10, 3, 0, 0, time.UTC) // out of order (between t1 and t2)

	icao := hexIcao24(0xABC)
	events := []*avroschema.FlightTelemetry{
		baseEvent(testUUID(10), icao, t1, 52.0, 13.0),
		baseEvent(testUUID(11), icao, t2, 52.1, 13.1),
		baseEvent(testUUID(12), icao, t3, 52.05, 13.05), // arrives late
	}

	for _, ev := range events {
		if _, err := store.InsertEvent(ctx, ev); err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// All 3 events should be in flight_telemetry (different event_ids).
	var rawCount int
	err := store.pool.QueryRow(ctx, "SELECT count(*) FROM flight_telemetry WHERE icao24 = $1", icao).Scan(&rawCount)
	if err != nil {
		t.Fatalf("count raw: %v", err)
	}
	if rawCount != 3 {
		t.Errorf("flight_telemetry count = %d, want 3", rawCount)
	}

	// aircraft_state should reflect t2 (the latest timestamp), NOT t3.
	var stateTS time.Time
	var stateLat, stateLon float64
	var lastEventID string
	err = store.pool.QueryRow(ctx,
		"SELECT observed_at, lat, lon, last_event_id FROM aircraft_state WHERE icao24=$1",
		icao,
	).Scan(&stateTS, &stateLat, &stateLon, &lastEventID)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	if !stateTS.Equal(t2) {
		t.Errorf("state observed_at = %v, want %v (should be the latest)", stateTS, t2)
	}
	if stateLat != 52.1 || stateLon != 13.1 {
		t.Errorf("state position = (%f, %f), want (52.1, 13.1)", stateLat, stateLon)
	}
	if lastEventID != testUUID(11) {
		t.Errorf("last_event_id = %q, want %q", lastEventID, testUUID(11))
	}
}

func TestInsertEvent_MultipleAircraft(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ev1 := baseEvent(testUUID(20), hexIcao24(0x100), ts, 52.0, 13.0)
	ev1.Callsign = ptr("BAW123")
	ev1.VelocityMs = ptr(245.5)

	ev2 := baseEvent(testUUID(21), hexIcao24(0x101), ts, 40.7, -74.0)
	ev2.OriginCountry = "US"

	ev3 := baseEvent(testUUID(22), hexIcao24(0x102), ts, 35.6, 139.6)
	ev3.OriginCountry = "JP"
	ev3.Squawk = ptr("7700") // emergency

	for _, ev := range []*avroschema.FlightTelemetry{ev1, ev2, ev3} {
		if _, err := store.InsertEvent(ctx, ev); err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// 3 raw observations.
	var rawCount int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM flight_telemetry").Scan(&rawCount); err != nil {
		t.Fatalf("count flight_telemetry: %v", err)
	}
	if rawCount != 3 {
		t.Errorf("flight_telemetry = %d, want 3", rawCount)
	}

	// 3 distinct aircraft state rows.
	var stateCount int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM aircraft_state").Scan(&stateCount); err != nil {
		t.Fatalf("count aircraft_state: %v", err)
	}
	if stateCount != 3 {
		t.Errorf("aircraft_state = %d, want 3", stateCount)
	}

	// Verify the emergency squawk is preserved on aircraft_state for ev3.
	var sq *string
	if err := store.pool.QueryRow(ctx, "SELECT last_squawk FROM aircraft_state WHERE icao24=$1", ev3.Icao24).Scan(&sq); err != nil {
		t.Fatalf("query squawk: %v", err)
	}
	if sq == nil || *sq != "7700" {
		t.Errorf("last_squawk = %v, want 7700", sq)
	}
}

func TestInsertEvent_MixedBatch(t *testing.T) {
	// A mixed batch with originals, dupes, and out-of-order events across
	// multiple aircraft.
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	t1 := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 3, 1, 10, 1, 0, 0, time.UTC)
	t3 := time.Date(2025, 3, 1, 10, 2, 0, 0, time.UTC)
	t0 := time.Date(2025, 3, 1, 9, 59, 0, 0, time.UTC) // earlier than t1

	a1 := testUUID(30)
	a2 := testUUID(31)
	a3 := testUUID(32)
	b1 := testUUID(33)
	c1 := testUUID(34)
	c2 := testUUID(35)

	icaoA := hexIcao24(0xA00)
	icaoB := hexIcao24(0xB00)
	icaoC := hexIcao24(0xC00)

	batch := []*avroschema.FlightTelemetry{
		// Aircraft A: 3 events arriving in order.
		baseEvent(a1, icaoA, t1, 1.0, 1.0),
		baseEvent(a2, icaoA, t2, 2.0, 2.0),
		baseEvent(a3, icaoA, t3, 3.0, 3.0),
		// Aircraft A duplicate: same event_id as a1.
		baseEvent(a1, icaoA, t1, 1.0, 1.0),
		// Aircraft B: 1 event.
		baseEvent(b1, icaoB, t2, 10.0, 10.0),
		// Aircraft C: out-of-order — newer arrives first.
		baseEvent(c2, icaoC, t3, 20.0, 20.0),
		baseEvent(c1, icaoC, t0, 19.0, 19.0),
	}

	for _, ev := range batch {
		if _, err := store.InsertEvent(ctx, ev); err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// flight_telemetry: 6 unique event_ids (a1,a2,a3,b1,c1,c2) — duplicate a1 ignored.
	var rawCount int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM flight_telemetry").Scan(&rawCount); err != nil {
		t.Fatalf("count flight_telemetry: %v", err)
	}
	if rawCount != 6 {
		t.Errorf("flight_telemetry = %d, want 6 (duplicate should be ignored)", rawCount)
	}

	// Aircraft A: state at t3 (latest).
	assertState(t, store.pool, icaoA, t3, 3.0, 3.0, a3)
	// Aircraft B: state at t2.
	assertState(t, store.pool, icaoB, t2, 10.0, 10.0, b1)
	// Aircraft C: state at t3 (c2), NOT t0 (c1 arrived late).
	assertState(t, store.pool, icaoC, t3, 20.0, 20.0, c2)
}

func assertState(t *testing.T, pool *pgxpool.Pool, icao24 string, wantTS time.Time, wantLat, wantLon float64, wantEventID string) {
	t.Helper()
	var ts time.Time
	var lat, lon float64
	var eid string
	err := pool.QueryRow(context.Background(),
		"SELECT observed_at, lat, lon, last_event_id FROM aircraft_state WHERE icao24=$1",
		icao24,
	).Scan(&ts, &lat, &lon, &eid)
	if err != nil {
		t.Fatalf("query state %s: %v", icao24, err)
	}
	if !ts.Equal(wantTS) {
		t.Errorf("%s observed_at = %v, want %v", icao24, ts, wantTS)
	}
	if lat != wantLat || lon != wantLon {
		t.Errorf("%s position = (%f, %f), want (%f, %f)", icao24, lat, lon, wantLat, wantLon)
	}
	if eid != wantEventID {
		t.Errorf("%s last_event_id = %q, want %q", icao24, eid, wantEventID)
	}
}

// TestInsertEvent_BadCoordinates asserts that the lat/lon CHECK constraints
// on flight_telemetry reject out-of-range values. Defense-in-depth against
// a decoder bug — the handler-side ValidateEvent also enforces these ranges.
func TestInsertEvent_BadCoordinates(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	ts := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		lat, lon float64
	}{
		{"lat too high", 91.0, 0.0},
		{"lat too low", -91.0, 0.0},
		{"lon too high", 0.0, 181.0},
		{"lon too low", 0.0, -181.0},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := baseEvent(testUUID(100+i), hexIcao24(0xBAD000+i), ts, tc.lat, tc.lon)
			_, err := store.InsertEvent(ctx, ev)
			if err == nil {
				t.Fatalf("expected CHECK violation for lat=%v lon=%v, got nil", tc.lat, tc.lon)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("expected *pgconn.PgError, got %T: %v", err, err)
			}
			if pgErr.Code != pgCheckViolation {
				t.Errorf("SQLSTATE = %q, want %q (check_violation); msg=%s", pgErr.Code, pgCheckViolation, pgErr.Message)
			}
		})
	}
}
