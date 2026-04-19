//go:build integration

package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	avroschema "github.com/nis94/dream-mobility/internal/avro"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// testUUID generates a deterministic UUID-shaped string for test data.
func testUUID(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
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

func TestInsertEvent_Deduplicate(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	ts := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	ev := &avroschema.MovementEvent{
		EventID:    testUUID(1),
		EntityType: "vehicle",
		EntityID:   "v1",
		Timestamp:  ts,
		Lat:        52.52,
		Lon:        13.405,
	}

	// First insert should succeed.
	r1, err := store.InsertEvent(ctx, ev)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !r1.RawInserted {
		t.Error("expected RawInserted=true on first insert")
	}
	if !r1.PositionUpdated {
		t.Error("expected PositionUpdated=true on first insert")
	}

	// Second insert with same event_id should be a no-op for raw_events.
	r2, err := store.InsertEvent(ctx, ev)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if r2.RawInserted {
		t.Error("expected RawInserted=false for duplicate")
	}
	// Position should NOT update either (same timestamp, not strictly greater).
	if r2.PositionUpdated {
		t.Error("expected PositionUpdated=false for duplicate")
	}

	// Verify raw_events count.
	var count int
	err = store.pool.QueryRow(ctx, "SELECT count(*) FROM raw_events WHERE event_id = $1", ev.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("raw_events count = %d, want 1", count)
	}
}

func TestInsertEvent_OutOfOrder(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	t1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 10, 5, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 1, 10, 3, 0, 0, time.UTC) // out of order (between t1 and t2)

	events := []*avroschema.MovementEvent{
		{EventID: testUUID(10), EntityType: "vehicle", EntityID: "v1", Timestamp: t1, Lat: 52.0, Lon: 13.0},
		{EventID: testUUID(11), EntityType: "vehicle", EntityID: "v1", Timestamp: t2, Lat: 52.1, Lon: 13.1},
		{EventID: testUUID(12), EntityType: "vehicle", EntityID: "v1", Timestamp: t3, Lat: 52.05, Lon: 13.05}, // arrives late
	}

	for _, ev := range events {
		if _, err := store.InsertEvent(ctx, ev); err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// All 3 events should be in raw_events (different event_ids).
	var rawCount int
	err := store.pool.QueryRow(ctx, "SELECT count(*) FROM raw_events WHERE entity_id = 'v1'").Scan(&rawCount)
	if err != nil {
		t.Fatalf("count raw: %v", err)
	}
	if rawCount != 3 {
		t.Errorf("raw_events count = %d, want 3", rawCount)
	}

	// entity_positions should reflect t2 (the latest timestamp), NOT t3.
	var posTS time.Time
	var posLat, posLon float64
	var lastEventID string
	err = store.pool.QueryRow(ctx,
		"SELECT timestamp, lat, lon, last_event_id FROM entity_positions WHERE entity_type='vehicle' AND entity_id='v1'",
	).Scan(&posTS, &posLat, &posLon, &lastEventID)
	if err != nil {
		t.Fatalf("query position: %v", err)
	}
	if !posTS.Equal(t2) {
		t.Errorf("position timestamp = %v, want %v (should be the latest)", posTS, t2)
	}
	if posLat != 52.1 || posLon != 13.1 {
		t.Errorf("position = (%f, %f), want (52.1, 13.1)", posLat, posLon)
	}
	if lastEventID != testUUID(11) {
		t.Errorf("last_event_id = %q, want %q", lastEventID, testUUID(11))
	}
}

func TestInsertEvent_MultipleEntities(t *testing.T) {
	store, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []*avroschema.MovementEvent{
		{EventID: testUUID(20), EntityType: "vehicle", EntityID: "v1", Timestamp: ts, Lat: 52.0, Lon: 13.0, SpeedKmh: ptr(40.0), Source: ptr("gps")},
		{EventID: testUUID(21), EntityType: "courier", EntityID: "c1", Timestamp: ts, Lat: 40.7, Lon: -74.0},
		{EventID: testUUID(22), EntityType: "vehicle", EntityID: "v2", Timestamp: ts, Lat: 35.6, Lon: 139.6, Attributes: ptr(`{"battery_level":0.82}`)},
	}

	for _, ev := range events {
		if _, err := store.InsertEvent(ctx, ev); err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// 3 raw events.
	var rawCount int
	store.pool.QueryRow(ctx, "SELECT count(*) FROM raw_events").Scan(&rawCount)
	if rawCount != 3 {
		t.Errorf("raw_events = %d, want 3", rawCount)
	}

	// 3 distinct entity positions.
	var posCount int
	store.pool.QueryRow(ctx, "SELECT count(*) FROM entity_positions").Scan(&posCount)
	if posCount != 3 {
		t.Errorf("entity_positions = %d, want 3", posCount)
	}

	// Verify attributes stored as JSONB.
	var attrs *string
	store.pool.QueryRow(ctx, "SELECT attributes::text FROM raw_events WHERE event_id=$1", testUUID(22)).Scan(&attrs)
	if attrs == nil {
		t.Fatal("expected non-nil attributes for me-3")
	}
	if *attrs != `{"battery_level": 0.82}` {
		t.Errorf("attributes = %q", *attrs)
	}
}

func TestInsertEvent_MixedBatch(t *testing.T) {
	// The plan's highest-value test: a mixed batch with originals, dupes,
	// and out-of-order events across multiple entities.
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

	batch := []*avroschema.MovementEvent{
		// Entity A: 3 events, arriving in order.
		{EventID: a1, EntityType: "vehicle", EntityID: "A", Timestamp: t1, Lat: 1.0, Lon: 1.0},
		{EventID: a2, EntityType: "vehicle", EntityID: "A", Timestamp: t2, Lat: 2.0, Lon: 2.0},
		{EventID: a3, EntityType: "vehicle", EntityID: "A", Timestamp: t3, Lat: 3.0, Lon: 3.0},
		// Entity A duplicate: same event_id as a1.
		{EventID: a1, EntityType: "vehicle", EntityID: "A", Timestamp: t1, Lat: 1.0, Lon: 1.0},
		// Entity B: 1 event.
		{EventID: b1, EntityType: "courier", EntityID: "B", Timestamp: t2, Lat: 10.0, Lon: 10.0},
		// Entity C: out-of-order — newer arrives first.
		{EventID: c2, EntityType: "scooter", EntityID: "C", Timestamp: t3, Lat: 20.0, Lon: 20.0},
		{EventID: c1, EntityType: "scooter", EntityID: "C", Timestamp: t0, Lat: 19.0, Lon: 19.0},
	}

	for _, ev := range batch {
		_, err := store.InsertEvent(ctx, ev)
		if err != nil {
			t.Fatalf("insert %s: %v", ev.EventID, err)
		}
	}

	// raw_events: 6 unique event_ids (a1,a2,a3,b1,c1,c2) — the duplicate a1 is ignored.
	var rawCount int
	store.pool.QueryRow(ctx, "SELECT count(*) FROM raw_events").Scan(&rawCount)
	if rawCount != 6 {
		t.Errorf("raw_events = %d, want 6 (duplicate should be ignored)", rawCount)
	}

	// Entity A: position should be at t3 (latest).
	assertPosition(t, store.pool, "vehicle", "A", t3, 3.0, 3.0, a3)
	// Entity B: position at t2.
	assertPosition(t, store.pool, "courier", "B", t2, 10.0, 10.0, b1)
	// Entity C: position should be at t3 (c2), NOT t0 (c1 arrived late).
	assertPosition(t, store.pool, "scooter", "C", t3, 20.0, 20.0, c2)
}

func assertPosition(t *testing.T, pool *pgxpool.Pool, entityType, entityID string, wantTS time.Time, wantLat, wantLon float64, wantEventID string) {
	t.Helper()
	var ts time.Time
	var lat, lon float64
	var eid string
	err := pool.QueryRow(context.Background(),
		"SELECT timestamp, lat, lon, last_event_id FROM entity_positions WHERE entity_type=$1 AND entity_id=$2",
		entityType, entityID,
	).Scan(&ts, &lat, &lon, &eid)
	if err != nil {
		t.Fatalf("query position %s:%s: %v", entityType, entityID, err)
	}
	if !ts.Equal(wantTS) {
		t.Errorf("%s:%s timestamp = %v, want %v", entityType, entityID, ts, wantTS)
	}
	if lat != wantLat || lon != wantLon {
		t.Errorf("%s:%s position = (%f, %f), want (%f, %f)", entityType, entityID, lat, lon, wantLat, wantLon)
	}
	if eid != wantEventID {
		t.Errorf("%s:%s last_event_id = %q, want %q", entityType, entityID, eid, wantEventID)
	}
}
