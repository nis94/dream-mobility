-- 001_init.sql — Core schema for the movement intelligence pipeline.
--
-- raw_events:       append-only, deduped by event_id via ON CONFLICT DO NOTHING.
-- entity_positions: last-known position per entity, updated by a timestamp-gated
--                   upsert (newer event_ts always wins; out-of-order events are
--                   silently discarded).
--
-- Column name note: the event timestamp is stored as `event_ts`, not
-- `timestamp`. `timestamp` is a reserved SQL type keyword; using it as a
-- column name works in PostgreSQL today but is rejected by stricter SQL
-- dialects and linters (sqlfluff, etc.). The Avro field remains named
-- `timestamp`; the Go struct `MovementEvent.Timestamp` maps to this column
-- positionally in the INSERT statements.

CREATE TABLE IF NOT EXISTS raw_events (
    event_id    UUID            PRIMARY KEY,
    entity_type TEXT            NOT NULL,
    entity_id   TEXT            NOT NULL,
    event_ts    TIMESTAMPTZ     NOT NULL,
    lat         DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon         DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    speed_kmh   DOUBLE PRECISION,
    heading_deg DOUBLE PRECISION,
    accuracy_m  DOUBLE PRECISION,
    source      TEXT,
    attributes  JSONB,
    ingested_at TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Covers lookups by entity + time range (Query API Phase 4).
CREATE INDEX IF NOT EXISTS idx_raw_events_entity_ts
    ON raw_events (entity_type, entity_id, event_ts DESC);

-- Tie semantics for the upsert below: the WHERE clause uses strict `>`, so
-- on equal event_ts the existing row wins and its last_event_id is retained.
-- Any future sink replicating this state (ClickHouse ReplacingMergeTree,
-- Iceberg MERGE) MUST use `version = event_ts` and the same tiebreak rule
-- to stay consistent.
CREATE TABLE IF NOT EXISTS entity_positions (
    entity_type   TEXT            NOT NULL,
    entity_id     TEXT            NOT NULL,
    event_ts      TIMESTAMPTZ     NOT NULL,
    lat           DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon           DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    last_event_id UUID            NOT NULL,
    updated_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
);
