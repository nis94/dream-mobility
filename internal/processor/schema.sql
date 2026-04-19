-- 001_init.sql — Core schema for the movement intelligence pipeline.
--
-- raw_events:       append-only, deduped by event_id via ON CONFLICT DO NOTHING.
-- entity_positions: last-known position per entity, updated by a timestamp-gated
--                   upsert (newer timestamp always wins; out-of-order events are
--                   silently discarded).

CREATE TABLE IF NOT EXISTS raw_events (
    event_id    UUID            PRIMARY KEY,
    entity_type TEXT            NOT NULL,
    entity_id   TEXT            NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    speed_kmh   DOUBLE PRECISION,
    heading_deg DOUBLE PRECISION,
    accuracy_m  DOUBLE PRECISION,
    source      TEXT,
    attributes  JSONB,
    ingested_at TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Covers lookups by entity + time range (Query API Phase 4).
CREATE INDEX IF NOT EXISTS idx_raw_events_entity_ts
    ON raw_events (entity_type, entity_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS entity_positions (
    entity_type   TEXT            NOT NULL,
    entity_id     TEXT            NOT NULL,
    timestamp     TIMESTAMPTZ     NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    lon           DOUBLE PRECISION NOT NULL,
    last_event_id UUID            NOT NULL,
    updated_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
);
