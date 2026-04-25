-- 001_init.sql — Postgres schema for the Dream Flight pipeline.
--
-- flight_telemetry: append-only, deduplicated by event_id via ON CONFLICT DO NOTHING.
--                   Stores every observation; one row per (icao24, observed_at, event_id).
-- aircraft_state:   last-known state per aircraft, updated by a timestamp-gated
--                   upsert (newer observed_at always wins; out-of-order events
--                   are silently discarded — the existing row is more recent).
--
-- The Avro field timestamp is named `observed_at` here (not `timestamp`) since
-- `timestamp` is a reserved SQL type keyword and rejected by stricter linters.

CREATE TABLE IF NOT EXISTS flight_telemetry (
    event_id         UUID            PRIMARY KEY,
    icao24           TEXT            NOT NULL,
    callsign         TEXT,
    origin_country   TEXT            NOT NULL,
    observed_at      TIMESTAMPTZ     NOT NULL,
    position_source  TEXT            NOT NULL,
    lat              DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon              DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    baro_altitude_m  DOUBLE PRECISION,
    geo_altitude_m   DOUBLE PRECISION,
    velocity_ms      DOUBLE PRECISION,
    true_track_deg   DOUBLE PRECISION,
    vertical_rate_ms DOUBLE PRECISION,
    on_ground        BOOLEAN         NOT NULL,
    squawk           TEXT,
    spi              BOOLEAN         NOT NULL,
    category         SMALLINT,
    ingested_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Per-aircraft trajectory queries: "all observations for icao24 X over the
-- last hour". Descending observed_at supports the trivial LIMIT N most-recent
-- pattern.
CREATE INDEX IF NOT EXISTS idx_flight_telemetry_icao24_observed_at
    ON flight_telemetry (icao24, observed_at DESC);

-- Country-scoped queries: "all UK aircraft observed in the last 5 minutes".
-- Drives the country leaderboard panels in Grafana.
CREATE INDEX IF NOT EXISTS idx_flight_telemetry_country_observed_at
    ON flight_telemetry (origin_country, observed_at DESC);

-- aircraft_state: latest known state per aircraft. The query-api's
-- /flights/active and /flights/by-country endpoints read directly from this
-- table — point lookups that don't need to scan flight_telemetry.
--
-- Tie semantics: the upsert below uses strict `>`, so on equal observed_at
-- the existing row wins and its last_event_id is retained. The ClickHouse
-- ReplacingMergeTree mirror MUST use `version = observed_at` and the same
-- tiebreak rule to stay consistent.
CREATE TABLE IF NOT EXISTS aircraft_state (
    icao24          TEXT            PRIMARY KEY,
    last_callsign   TEXT,
    origin_country  TEXT            NOT NULL,
    observed_at     TIMESTAMPTZ     NOT NULL,
    lat             DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon             DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    baro_altitude_m DOUBLE PRECISION,
    velocity_ms     DOUBLE PRECISION,
    on_ground       BOOLEAN         NOT NULL,
    last_squawk     TEXT,
    last_event_id   UUID            NOT NULL,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- "Active aircraft" panels filter aircraft_state on observed_at within a
-- short rolling window. The covering index on observed_at lets that scan
-- skip the heap entirely.
CREATE INDEX IF NOT EXISTS idx_aircraft_state_observed_at
    ON aircraft_state (observed_at DESC);
