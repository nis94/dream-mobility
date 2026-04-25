-- ClickHouse schema for the Dream Flight pipeline.
--
-- flight.telemetry uses ReplacingMergeTree(observed_at) so duplicate event_ids
-- are eventually collapsed at merge time. Queries that need zero-duplicate
-- reads before compaction MUST use FINAL — the flight.telemetry_final view
-- below is the canonical read target.
--
-- Three rollups feed off the raw table via materialized views, each answering
-- a different operational question:
--   * telemetry_hourly_by_country — country-level airspace operations.
--   * telemetry_hourly_by_airline — airline tempo (callsign prefix is the
--     ICAO airline code).
--   * airspace_grid_5min — geographic density heatmap (0.25° grid cells).
--
-- All three use AggregatingMergeTree with uniqExactState(icao24) rather than
-- count() so at-least-once redelivery does NOT inflate the active-aircraft
-- counts. Speed/altitude averages are stored as sum + count
-- (SimpleAggregateFunction) so merges remain associative.
--
-- The CREATE DATABASE step is done separately in internal/chsink/sink.go
-- before the embedded schema runs, so CREATE DATABASE is NOT in this file.

CREATE TABLE IF NOT EXISTS flight.telemetry (
    event_id          String,
    icao24            LowCardinality(String),
    callsign          Nullable(String),
    origin_country    LowCardinality(String),
    observed_at       DateTime64(6, 'UTC'),
    position_source   LowCardinality(String),
    lat               Float64,
    lon               Float64,
    baro_altitude_m   Nullable(Float64),
    geo_altitude_m    Nullable(Float64),
    velocity_ms       Nullable(Float64),
    true_track_deg    Nullable(Float64),
    vertical_rate_ms  Nullable(Float64),
    on_ground         UInt8,
    squawk            Nullable(String),
    spi               UInt8,
    category          Nullable(Int8),
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (icao24, observed_at, event_id)
-- Keep raw observations for 180 days. Beyond that the lake (Iceberg/MinIO)
-- is the long-term source of truth; ClickHouse is for dashboard-latency
-- analytics. observed_at is DateTime64(6); ClickHouse's TTL engine wants a
-- DateTime (second resolution) so cast.
TTL toDateTime(observed_at) + INTERVAL 180 DAY DELETE;

-- Canonical read view: always reads post-merge so duplicates are collapsed.
CREATE VIEW IF NOT EXISTS flight.telemetry_final AS
    SELECT * FROM flight.telemetry FINAL;

-- ─── Rollup 1: hourly by origin_country ──────────────────────────────────────
-- Country-level airspace operations: how many distinct aircraft from each
-- operator country were active in each hour, plus average ground speed and
-- altitude (airborne-only), plus climb/descent/cruise/on-ground counts and
-- emergency-squawk counts. The vertical_rate threshold |2.5| m/s separates
-- climbing/descending from cruising. Emergency squawks are 7500 (hijack),
-- 7600 (radio failure), 7700 (general emergency).

CREATE TABLE IF NOT EXISTS flight.telemetry_hourly_by_country (
    origin_country         LowCardinality(String),
    hour                   DateTime('UTC'),
    uniq_aircraft          AggregateFunction(uniqExact, String),
    sum_velocity_ms        SimpleAggregateFunction(sum, Float64),
    count_velocity_obs     SimpleAggregateFunction(sum, UInt64),
    sum_baro_altitude_m    SimpleAggregateFunction(sum, Float64),
    count_alt_obs          SimpleAggregateFunction(sum, UInt64),
    climbing_count         SimpleAggregateFunction(sum, UInt64),
    descending_count       SimpleAggregateFunction(sum, UInt64),
    cruising_count         SimpleAggregateFunction(sum, UInt64),
    on_ground_count        SimpleAggregateFunction(sum, UInt64),
    emergency_squawks_count SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (origin_country, hour)
TTL hour + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS flight.telemetry_hourly_by_country_mv
TO flight.telemetry_hourly_by_country
AS SELECT
    origin_country,
    toStartOfHour(observed_at) AS hour,
    uniqExactState(icao24) AS uniq_aircraft,
    -- velocity_ms / baro_altitude_m can be null for on-ground aircraft;
    -- coalesce keeps a 0 flowing into the non-nullable sum but we count only
    -- non-null observations so the average is correct.
    toFloat64(coalesce(sum(if(NOT on_ground, velocity_ms, NULL)), 0)) AS sum_velocity_ms,
    toUInt64(count(if(NOT on_ground, velocity_ms, NULL))) AS count_velocity_obs,
    toFloat64(coalesce(sum(if(NOT on_ground, baro_altitude_m, NULL)), 0)) AS sum_baro_altitude_m,
    toUInt64(count(if(NOT on_ground, baro_altitude_m, NULL))) AS count_alt_obs,
    toUInt64(countIf(vertical_rate_ms > 2.5)) AS climbing_count,
    toUInt64(countIf(vertical_rate_ms < -2.5)) AS descending_count,
    toUInt64(countIf(vertical_rate_ms BETWEEN -2.5 AND 2.5)) AS cruising_count,
    toUInt64(countIf(on_ground)) AS on_ground_count,
    toUInt64(countIf(squawk IN ('7500','7600','7700'))) AS emergency_squawks_count
FROM flight.telemetry
GROUP BY origin_country, hour;

-- Read-facing view: merges aggregate state across unmerged parts and exposes
-- scalar columns. event_count=uniqExactMerge so double-delivery doesn't
-- inflate. avg_velocity / avg_altitude are reconstructed as sum/count so
-- merges stay associative.
CREATE VIEW IF NOT EXISTS flight.telemetry_hourly_by_country_final AS
    SELECT
        origin_country,
        hour,
        uniqExactMerge(uniq_aircraft) AS active_aircraft,
        CASE WHEN sum(count_velocity_obs) > 0
             THEN sum(sum_velocity_ms) / sum(count_velocity_obs)
             ELSE NULL END AS avg_velocity_ms,
        CASE WHEN sum(count_alt_obs) > 0
             THEN sum(sum_baro_altitude_m) / sum(count_alt_obs)
             ELSE NULL END AS avg_baro_altitude_m,
        sum(climbing_count)         AS climbing_count,
        sum(descending_count)       AS descending_count,
        sum(cruising_count)         AS cruising_count,
        sum(on_ground_count)        AS on_ground_count,
        sum(emergency_squawks_count) AS emergency_squawks_count
    FROM flight.telemetry_hourly_by_country
    GROUP BY origin_country, hour;

-- ─── Rollup 2: hourly by airline (callsign prefix = ICAO airline code) ───────
-- The first 3 chars of the callsign identify the operator. Empty / null
-- callsigns map to '' (general aviation, ferry flights, private). Same
-- metric columns as rollup 1 so dashboards can swap groupings without
-- rewriting the query.

CREATE TABLE IF NOT EXISTS flight.telemetry_hourly_by_airline (
    callsign_prefix        LowCardinality(String),
    hour                   DateTime('UTC'),
    uniq_aircraft          AggregateFunction(uniqExact, String),
    uniq_callsigns         AggregateFunction(uniqExact, String),
    sum_velocity_ms        SimpleAggregateFunction(sum, Float64),
    count_velocity_obs     SimpleAggregateFunction(sum, UInt64),
    climbing_count         SimpleAggregateFunction(sum, UInt64),
    descending_count       SimpleAggregateFunction(sum, UInt64),
    cruising_count         SimpleAggregateFunction(sum, UInt64),
    on_ground_count        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (callsign_prefix, hour)
TTL hour + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS flight.telemetry_hourly_by_airline_mv
TO flight.telemetry_hourly_by_airline
AS SELECT
    coalesce(substring(callsign, 1, 3), '') AS callsign_prefix,
    toStartOfHour(observed_at) AS hour,
    uniqExactState(icao24) AS uniq_aircraft,
    uniqExactState(coalesce(callsign, '')) AS uniq_callsigns,
    toFloat64(coalesce(sum(if(NOT on_ground, velocity_ms, NULL)), 0)) AS sum_velocity_ms,
    toUInt64(count(if(NOT on_ground, velocity_ms, NULL))) AS count_velocity_obs,
    toUInt64(countIf(vertical_rate_ms > 2.5)) AS climbing_count,
    toUInt64(countIf(vertical_rate_ms < -2.5)) AS descending_count,
    toUInt64(countIf(vertical_rate_ms BETWEEN -2.5 AND 2.5)) AS cruising_count,
    toUInt64(countIf(on_ground)) AS on_ground_count
FROM flight.telemetry
GROUP BY callsign_prefix, hour;

CREATE VIEW IF NOT EXISTS flight.telemetry_hourly_by_airline_final AS
    SELECT
        callsign_prefix,
        hour,
        uniqExactMerge(uniq_aircraft)  AS active_aircraft,
        uniqExactMerge(uniq_callsigns) AS distinct_callsigns,
        CASE WHEN sum(count_velocity_obs) > 0
             THEN sum(sum_velocity_ms) / sum(count_velocity_obs)
             ELSE NULL END AS avg_velocity_ms,
        sum(climbing_count)   AS climbing_count,
        sum(descending_count) AS descending_count,
        sum(cruising_count)   AS cruising_count,
        sum(on_ground_count)  AS on_ground_count
    FROM flight.telemetry_hourly_by_airline
    GROUP BY callsign_prefix, hour;

-- ─── Rollup 3: airspace density grid (5-minute, 0.25° cells) ────────────────
-- 0.25° latitude ≈ 28 km; longitude varies with latitude but at mid-latitudes
-- the cell is roughly 18 km wide. Wide enough for a continental world map,
-- narrow enough to see corridors. Bucket size 5 min keeps the grid live
-- without exploding cardinality.

CREATE TABLE IF NOT EXISTS flight.airspace_grid_5min (
    bucket_5min   DateTime('UTC'),
    lat_cell      Int16,
    lon_cell      Int16,
    uniq_aircraft AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(bucket_5min)
ORDER BY (bucket_5min, lat_cell, lon_cell)
TTL bucket_5min + INTERVAL 30 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS flight.airspace_grid_5min_mv
TO flight.airspace_grid_5min
AS SELECT
    toStartOfFiveMinute(observed_at) AS bucket_5min,
    -- Cell index = floor(coord * 4); lat range [-360, 360], lon [-720, 720]
    -- both fit cleanly in Int16. NOT on_ground filter — heatmap is
    -- airborne aircraft only.
    toInt16(floor(lat * 4)) AS lat_cell,
    toInt16(floor(lon * 4)) AS lon_cell,
    uniqExactState(icao24)  AS uniq_aircraft
FROM flight.telemetry
WHERE NOT on_ground
GROUP BY bucket_5min, lat_cell, lon_cell;

CREATE VIEW IF NOT EXISTS flight.airspace_grid_5min_final AS
    SELECT
        bucket_5min,
        lat_cell,
        lon_cell,
        -- Convert cell index back to coordinates of cell origin so the
        -- worldmap panel can render directly.
        lat_cell / 4.0 AS lat,
        lon_cell / 4.0 AS lon,
        uniqExactMerge(uniq_aircraft) AS aircraft_count
    FROM flight.airspace_grid_5min
    GROUP BY bucket_5min, lat_cell, lon_cell;

-- Idempotent TTL backfill: CREATE TABLE IF NOT EXISTS is a no-op on
-- existing tables, so a cluster created before the TTL was added wouldn't
-- pick up the new retention policy. This ALTER reconciles both paths —
-- fresh installs get TTL from CREATE, old installs get it from here.
ALTER TABLE flight.telemetry MODIFY TTL toDateTime(observed_at) + INTERVAL 180 DAY DELETE;
