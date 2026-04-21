-- ClickHouse schema for the Dream Mobility pipeline.
--
-- raw_events uses ReplacingMergeTree(event_ts) so duplicate event_ids
-- (same ORDER BY tuple) are eventually collapsed at merge time. Queries
-- that need zero-duplicate reads before compaction MUST use FINAL — the
-- mobility.raw_events_final view below is the canonical read target.
--
-- events_hourly_mv is a materialized view backed by AggregatingMergeTree.
-- It uses uniqExactState(event_id) rather than count() so at-least-once
-- redelivery of the same event_id does NOT inflate the hourly count.
-- avg speed is stored as sum + count (via SimpleAggregateFunction) so
-- merges remain associative and the read-time average is exact:
--     sum_speed / speed_count
-- Duplicate redelivery can still inflate sum_speed / speed_count
-- individually, but their ratio — the displayed avg — remains correct.
--
-- The CREATE DATABASE step is done separately in internal/chsink/sink.go
-- before the embedded schema runs, so CREATE DATABASE is NOT in this file.

CREATE TABLE IF NOT EXISTS mobility.raw_events (
    event_id    String,
    entity_type LowCardinality(String),
    entity_id   String,
    event_ts    DateTime64(6, 'UTC'),
    lat         Float64,
    lon         Float64,
    speed_kmh   Nullable(Float64),
    heading_deg Nullable(Float64),
    accuracy_m  Nullable(Float64),
    source      LowCardinality(Nullable(String)),
    attributes  String DEFAULT '' CODEC(ZSTD(3)),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (entity_type, entity_id, event_ts, event_id);

-- Canonical read view: always reads post-merge so duplicates are collapsed.
CREATE VIEW IF NOT EXISTS mobility.raw_events_final AS
    SELECT * FROM mobility.raw_events FINAL;

-- Hourly rollup backed by AggregatingMergeTree. See top-of-file comment
-- on why uniqExactState / SimpleAggregateFunction over count()/Summing —
-- the Phase 5 audit showed SummingMergeTree double-counts at-least-once
-- redeliveries because the MV runs per-INSERT-block and has already
-- written an extra row by the time ReplacingMergeTree collapses the raw.
CREATE TABLE IF NOT EXISTS mobility.events_hourly (
    entity_type LowCardinality(String),
    entity_id   String,
    hour        DateTime('UTC'),
    uniq_events AggregateFunction(uniqExact, String),
    sum_speed   SimpleAggregateFunction(sum, Float64),
    speed_count SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (entity_type, entity_id, hour)
TTL hour + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mobility.events_hourly_mv
TO mobility.events_hourly
AS SELECT
    entity_type,
    entity_id,
    toStartOfHour(event_ts) AS hour,
    uniqExactState(event_id) AS uniq_events,
    sumIf(speed_kmh, speed_kmh IS NOT NULL) AS sum_speed,
    countIf(speed_kmh IS NOT NULL) AS speed_count
FROM mobility.raw_events
GROUP BY entity_type, entity_id, hour;

-- Read-facing view: merges the aggregate state and exposes event_count
-- + avg_speed as scalar columns.
CREATE VIEW IF NOT EXISTS mobility.events_hourly_final AS
    SELECT
        entity_type,
        entity_id,
        hour,
        uniqExactMerge(uniq_events) AS event_count,
        sum_speed,
        speed_count,
        CASE WHEN speed_count > 0 THEN sum_speed / speed_count ELSE NULL END AS avg_speed_kmh
    FROM mobility.events_hourly
    GROUP BY entity_type, entity_id, hour, sum_speed, speed_count;
