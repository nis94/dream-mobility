-- ClickHouse schema for the Dream Mobility pipeline.
--
-- raw_events uses ReplacingMergeTree(event_ts) so duplicate event_ids
-- (same ORDER BY key) are eventually collapsed at merge time. Queries
-- MUST use FINAL or argMax to get correct results before compaction.
--
-- events_hourly_mv is a real-time materialized view that rolls up event
-- counts and average speed per entity per hour using SummingMergeTree.

CREATE DATABASE IF NOT EXISTS mobility;

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
    attributes  String DEFAULT '',
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (entity_type, entity_id, event_id);

-- Hourly rollup: event count + average speed per entity per hour.
-- SummingMergeTree merges rows with the same ORDER BY key by summing
-- numeric columns. For avg_speed we track sum + count separately and
-- compute the average at query time: sum_speed / speed_count.
CREATE TABLE IF NOT EXISTS mobility.events_hourly (
    entity_type LowCardinality(String),
    entity_id   String,
    hour        DateTime('UTC'),
    event_count UInt64,
    sum_speed   Float64,
    speed_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (entity_type, entity_id, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS mobility.events_hourly_mv
TO mobility.events_hourly
AS SELECT
    entity_type,
    entity_id,
    toStartOfHour(event_ts) AS hour,
    count()                  AS event_count,
    sumIf(speed_kmh, speed_kmh IS NOT NULL) AS sum_speed,
    countIf(speed_kmh IS NOT NULL)          AS speed_count
FROM mobility.raw_events
GROUP BY entity_type, entity_id, hour;
