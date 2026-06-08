-- Drop old objects
DROP VIEW IF EXISTS otel_logs_archive;
DROP VIEW IF EXISTS hyperdx_sessions_archive;
DROP VIEW IF EXISTS otel_traces_archive;
DROP VIEW IF EXISTS otel_metrics_gauge_archive;
DROP VIEW IF EXISTS otel_metrics_sum_archive;
DROP VIEW IF EXISTS otel_metrics_histogram_archive;
DROP VIEW IF EXISTS otel_metrics_exp_histogram_archive;
DROP VIEW IF EXISTS otel_metrics_summary_archive;
DROP VIEW IF EXISTS otel_logs_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS hyperdx_sessions_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_traces_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_gauge_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_sum_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_histogram_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_exp_histogram_mv ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_summary_mv ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_logs ON CLUSTER 'default';
DROP TABLE IF EXISTS hyperdx_sessions ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_traces ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_traces_trace_id_ts ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_traces_trace_id_ts;
DROP TABLE IF EXISTS otel_metrics_gauge ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_sum ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_histogram ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_exp_histogram ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_summary ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_logs_queue ON CLUSTER 'default';
DROP TABLE IF EXISTS hyperdx_sessions_queue ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_traces_queue ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_queue ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_logs_local ON CLUSTER 'default';
DROP TABLE IF EXISTS hyperdx_sessions_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_traces_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_gauge_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_sum_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_histogram_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_exp_histogram_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_summary_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_logs_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_traces_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_gauge_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_sum_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_histogram_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_exp_histogram_local ON CLUSTER 'default';
DROP TABLE IF EXISTS otel_metrics_summary_local ON CLUSTER 'default';
DROP VIEW IF EXISTS otel_metrics_summary;
DROP VIEW IF EXISTS otel_metrics_exp_histogram;
DROP VIEW IF EXISTS otel_metrics_histogram;
DROP VIEW IF EXISTS otel_metrics_sum;
DROP VIEW IF EXISTS otel_metrics_gauge;
DROP VIEW IF EXISTS hyperdx_sessions;
DROP VIEW IF EXISTS otel_traces_hybrid_trace_id_ts;
DROP VIEW IF EXISTS otel_traces_trace_id_ts;
DROP VIEW IF EXISTS otel_traces_hybrid;
DROP VIEW IF EXISTS otel_traces;
DROP VIEW IF EXISTS otel_logs;

CREATE TABLE IF NOT EXISTS otel_logs_local ON CLUSTER 'default'
(
    Timestamp           DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId             String CODEC(ZSTD(1)),
    SpanId              String CODEC(ZSTD(1)),
    TraceFlags          UInt32 CODEC(ZSTD(1)),
    SeverityText        LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber      Int32 CODEC(ZSTD(1)),
    ServiceName         LowCardinality(String) CODEC(ZSTD(1)),
    Body                String CODEC(ZSTD(1)),
    ResourceSchemaUrl   String CODEC(ZSTD(1)),
    ResourceAttributes  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl      String CODEC(ZSTD(1)),
    ScopeName           String CODEC(ZSTD(1)),
    ScopeVersion        String CODEC(ZSTD(1)),
    ScopeAttributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName           String CODEC(ZSTD(1)),
    TimestampTime       DateTime MATERIALIZED toDateTime(Timestamp)
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_traces_local ON CLUSTER 'default'
(
    Timestamp           DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId             String CODEC(ZSTD(1)),
    SpanId              String CODEC(ZSTD(1)),
    ParentSpanId        String CODEC(ZSTD(1)),
    TraceState          String CODEC(ZSTD(1)),
    SpanName            LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind            LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName         LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName           String CODEC(ZSTD(1)),
    ScopeVersion        String CODEC(ZSTD(1)),
    SpanAttributes      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration            UInt64 CODEC(ZSTD(1)),
    StatusCode          LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage       String CODEC(ZSTD(1)),
    `Events.Timestamp`  Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name`       Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Links.TraceId`     Array(String) CODEC(ZSTD(1)),
    `Links.SpanId`      Array(String) CODEC(ZSTD(1)),
    `Links.TraceState`  Array(String) CODEC(ZSTD(1)),
    `Links.Attributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_gauge_local ON CLUSTER 'default'
(
    ResourceAttributes               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl                String CODEC(ZSTD(1)),
    ScopeName                        String CODEC(ZSTD(1)),
    ScopeVersion                     String CODEC(ZSTD(1)),
    ScopeAttributes                  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount            UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl                   String CODEC(ZSTD(1)),
    ServiceName                      LowCardinality(String) CODEC(ZSTD(1)),
    MetricName                       LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription                String CODEC(ZSTD(1)),
    MetricUnit                       String CODEC(ZSTD(1)),
    Attributes                       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix                    DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix                         DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value                            Float64 CODEC(ZSTD(1)),
    Flags                            UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`             Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`                Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`               Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`              Array(String) CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_sum_local ON CLUSTER 'default'
(
    ResourceAttributes               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl                String CODEC(ZSTD(1)),
    ScopeName                        String CODEC(ZSTD(1)),
    ScopeVersion                     String CODEC(ZSTD(1)),
    ScopeAttributes                  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount            UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl                   String CODEC(ZSTD(1)),
    ServiceName                      LowCardinality(String) CODEC(ZSTD(1)),
    MetricName                       LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription                String CODEC(ZSTD(1)),
    MetricUnit                       String CODEC(ZSTD(1)),
    Attributes                       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix                    DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix                         DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value                            Float64 CODEC(ZSTD(1)),
    Flags                            UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`             Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`                Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`               Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`              Array(String) CODEC(ZSTD(1)),
    AggregationTemporality           Int32 CODEC(ZSTD(1)),
    IsMonotonic                      Bool CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_histogram_local ON CLUSTER 'default'
(
    ResourceAttributes               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl                String CODEC(ZSTD(1)),
    ScopeName                        String CODEC(ZSTD(1)),
    ScopeVersion                     String CODEC(ZSTD(1)),
    ScopeAttributes                  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount            UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl                   String CODEC(ZSTD(1)),
    ServiceName                      LowCardinality(String) CODEC(ZSTD(1)),
    MetricName                       LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription                String CODEC(ZSTD(1)),
    MetricUnit                       String CODEC(ZSTD(1)),
    Attributes                       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix                    DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix                         DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                            UInt64 CODEC(ZSTD(1)),
    Sum                              Float64 CODEC(ZSTD(1)),
    BucketCounts                     Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds                   Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`             Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`                Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`               Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`              Array(String) CODEC(ZSTD(1)),
    Flags                            UInt32 CODEC(ZSTD(1)),
    Min                              Float64 CODEC(ZSTD(1)),
    Max                              Float64 CODEC(ZSTD(1)),
    AggregationTemporality           Int32 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_exp_histogram_local ON CLUSTER 'default'
(
    ResourceAttributes               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl                String CODEC(ZSTD(1)),
    ScopeName                        String CODEC(ZSTD(1)),
    ScopeVersion                     String CODEC(ZSTD(1)),
    ScopeAttributes                  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount            UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl                   String CODEC(ZSTD(1)),
    ServiceName                      LowCardinality(String) CODEC(ZSTD(1)),
    MetricName                       LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription                String CODEC(ZSTD(1)),
    MetricUnit                       String CODEC(ZSTD(1)),
    Attributes                       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix                    DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix                         DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                            UInt64 CODEC(ZSTD(1)),
    Sum                              Float64 CODEC(ZSTD(1)),
    Scale                            Int32 CODEC(ZSTD(1)),
    ZeroCount                        UInt64 CODEC(ZSTD(1)),
    PositiveOffset                   Int32 CODEC(ZSTD(1)),
    PositiveBucketCounts             Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset                   Int32 CODEC(ZSTD(1)),
    NegativeBucketCounts             Array(UInt64) CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`             Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`                Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`               Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`              Array(String) CODEC(ZSTD(1)),
    Flags                            UInt32 CODEC(ZSTD(1)),
    Min                              Float64 CODEC(ZSTD(1)),
    Max                              Float64 CODEC(ZSTD(1)),
    AggregationTemporality           Int32 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_summary_local ON CLUSTER 'default'
(
    ResourceAttributes               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl                String CODEC(ZSTD(1)),
    ScopeName                        String CODEC(ZSTD(1)),
    ScopeVersion                     String CODEC(ZSTD(1)),
    ScopeAttributes                  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount            UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl                   String CODEC(ZSTD(1)),
    ServiceName                      LowCardinality(String) CODEC(ZSTD(1)),
    MetricName                       LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription                String CODEC(ZSTD(1)),
    MetricUnit                       String CODEC(ZSTD(1)),
    Attributes                       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix                    DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix                         DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count                            UInt64 CODEC(ZSTD(1)),
    Sum                              Float64 CODEC(ZSTD(1)),
    `ValueAtQuantiles.Quantile`      Array(Float64) CODEC(ZSTD(1)),
    `ValueAtQuantiles.Value`         Array(Float64) CODEC(ZSTD(1)),
    Flags                            UInt32 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp(TimeUnix))
TTL toDateTime(TimeUnix) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;


CREATE TABLE IF NOT EXISTS hyperdx_sessions_local ON CLUSTER 'default'
(
    Timestamp           DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId             String CODEC(ZSTD(1)),
    SpanId              String CODEC(ZSTD(1)),
    TraceFlags          UInt32 CODEC(ZSTD(1)),
    SeverityText        LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber      Int32 CODEC(ZSTD(1)),
    ServiceName         LowCardinality(String) CODEC(ZSTD(1)),
    Body                String CODEC(ZSTD(1)),
    ResourceSchemaUrl   String CODEC(ZSTD(1)),
    ResourceAttributes  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl      String CODEC(ZSTD(1)),
    ScopeName           String CODEC(ZSTD(1)),
    ScopeVersion        String CODEC(ZSTD(1)),
    ScopeAttributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName           String CODEC(ZSTD(1)),
    TimestampTime       DateTime MATERIALIZED toDateTime(Timestamp)
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered', index_granularity = 8192, ttl_only_drop_parts = 1;

-- Distributed tables (INSERT 라우터 + 전체 shard 조회)
CREATE TABLE IF NOT EXISTS otel_logs ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_logs_local, rand());

CREATE TABLE IF NOT EXISTS otel_traces ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_traces_local, rand());

CREATE TABLE IF NOT EXISTS otel_metrics_gauge ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_metrics_gauge_local, rand());

CREATE TABLE IF NOT EXISTS otel_metrics_sum ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_metrics_sum_local, rand());

CREATE TABLE IF NOT EXISTS otel_metrics_histogram ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_metrics_histogram_local, rand());

CREATE TABLE IF NOT EXISTS otel_metrics_exp_histogram ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_metrics_exp_histogram_local, rand());

CREATE TABLE IF NOT EXISTS otel_metrics_summary ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), otel_metrics_summary_local, rand());

CREATE TABLE IF NOT EXISTS hyperdx_sessions ON CLUSTER 'default'
ENGINE = Distributed('default', currentDatabase(), hyperdx_sessions_local, rand());

CREATE VIEW IF NOT EXISTS otel_traces_trace_id_ts AS
SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End
FROM otel_traces WHERE TraceId != '' GROUP BY TraceId;

-- ============================================================
-- Verification queries: TTL MOVE 동작 확인
-- ============================================================
-- 1. 현재 파트별 디스크 위치 확인 (hot=default, cold=s3_cold)
-- SELECT table, partition, name, disk_name, rows, bytes_on_disk
-- FROM system.parts
-- WHERE database = currentDatabase()
--   AND table IN ('otel_logs', 'otel_traces', 'otel_metrics_gauge')
--   AND active = 1
-- ORDER BY table, disk_name, partition;

-- 2. 디스크별 데이터 크기 요약
-- SELECT disk_name, table, sum(bytes_on_disk) AS total_bytes, sum(rows) AS total_rows
-- FROM system.parts
-- WHERE database = currentDatabase() AND active = 1
-- GROUP BY disk_name, table
-- ORDER BY disk_name, table;

-- 3. cold 디스크에 데이터가 있는지 확인 (10분 후 실행)
-- SELECT count() FROM system.parts
-- WHERE database = currentDatabase()
--   AND disk_name = 's3_cold'
--   AND active = 1;

-- 4. TTL 설정 확인
-- SELECT name, engine_full
-- FROM system.tables
-- WHERE database = currentDatabase()
--   AND name IN ('otel_logs', 'otel_traces', 'otel_metrics_gauge');

-- 5. 실제 데이터가 cold에서도 조회되는지 확인 (10분 후)
-- SELECT count(), min(Timestamp), max(Timestamp)
-- FROM otel_logs;
-- ============================================================
