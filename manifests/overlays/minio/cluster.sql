DROP VIEW IF EXISTS otel_metrics_summary;
DROP VIEW IF EXISTS otel_metrics_summary_s3;
DROP VIEW IF EXISTS otel_metrics_exp_histogram;
DROP VIEW IF EXISTS otel_metrics_exp_histogram_s3;
DROP VIEW IF EXISTS otel_metrics_histogram;
DROP VIEW IF EXISTS otel_metrics_histogram_s3;
DROP VIEW IF EXISTS otel_metrics_sum;
DROP VIEW IF EXISTS otel_metrics_sum_s3;
DROP VIEW IF EXISTS otel_metrics_gauge;
DROP VIEW IF EXISTS otel_metrics_gauge_s3;
DROP VIEW IF EXISTS hyperdx_sessions;
DROP VIEW IF EXISTS hyperdx_sessions_s3;
DROP VIEW IF EXISTS otel_traces_trace_id_ts;
DROP VIEW IF EXISTS otel_traces;
DROP VIEW IF EXISTS otel_traces_s3;
DROP VIEW IF EXISTS otel_logs;
DROP VIEW IF EXISTS otel_logs_s3;
DROP VIEW IF EXISTS otel_traces_grafana;

DROP TABLE IF EXISTS otel_metrics_summary_local_dist;
DROP TABLE IF EXISTS otel_metrics_exp_histogram_local_dist;
DROP TABLE IF EXISTS otel_metrics_histogram_local_dist;
DROP TABLE IF EXISTS otel_metrics_sum_local_dist;
DROP TABLE IF EXISTS otel_metrics_gauge_local_dist;
DROP TABLE IF EXISTS hyperdx_sessions_local_dist;
DROP TABLE IF EXISTS otel_traces_local_dist;
DROP TABLE IF EXISTS otel_logs_local_dist;

DROP TABLE IF EXISTS otel_metrics_summary_local;
DROP TABLE IF EXISTS otel_metrics_exp_histogram_local;
DROP TABLE IF EXISTS otel_metrics_histogram_local;
DROP TABLE IF EXISTS otel_metrics_sum_local;
DROP TABLE IF EXISTS otel_metrics_gauge_local;
DROP TABLE IF EXISTS hyperdx_sessions_local;
DROP TABLE IF EXISTS otel_traces_local;
DROP TABLE IF EXISTS otel_logs_local;

DROP TABLE IF EXISTS otel_metrics_summary;
DROP TABLE IF EXISTS otel_metrics_exp_histogram;
DROP TABLE IF EXISTS otel_metrics_histogram;
DROP TABLE IF EXISTS otel_metrics_sum;
DROP TABLE IF EXISTS otel_metrics_gauge;
DROP TABLE IF EXISTS otel_metrics;
DROP TABLE IF EXISTS otel_metrics_queue;
DROP TABLE IF EXISTS hyperdx_sessions;
DROP TABLE IF EXISTS hyperdx_sessions_queue;
DROP TABLE IF EXISTS otel_traces_trace_id_ts;
DROP TABLE IF EXISTS otel_traces;
DROP TABLE IF EXISTS otel_traces_queue;
DROP TABLE IF EXISTS otel_logs;
DROP TABLE IF EXISTS otel_logs_queue;

CREATE OR REPLACE FUNCTION otel_value AS (value) ->
    multiIf(
        JSONHas(value, 'stringValue'), JSONExtractString(value, 'stringValue'),
        JSONHas(value, 'boolValue'), if(JSONExtractBool(value, 'boolValue'), 'true', 'false'),
        JSONHas(value, 'intValue'), JSONExtractString(value, 'intValue'),
        JSONHas(value, 'doubleValue'), toString(JSONExtractFloat(value, 'doubleValue')),
        JSONHas(value, 'bytesValue'), JSONExtractString(value, 'bytesValue'),
        JSONHas(value, 'arrayValue'), ifNull(JSONExtractRaw(value, 'arrayValue'), '[]'),
        JSONHas(value, 'kvlistValue'), ifNull(JSONExtractRaw(value, 'kvlistValue'), '{}'),
        JSONHas(value, 'nullValue'), 'null',
        ''
    );

CREATE OR REPLACE FUNCTION otel_attributes AS (attributes) ->
    mapFromArrays(
        arrayMap(attribute -> JSONExtractString(attribute, 'key'), attributes),
        arrayMap(attribute -> otel_value(JSONExtractRaw(attribute, 'value')), attributes)
    );

CREATE OR REPLACE FUNCTION otel_uint_array AS (items) ->
    arrayMap(item -> toUInt64OrZero(replaceAll(item, '"', '')), items);

CREATE OR REPLACE FUNCTION otel_float_array AS (items) ->
    arrayMap(item -> toFloat64OrZero(replaceAll(item, '"', '')), items);

CREATE OR REPLACE FUNCTION otel_span_kind AS (kind) ->
    multiIf(
        kind = 5, 'SPAN_KIND_CONSUMER',
        kind = 4, 'SPAN_KIND_PRODUCER',
        kind = 3, 'SPAN_KIND_CLIENT',
        kind = 2, 'SPAN_KIND_SERVER',
        kind = 1, 'SPAN_KIND_INTERNAL',
        'SPAN_KIND_UNSPECIFIED'
    );

CREATE OR REPLACE FUNCTION otel_status_code AS (code) ->
    multiIf(
        code = 2, 'STATUS_CODE_ERROR',
        code = 1, 'STATUS_CODE_OK',
        'STATUS_CODE_UNSET'
    );

CREATE OR REPLACE FUNCTION otel_aggregation_temporality AS (code) ->
    multiIf(
        code IN (1, 2), toInt32(code),
        toInt32(0)
    );

CREATE TABLE IF NOT EXISTS otel_logs_local
(
    Timestamp DateTime64(9),
    TimestampTime DateTime MATERIALIZED toDateTime(Timestamp),
    TraceId String,
    SpanId String,
    TraceFlags UInt8,
    SeverityText LowCardinality(String),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String),
    Body String,
    ResourceSchemaUrl String,
    ResourceAttributes Map(String, String),
    ScopeSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    LogAttributes Map(String, String),
    EventName String,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
PRIMARY KEY (toStartOfFiveMinutes(Timestamp), ServiceName)
ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, SeverityText, Timestamp)
TTL Timestamp + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS hyperdx_sessions_local
(
    Timestamp DateTime64(9),
    TimestampTime DateTime DEFAULT toDateTime(Timestamp),
    TraceId String,
    SpanId String,
    TraceFlags UInt8,
    SeverityText LowCardinality(String),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String),
    Body String,
    ResourceSchemaUrl String,
    ResourceAttributes Map(String, String),
    ScopeSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    LogAttributes Map(String, String),
    `__hdx_materialized_rum.sessionId` String MATERIALIZED ResourceAttributes['rum.sessionId'],
    `__hdx_materialized_type` LowCardinality(String) MATERIALIZED toString(simpleJSONExtractInt(Body, 'type')),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_rum_session_id `__hdx_materialized_rum.sessionId` TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree()
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
TTL Timestamp + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_traces_local
(
    Timestamp DateTime64(9),
    TraceId String,
    SpanId String,
    ParentSpanId String,
    TraceState String,
    SpanName LowCardinality(String),
    SpanKind LowCardinality(String),
    ServiceName LowCardinality(String),
    ResourceAttributes Map(String, String),
    ScopeName String,
    ScopeVersion String,
    SpanAttributes Map(String, String),
    Duration UInt64,
    StatusCode LowCardinality(String),
    StatusMessage String,
    `Events.Timestamp` Array(DateTime64(9)),
    `Events.Name` Array(String),
    `Events.Attributes` Array(Map(String, String)),
    `Links.TraceId` Array(String),
    `Links.SpanId` Array(String),
    `Links.TraceState` Array(String),
    `Links.Attributes` Array(Map(String, String)),
    `__hdx_materialized_rum.sessionId` String MATERIALIZED ResourceAttributes['rum.sessionId'],
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_rum_session_id `__hdx_materialized_rum.sessionId` TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp), Timestamp)
TTL Timestamp + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_gauge_local
(
    ResourceAttributes Map(String, String),
    ResourceSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl String,
    ServiceName LowCardinality(String),
    MetricName String,
    MetricDescription String,
    MetricUnit String,
    Attributes Map(String, String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9),
    Value Float64,
    Flags UInt32,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
TTL TimeUnix + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_sum_local
(
    ResourceAttributes Map(String, String),
    ResourceSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl String,
    ServiceName LowCardinality(String),
    MetricName String,
    MetricDescription String,
    MetricUnit String,
    Attributes Map(String, String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9),
    Value Float64,
    Flags UInt32,
    AggregationTemporality Int32,
    IsMonotonic Bool,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
TTL TimeUnix + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_histogram_local
(
    ResourceAttributes Map(String, String),
    ResourceSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl String,
    ServiceName LowCardinality(String),
    MetricName String,
    MetricDescription String,
    MetricUnit String,
    Attributes Map(String, String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9),
    Count UInt64,
    Sum Float64,
    BucketCounts Array(UInt64),
    ExplicitBounds Array(Float64),
    Flags UInt32,
    Min Nullable(Float64),
    Max Nullable(Float64),
    AggregationTemporality Int32,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
TTL TimeUnix + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_exp_histogram_local
(
    ResourceAttributes Map(String, String),
    ResourceSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl String,
    ServiceName LowCardinality(String),
    MetricName String,
    MetricDescription String,
    MetricUnit String,
    Attributes Map(String, String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9),
    Count UInt64,
    Sum Float64,
    Scale Int32,
    ZeroCount UInt64,
    PositiveOffset Int32,
    PositiveBucketCounts Array(UInt64),
    NegativeOffset Int32,
    NegativeBucketCounts Array(UInt64),
    Flags UInt32,
    Min Nullable(Float64),
    Max Nullable(Float64),
    AggregationTemporality Int32,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
TTL TimeUnix + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_summary_local
(
    ResourceAttributes Map(String, String),
    ResourceSchemaUrl String,
    ScopeName String,
    ScopeVersion String,
    ScopeAttributes Map(String, String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl String,
    ServiceName LowCardinality(String),
    MetricName String,
    MetricDescription String,
    MetricUnit String,
    Attributes Map(String, String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9),
    Count UInt64,
    Sum Float64,
    `ValueAtQuantiles.Quantile` Array(Float64),
    `ValueAtQuantiles.Value` Array(Float64),
    Flags UInt32,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
TTL TimeUnix + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_logs_local_dist AS otel_logs_local
ENGINE = Distributed('default', 'default', 'otel_logs_local', cityHash64(TraceId, SpanId, ServiceName));

CREATE TABLE IF NOT EXISTS hyperdx_sessions_local_dist AS hyperdx_sessions_local
ENGINE = Distributed('default', 'default', 'hyperdx_sessions_local', cityHash64(TraceId, SpanId, ServiceName));

CREATE TABLE IF NOT EXISTS otel_traces_local_dist AS otel_traces_local
ENGINE = Distributed('default', 'default', 'otel_traces_local', cityHash64(TraceId, SpanId, ServiceName));

CREATE TABLE IF NOT EXISTS otel_metrics_gauge_local_dist AS otel_metrics_gauge_local
ENGINE = Distributed('default', 'default', 'otel_metrics_gauge_local', cityHash64(MetricName, ServiceName));

CREATE TABLE IF NOT EXISTS otel_metrics_sum_local_dist AS otel_metrics_sum_local
ENGINE = Distributed('default', 'default', 'otel_metrics_sum_local', cityHash64(MetricName, ServiceName));

CREATE TABLE IF NOT EXISTS otel_metrics_histogram_local_dist AS otel_metrics_histogram_local
ENGINE = Distributed('default', 'default', 'otel_metrics_histogram_local', cityHash64(MetricName, ServiceName));

CREATE TABLE IF NOT EXISTS otel_metrics_exp_histogram_local_dist AS otel_metrics_exp_histogram_local
ENGINE = Distributed('default', 'default', 'otel_metrics_exp_histogram_local', cityHash64(MetricName, ServiceName));

CREATE TABLE IF NOT EXISTS otel_metrics_summary_local_dist AS otel_metrics_summary_local
ENGINE = Distributed('default', 'default', 'otel_metrics_summary_local', cityHash64(MetricName, ServiceName));

CREATE VIEW IF NOT EXISTS otel_logs_s3 AS
SELECT
    Timestamp,
    toDateTime(Timestamp) AS TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ResourceAttributes['service.name'] AS ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    EventName
FROM (
    SELECT
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'timeUnixNano'))) AS Timestamp,
        JSONExtractString(lr, 'traceId') AS TraceId,
        JSONExtractString(lr, 'spanId') AS SpanId,
        toUInt8(JSONExtractUInt(lr, 'flags')) AS TraceFlags,
        JSONExtractString(lr, 'severityText') AS SeverityText,
        toUInt8(JSONExtractUInt(lr, 'severityNumber')) AS SeverityNumber,
        otel_value(JSONExtractRaw(lr, 'body')) AS Body,
        JSONExtractString(JSONExtractRaw(rl, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(sl, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(JSONExtractRaw(sl, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sl, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sl, 'scope'), 'attributes')) AS ScopeAttributes,
        otel_attributes(JSONExtractArrayRaw(lr, 'attributes')) AS LogAttributes,
        JSONExtractString(lr, 'eventName') AS EventName
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceLogs') AS resource_logs
        FROM s3(
            'http://minio-service:9000/otel-logs/logs/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_logs AS rl
    ARRAY JOIN JSONExtractArrayRaw(rl, 'scopeLogs') AS sl
    ARRAY JOIN JSONExtractArrayRaw(sl, 'logRecords') AS lr
);

CREATE VIEW IF NOT EXISTS hyperdx_sessions_s3 AS
SELECT
    Timestamp,
    toDateTime(Timestamp) AS TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ResourceAttributes['service.name'] AS ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    ResourceAttributes['rum.sessionId'] AS `__hdx_materialized_rum.sessionId`,
    toString(simpleJSONExtractInt(Body, 'type')) AS `__hdx_materialized_type`
FROM (
    SELECT
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'timeUnixNano'))) AS Timestamp,
        JSONExtractString(lr, 'traceId') AS TraceId,
        JSONExtractString(lr, 'spanId') AS SpanId,
        toUInt8(JSONExtractUInt(lr, 'flags')) AS TraceFlags,
        JSONExtractString(lr, 'severityText') AS SeverityText,
        toUInt8(JSONExtractUInt(lr, 'severityNumber')) AS SeverityNumber,
        otel_value(JSONExtractRaw(lr, 'body')) AS Body,
        JSONExtractString(JSONExtractRaw(rl, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(sl, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(JSONExtractRaw(sl, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sl, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sl, 'scope'), 'attributes')) AS ScopeAttributes,
        otel_attributes(JSONExtractArrayRaw(lr, 'attributes')) AS LogAttributes
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceLogs') AS resource_logs
        FROM s3(
            'http://minio-service:9000/hyperdx-sessions/sessions/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_logs AS rl
    ARRAY JOIN JSONExtractArrayRaw(rl, 'scopeLogs') AS sl
    ARRAY JOIN JSONExtractArrayRaw(sl, 'logRecords') AS lr
);

CREATE VIEW IF NOT EXISTS otel_traces_s3 AS
SELECT
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ResourceAttributes['service.name'] AS ServiceName,
    ResourceAttributes,
    ScopeName,
    ScopeVersion,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage,
    `Events.Timestamp`,
    `Events.Name`,
    `Events.Attributes`,
    `Links.TraceId`,
    `Links.SpanId`,
    `Links.TraceState`,
    `Links.Attributes`,
    ResourceAttributes['rum.sessionId'] AS `__hdx_materialized_rum.sessionId`
FROM (
    SELECT
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(sp, 'startTimeUnixNano'))) AS Timestamp,
        JSONExtractString(sp, 'traceId') AS TraceId,
        JSONExtractString(sp, 'spanId') AS SpanId,
        JSONExtractString(sp, 'parentSpanId') AS ParentSpanId,
        JSONExtractString(sp, 'traceState') AS TraceState,
        JSONExtractString(sp, 'name') AS SpanName,
        otel_span_kind(toUInt8(JSONExtractUInt(sp, 'kind'))) AS SpanKind,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rs, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(ss, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(ss, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(sp, 'attributes')) AS SpanAttributes,
        toUInt64(greatest(
            toInt64OrZero(JSONExtractString(sp, 'endTimeUnixNano')) - toInt64OrZero(JSONExtractString(sp, 'startTimeUnixNano')),
            0
        )) AS Duration,
        otel_status_code(toUInt8(JSONExtractUInt(JSONExtractRaw(sp, 'status'), 'code'))) AS StatusCode,
        JSONExtractString(JSONExtractRaw(sp, 'status'), 'message') AS StatusMessage,
        arrayMap(
            event -> fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(event, 'timeUnixNano'))),
            JSONExtractArrayRaw(if(JSONHas(sp, 'events'), JSONExtractRaw(sp, 'events'), '[]'))
        ) AS `Events.Timestamp`,
        arrayMap(
            event -> JSONExtractString(event, 'name'),
            JSONExtractArrayRaw(if(JSONHas(sp, 'events'), JSONExtractRaw(sp, 'events'), '[]'))
        ) AS `Events.Name`,
        arrayMap(
            event -> otel_attributes(JSONExtractArrayRaw(event, 'attributes')),
            JSONExtractArrayRaw(if(JSONHas(sp, 'events'), JSONExtractRaw(sp, 'events'), '[]'))
        ) AS `Events.Attributes`,
        arrayMap(
            link -> JSONExtractString(link, 'traceId'),
            JSONExtractArrayRaw(if(JSONHas(sp, 'links'), JSONExtractRaw(sp, 'links'), '[]'))
        ) AS `Links.TraceId`,
        arrayMap(
            link -> JSONExtractString(link, 'spanId'),
            JSONExtractArrayRaw(if(JSONHas(sp, 'links'), JSONExtractRaw(sp, 'links'), '[]'))
        ) AS `Links.SpanId`,
        arrayMap(
            link -> JSONExtractString(link, 'traceState'),
            JSONExtractArrayRaw(if(JSONHas(sp, 'links'), JSONExtractRaw(sp, 'links'), '[]'))
        ) AS `Links.TraceState`,
        arrayMap(
            link -> otel_attributes(JSONExtractArrayRaw(link, 'attributes')),
            JSONExtractArrayRaw(if(JSONHas(sp, 'links'), JSONExtractRaw(sp, 'links'), '[]'))
        ) AS `Links.Attributes`
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceSpans') AS resource_spans
        FROM s3(
            'http://minio-service:9000/otel-traces/traces/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_spans AS rs
    ARRAY JOIN JSONExtractArrayRaw(rs, 'scopeSpans') AS ss
    ARRAY JOIN JSONExtractArrayRaw(ss, 'spans') AS sp
);

CREATE VIEW IF NOT EXISTS otel_metrics_gauge_s3 AS
SELECT
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ResourceAttributes['service.name'] AS ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Value,
    Flags
FROM (
    SELECT
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rm, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(rm, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sm, 'scope'), 'attributes')) AS ScopeAttributes,
        toUInt32(JSONExtractUInt(JSONExtractRaw(sm, 'scope'), 'droppedAttributesCount')) AS ScopeDroppedAttrCount,
        JSONExtractString(sm, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(metric, 'name') AS MetricName,
        JSONExtractString(metric, 'description') AS MetricDescription,
        JSONExtractString(metric, 'unit') AS MetricUnit,
        otel_attributes(JSONExtractArrayRaw(dp, 'attributes')) AS Attributes,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'startTimeUnixNano'))) AS StartTimeUnix,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'timeUnixNano'))) AS TimeUnix,
        if(JSONHas(dp, 'asDouble'), JSONExtractFloat(dp, 'asDouble'), toFloat64OrZero(JSONExtractString(dp, 'asInt'))) AS Value,
        toUInt32(JSONExtractUInt(dp, 'flags')) AS Flags
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceMetrics') AS resource_metrics
        FROM s3(
            'http://minio-service:9000/otel-metrics/metrics/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_metrics AS rm
    ARRAY JOIN JSONExtractArrayRaw(rm, 'scopeMetrics') AS sm
    ARRAY JOIN JSONExtractArrayRaw(sm, 'metrics') AS metric
    ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(metric, 'gauge'), 'dataPoints') AS dp
    WHERE JSONHas(metric, 'gauge')
);

CREATE VIEW IF NOT EXISTS otel_metrics_sum_s3 AS
SELECT
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ResourceAttributes['service.name'] AS ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Value,
    Flags,
    AggregationTemporality,
    IsMonotonic
FROM (
    SELECT
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rm, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(rm, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sm, 'scope'), 'attributes')) AS ScopeAttributes,
        toUInt32(JSONExtractUInt(JSONExtractRaw(sm, 'scope'), 'droppedAttributesCount')) AS ScopeDroppedAttrCount,
        JSONExtractString(sm, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(metric, 'name') AS MetricName,
        JSONExtractString(metric, 'description') AS MetricDescription,
        JSONExtractString(metric, 'unit') AS MetricUnit,
        otel_attributes(JSONExtractArrayRaw(dp, 'attributes')) AS Attributes,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'startTimeUnixNano'))) AS StartTimeUnix,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'timeUnixNano'))) AS TimeUnix,
        if(JSONHas(dp, 'asDouble'), JSONExtractFloat(dp, 'asDouble'), toFloat64OrZero(JSONExtractString(dp, 'asInt'))) AS Value,
        toUInt32(JSONExtractUInt(dp, 'flags')) AS Flags,
        otel_aggregation_temporality(toUInt8(JSONExtractUInt(JSONExtractRaw(metric, 'sum'), 'aggregationTemporality'))) AS AggregationTemporality,
        JSONExtractBool(JSONExtractRaw(metric, 'sum'), 'isMonotonic') AS IsMonotonic
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceMetrics') AS resource_metrics
        FROM s3(
            'http://minio-service:9000/otel-metrics/metrics/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_metrics AS rm
    ARRAY JOIN JSONExtractArrayRaw(rm, 'scopeMetrics') AS sm
    ARRAY JOIN JSONExtractArrayRaw(sm, 'metrics') AS metric
    ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(metric, 'sum'), 'dataPoints') AS dp
    WHERE JSONHas(metric, 'sum')
);

CREATE VIEW IF NOT EXISTS otel_metrics_histogram_s3 AS
SELECT
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ResourceAttributes['service.name'] AS ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    BucketCounts,
    ExplicitBounds,
    Flags,
    Min,
    Max,
    AggregationTemporality
FROM (
    SELECT
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rm, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(rm, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sm, 'scope'), 'attributes')) AS ScopeAttributes,
        toUInt32(JSONExtractUInt(JSONExtractRaw(sm, 'scope'), 'droppedAttributesCount')) AS ScopeDroppedAttrCount,
        JSONExtractString(sm, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(metric, 'name') AS MetricName,
        JSONExtractString(metric, 'description') AS MetricDescription,
        JSONExtractString(metric, 'unit') AS MetricUnit,
        otel_attributes(JSONExtractArrayRaw(dp, 'attributes')) AS Attributes,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'startTimeUnixNano'))) AS StartTimeUnix,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'timeUnixNano'))) AS TimeUnix,
        toUInt64OrZero(JSONExtractString(dp, 'count')) AS Count,
        JSONExtractFloat(dp, 'sum') AS Sum,
        otel_uint_array(JSONExtractArrayRaw(if(JSONHas(dp, 'bucketCounts'), JSONExtractRaw(dp, 'bucketCounts'), '[]'))) AS BucketCounts,
        otel_float_array(JSONExtractArrayRaw(if(JSONHas(dp, 'explicitBounds'), JSONExtractRaw(dp, 'explicitBounds'), '[]'))) AS ExplicitBounds,
        toUInt32(JSONExtractUInt(dp, 'flags')) AS Flags,
        if(JSONHas(dp, 'min'), CAST(JSONExtractFloat(dp, 'min'), 'Nullable(Float64)'), CAST(NULL, 'Nullable(Float64)')) AS Min,
        if(JSONHas(dp, 'max'), CAST(JSONExtractFloat(dp, 'max'), 'Nullable(Float64)'), CAST(NULL, 'Nullable(Float64)')) AS Max,
        otel_aggregation_temporality(toUInt8(JSONExtractUInt(JSONExtractRaw(metric, 'histogram'), 'aggregationTemporality'))) AS AggregationTemporality
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceMetrics') AS resource_metrics
        FROM s3(
            'http://minio-service:9000/otel-metrics/metrics/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_metrics AS rm
    ARRAY JOIN JSONExtractArrayRaw(rm, 'scopeMetrics') AS sm
    ARRAY JOIN JSONExtractArrayRaw(sm, 'metrics') AS metric
    ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(metric, 'histogram'), 'dataPoints') AS dp
    WHERE JSONHas(metric, 'histogram')
);

CREATE VIEW IF NOT EXISTS otel_metrics_exp_histogram_s3 AS
SELECT
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ResourceAttributes['service.name'] AS ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    Scale,
    ZeroCount,
    PositiveOffset,
    PositiveBucketCounts,
    NegativeOffset,
    NegativeBucketCounts,
    Flags,
    Min,
    Max,
    AggregationTemporality
FROM (
    SELECT
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rm, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(rm, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sm, 'scope'), 'attributes')) AS ScopeAttributes,
        toUInt32(JSONExtractUInt(JSONExtractRaw(sm, 'scope'), 'droppedAttributesCount')) AS ScopeDroppedAttrCount,
        JSONExtractString(sm, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(metric, 'name') AS MetricName,
        JSONExtractString(metric, 'description') AS MetricDescription,
        JSONExtractString(metric, 'unit') AS MetricUnit,
        otel_attributes(JSONExtractArrayRaw(dp, 'attributes')) AS Attributes,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'startTimeUnixNano'))) AS StartTimeUnix,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'timeUnixNano'))) AS TimeUnix,
        toUInt64OrZero(JSONExtractString(dp, 'count')) AS Count,
        JSONExtractFloat(dp, 'sum') AS Sum,
        toInt32(JSONExtractInt(dp, 'scale')) AS Scale,
        toUInt64OrZero(JSONExtractString(dp, 'zeroCount')) AS ZeroCount,
        toInt32(JSONExtractInt(if(JSONHas(dp, 'positive'), JSONExtractRaw(dp, 'positive'), '{}'), 'offset')) AS PositiveOffset,
        otel_uint_array(JSONExtractArrayRaw(if(JSONHas(if(JSONHas(dp, 'positive'), JSONExtractRaw(dp, 'positive'), '{}'), 'bucketCounts'), JSONExtractRaw(if(JSONHas(dp, 'positive'), JSONExtractRaw(dp, 'positive'), '{}'), 'bucketCounts'), '[]'))) AS PositiveBucketCounts,
        toInt32(JSONExtractInt(if(JSONHas(dp, 'negative'), JSONExtractRaw(dp, 'negative'), '{}'), 'offset')) AS NegativeOffset,
        otel_uint_array(JSONExtractArrayRaw(if(JSONHas(if(JSONHas(dp, 'negative'), JSONExtractRaw(dp, 'negative'), '{}'), 'bucketCounts'), JSONExtractRaw(if(JSONHas(dp, 'negative'), JSONExtractRaw(dp, 'negative'), '{}'), 'bucketCounts'), '[]'))) AS NegativeBucketCounts,
        toUInt32(JSONExtractUInt(dp, 'flags')) AS Flags,
        if(JSONHas(dp, 'min'), CAST(JSONExtractFloat(dp, 'min'), 'Nullable(Float64)'), CAST(NULL, 'Nullable(Float64)')) AS Min,
        if(JSONHas(dp, 'max'), CAST(JSONExtractFloat(dp, 'max'), 'Nullable(Float64)'), CAST(NULL, 'Nullable(Float64)')) AS Max,
        otel_aggregation_temporality(toUInt8(JSONExtractUInt(JSONExtractRaw(metric, 'exponentialHistogram'), 'aggregationTemporality'))) AS AggregationTemporality
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceMetrics') AS resource_metrics
        FROM s3(
            'http://minio-service:9000/otel-metrics/metrics/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_metrics AS rm
    ARRAY JOIN JSONExtractArrayRaw(rm, 'scopeMetrics') AS sm
    ARRAY JOIN JSONExtractArrayRaw(sm, 'metrics') AS metric
    ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(metric, 'exponentialHistogram'), 'dataPoints') AS dp
    WHERE JSONHas(metric, 'exponentialHistogram')
);

CREATE VIEW IF NOT EXISTS otel_metrics_summary_s3 AS
SELECT
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ResourceAttributes['service.name'] AS ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    `ValueAtQuantiles.Quantile`,
    `ValueAtQuantiles.Value`,
    Flags
FROM (
    SELECT
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(rm, 'resource'), 'attributes')) AS ResourceAttributes,
        JSONExtractString(JSONExtractRaw(rm, 'resource'), 'schemaUrl') AS ResourceSchemaUrl,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'name') AS ScopeName,
        JSONExtractString(JSONExtractRaw(sm, 'scope'), 'version') AS ScopeVersion,
        otel_attributes(JSONExtractArrayRaw(JSONExtractRaw(sm, 'scope'), 'attributes')) AS ScopeAttributes,
        toUInt32(JSONExtractUInt(JSONExtractRaw(sm, 'scope'), 'droppedAttributesCount')) AS ScopeDroppedAttrCount,
        JSONExtractString(sm, 'schemaUrl') AS ScopeSchemaUrl,
        JSONExtractString(metric, 'name') AS MetricName,
        JSONExtractString(metric, 'description') AS MetricDescription,
        JSONExtractString(metric, 'unit') AS MetricUnit,
        otel_attributes(JSONExtractArrayRaw(dp, 'attributes')) AS Attributes,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'startTimeUnixNano'))) AS StartTimeUnix,
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(dp, 'timeUnixNano'))) AS TimeUnix,
        toUInt64OrZero(JSONExtractString(dp, 'count')) AS Count,
        JSONExtractFloat(dp, 'sum') AS Sum,
        arrayMap(
            quantile_value -> JSONExtractFloat(quantile_value, 'quantile'),
            JSONExtractArrayRaw(if(JSONHas(dp, 'quantileValues'), JSONExtractRaw(dp, 'quantileValues'), '[]'))
        ) AS `ValueAtQuantiles.Quantile`,
        arrayMap(
            quantile_value -> JSONExtractFloat(quantile_value, 'value'),
            JSONExtractArrayRaw(if(JSONHas(dp, 'quantileValues'), JSONExtractRaw(dp, 'quantileValues'), '[]'))
        ) AS `ValueAtQuantiles.Value`,
        toUInt32(JSONExtractUInt(dp, 'flags')) AS Flags
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceMetrics') AS resource_metrics
        FROM s3(
            'http://minio-service:9000/otel-metrics/metrics/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin',
            'minioadmin',
            'JSONAsString',
            'json String',
            'none'
        )
    )
    ARRAY JOIN resource_metrics AS rm
    ARRAY JOIN JSONExtractArrayRaw(rm, 'scopeMetrics') AS sm
    ARRAY JOIN JSONExtractArrayRaw(sm, 'metrics') AS metric
    ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(metric, 'summary'), 'dataPoints') AS dp
    WHERE JSONHas(metric, 'summary')
);

CREATE VIEW IF NOT EXISTS otel_logs AS
SELECT
    Timestamp,
    TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    EventName
FROM otel_logs_local_dist
UNION ALL
SELECT
    Timestamp,
    TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    EventName
FROM otel_logs_s3
WHERE Timestamp < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS hyperdx_sessions AS
SELECT
    Timestamp,
    TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    `__hdx_materialized_rum.sessionId`,
    `__hdx_materialized_type`
FROM hyperdx_sessions_local_dist
UNION ALL
SELECT
    Timestamp,
    TimestampTime,
    TraceId,
    SpanId,
    TraceFlags,
    SeverityText,
    SeverityNumber,
    ServiceName,
    Body,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    LogAttributes,
    `__hdx_materialized_rum.sessionId`,
    `__hdx_materialized_type`
FROM hyperdx_sessions_s3
WHERE Timestamp < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_traces AS
SELECT
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ServiceName,
    ResourceAttributes,
    ScopeName,
    ScopeVersion,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage,
    `Events.Timestamp`,
    `Events.Name`,
    `Events.Attributes`,
    `Links.TraceId`,
    `Links.SpanId`,
    `Links.TraceState`,
    `Links.Attributes`,
    `__hdx_materialized_rum.sessionId`
FROM otel_traces_local_dist
UNION ALL
SELECT
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ServiceName,
    ResourceAttributes,
    ScopeName,
    ScopeVersion,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage,
    `Events.Timestamp`,
    `Events.Name`,
    `Events.Attributes`,
    `Links.TraceId`,
    `Links.SpanId`,
    `Links.TraceState`,
    `Links.Attributes`,
    `__hdx_materialized_rum.sessionId`
FROM otel_traces_s3
WHERE Timestamp < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_metrics_gauge AS
SELECT * FROM otel_metrics_gauge_local_dist
UNION ALL
SELECT * FROM otel_metrics_gauge_s3
WHERE TimeUnix < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_metrics_sum AS
SELECT * FROM otel_metrics_sum_local_dist
UNION ALL
SELECT * FROM otel_metrics_sum_s3
WHERE TimeUnix < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_metrics_histogram AS
SELECT * FROM otel_metrics_histogram_local_dist
UNION ALL
SELECT * FROM otel_metrics_histogram_s3
WHERE TimeUnix < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_metrics_exp_histogram AS
SELECT * FROM otel_metrics_exp_histogram_local_dist
UNION ALL
SELECT * FROM otel_metrics_exp_histogram_s3
WHERE TimeUnix < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_metrics_summary AS
SELECT * FROM otel_metrics_summary_local_dist
UNION ALL
SELECT * FROM otel_metrics_summary_s3
WHERE TimeUnix < now64(9) - INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS otel_traces_trace_id_ts AS
SELECT
    TraceId,
    min(Start) AS Start,
    max(End) AS End
FROM (
    SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End
    FROM otel_traces_local_dist
    WHERE TraceId != ''
    GROUP BY TraceId
    UNION ALL
    SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End
    FROM otel_traces_s3
    WHERE TraceId != ''
      AND Timestamp < now64(9) - INTERVAL 30 DAY
    GROUP BY TraceId
)
GROUP BY TraceId;
