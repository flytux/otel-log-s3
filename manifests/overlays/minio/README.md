# OTEL 로그 조회
```
SELECT
    fromUnixTimestamp64Nano(CAST(log.1, 'Int64')) AS timestamp,
    arrayFirst(x -> x.1 = 'service.name', res.1.1).2.1 AS service_name,
    log.2 AS severity,
    log.3.1 AS body,
    log.4 AS trace_id,
    log.5 AS span_id,
    CAST((arrayMap(x -> x.1, log.6), arrayMap(x -> x.2.1, log.6)), 'Map(String, String)') AS log_attributes
FROM s3(
    'http://minio-service:9000/clickhouse-cloud/**/*.json', 
    'minioadmin', 
    'minioadmin', 
    'JSONEachRow',
    -- 💡 [핵심] ClickHouse가 타입을 명확히 알 수 있도록 구조(Schema)를 직접 기입합니다.
    'resourceLogs Array(Tuple(
        resource Tuple(attributes Array(Tuple(key String, value Tuple(stringValue String, intValue String)))),
        scopeLogs Array(Tuple(
            scope Tuple(name String),
            logRecords Array(Tuple(
                timeUnixNano String,
                severityText String,
                body Tuple(stringValue String),
                traceId String,
                spanId String,
                attributes Array(Tuple(key String, value Tuple(stringValue String)))
            ))
        ))
    ))'
)
LEFT ARRAY JOIN resourceLogs AS res
LEFT ARRAY JOIN res.2 AS scope
LEFT ARRAY JOIN scope.2 AS log;
```
----------------------------------------------------------
# K8S FILE 로그 조회
```
SELECT 
    -- 1. 로그 발생 타임스탬프 파싱 (나노초 변환)
    fromUnixTimestamp64Nano(CAST(JSONExtractString(line, 'resourceLogs', 1, 'scopeLogs', 1, 'logRecords', 1, 'timeUnixNano'), 'Int64')) AS timestamp,
    
    -- 2. 복잡한 OTLP 리소스 속성 배열 안에서 원하는 키(k8s.*)를 찾아 쏙 뽑아내는 로직
    arrayFirst(x -> x.1 = 'k8s.namespace.name', JSONExtract(line, 'resourceLogs', 1, 'resource', 'attributes', 'Array(Tuple(String, Tuple(String)))')).2.1 AS namespace,
    arrayFirst(x -> x.1 = 'k8s.pod.name',       JSONExtract(line, 'resourceLogs', 1, 'resource', 'attributes', 'Array(Tuple(String, Tuple(String)))')).2.1 AS pod_name,
    arrayFirst(x -> x.1 = 'k8s.container.name', JSONExtract(line, 'resourceLogs', 1, 'resource', 'attributes', 'Array(Tuple(String, Tuple(String)))')).2.1 AS container_name,
    
    -- 3. 실제 컨테이너가 콘솔에 출력한 원본 로그 본문
    JSONExtractString(line, 'resourceLogs', 1, 'scopeLogs', 1, 'logRecords', 1, 'body', 'stringValue') AS log_body,
    
    -- 4. 로그 레벨 (INFO, WARN, ERROR 등)
    JSONExtractString(line, 'resourceLogs', 1, 'scopeLogs', 1, 'logRecords', 1, 'severityText') AS severity
FROM s3(
    'http://minio-service:9000/clickhouse-cold/k8s/**/*.json', 
    'minioadmin', 
    'minioadmin', 
    'JSONAsString',
    'line String' -- 💡 [핵심 교정] s3 함수에 들어올 단일 컬럼 이름과 형식을 명시적으로 선언합니다.
)
```
------------------------------------------------------------
# JournalD 로그 조회
