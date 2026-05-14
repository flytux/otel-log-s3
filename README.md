# ClickHouse + MinIO 감사 로그 파이프라인

OTel Collector → MinIO(S3) → ClickHouse 실시간 감사 로그 조회 설정 가이드

---

## 1. 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
|---|---|---|
| OTel Collector | 로그 수집 → MinIO 저장 | `otel-collector-service.monitoring:4317` |
| MinIO | S3 호환 오브젝트 스토리지 | `http://minio-service.monitoring:9000` |
| ClickHouse | 분석 쿼리 엔진 | `clickhouse-service.monitoring:8123` |
| ClickHouse Keeper | ZooKeeper 대체 (S3Queue 필요) | `localhost:9181` |

MinIO 버킷/경로 구조:
```
audit-log/
└── audit/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── hour=HH/
                    └── minute=MM/
                        └── logs_*.json   ← OTLP JSON 배치 파일
```

---

## 2. 배포

### ClickHouse 배포
```bash
kubectl apply -f clickhouse-deployment.yaml
kubectl rollout status deployment/clickhouse -n monitoring
```

### 배포 확인
```bash
kubectl get pods -n monitoring | grep clickhouse
kubectl logs -n monitoring deployment/clickhouse --tail=50
```

### ClickHouse 접속
```bash
# clickhouse-client (Pod 내부)
kubectl exec -it -n monitoring deployment/clickhouse -- clickhouse-client --password clickhouse

# HTTP API
kubectl port-forward -n monitoring svc/clickhouse-service 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'
```

---

## 3. 테이블 생성

ClickHouse에 접속한 후 아래 SQL을 순서대로 실행합니다.

### 3-1. 메인 테이블 (MergeTree)
```sql
CREATE TABLE IF NOT EXISTS audit_logs
(
    timestamp       DateTime64(9),
    observed_time   DateTime64(9),
    severity_text   String,
    severity_number UInt8,
    body            String,
    attributes      Map(String, String),
    resource_attrs  Map(String, String),
    trace_id        String,
    span_id         String
)
ENGINE = MergeTree()
ORDER BY (timestamp, severity_text);
```

### 3-2. S3Queue 테이블 (실시간 파일 감지)
```sql
CREATE TABLE IF NOT EXISTS audit_logs_queue
(json String)
ENGINE = S3Queue(
    'http://minio-service.monitoring:9000/audit-log/audit/year=*/month=*/day=*/hour=*/minute=*/*.json',
    'minioadmin', 'minioadmin',
    'JSONAsString'
)
SETTINGS
    mode = 'ordered',
    polling_min_timeout_ms = 1000,
    polling_max_timeout_ms = 10000;
```

> **주의**: `mode = 'ordered'`는 ClickHouse Keeper가 필요합니다.  
> Keeper 동작 확인: `SELECT name FROM system.zookeeper WHERE path = '/';`

### 3-3. Materialized View (자동 적재)
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS audit_logs_mv TO audit_logs AS
SELECT
    fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'timeUnixNano')))         AS timestamp,
    fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'observedTimeUnixNano'))) AS observed_time,
    JSONExtractString(lr, 'severityText')                                                  AS severity_text,
    JSONExtractUInt(lr, 'severityNumber')                                                  AS severity_number,
    JSONExtractString(JSONExtractRaw(lr, 'body'), 'stringValue')                           AS body,
    mapFromArrays(
        arrayMap(a -> JSONExtractString(a, 'key'), JSONExtractArrayRaw(lr, 'attributes')),
        arrayMap(a -> JSONExtractString(JSONExtractRaw(a, 'value'), 'stringValue'), JSONExtractArrayRaw(lr, 'attributes'))
    ) AS attributes,
    mapFromArrays(
        arrayMap(a -> JSONExtractString(a, 'key'),        JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes')),
        arrayMap(a -> JSONExtractString(JSONExtractRaw(a, 'value'), 'stringValue'), JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes'))
    ) AS resource_attrs,
    JSONExtractString(lr, 'traceId') AS trace_id,
    JSONExtractString(lr, 'spanId')  AS span_id
FROM (
    SELECT JSONExtractArrayRaw(json, 'resourceLogs') AS rls
    FROM audit_logs_queue
)
ARRAY JOIN rls                                    AS rl
ARRAY JOIN JSONExtractArrayRaw(rl, 'scopeLogs')  AS sl
ARRAY JOIN JSONExtractArrayRaw(sl, 'logRecords') AS lr;
```

---

## 4. 기존 데이터 백필

S3Queue는 생성 이후 신규 파일만 처리합니다. 기존 파일을 소급 적재할 때는 `s3()` 함수로 직접 INSERT합니다.

```sql
INSERT INTO audit_logs
SELECT
    fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'timeUnixNano')))         AS timestamp,
    fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'observedTimeUnixNano'))) AS observed_time,
    JSONExtractString(lr, 'severityText')                                                  AS severity_text,
    JSONExtractUInt(lr, 'severityNumber')                                                  AS severity_number,
    JSONExtractString(JSONExtractRaw(lr, 'body'), 'stringValue')                           AS body,
    mapFromArrays(
        arrayMap(a -> JSONExtractString(a, 'key'), JSONExtractArrayRaw(lr, 'attributes')),
        arrayMap(a -> JSONExtractString(JSONExtractRaw(a, 'value'), 'stringValue'), JSONExtractArrayRaw(lr, 'attributes'))
    ) AS attributes,
    mapFromArrays(
        arrayMap(a -> JSONExtractString(a, 'key'),        JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes')),
        arrayMap(a -> JSONExtractString(JSONExtractRaw(a, 'value'), 'stringValue'), JSONExtractArrayRaw(JSONExtractRaw(rl, 'resource'), 'attributes'))
    ) AS resource_attrs,
    JSONExtractString(lr, 'traceId') AS trace_id,
    JSONExtractString(lr, 'spanId')  AS span_id
FROM (
    SELECT JSONExtractArrayRaw(json, 'resourceLogs') AS rls
    FROM s3(
        'http://minio-service.monitoring:9000/audit-log/audit/year=*/month=*/day=*/hour=*/minute=*/*.json',
        'minioadmin', 'minioadmin',
        'JSONAsString'
    )
)
ARRAY JOIN rls                                    AS rl
ARRAY JOIN JSONExtractArrayRaw(rl, 'scopeLogs')  AS sl
ARRAY JOIN JSONExtractArrayRaw(sl, 'logRecords') AS lr;
```

---

## 5. 기본 조회

### 5-1. 적재 건수 및 시간 범위 확인
```sql
SELECT
    count()        AS total,
    min(timestamp) AS oldest,
    max(timestamp) AS latest
FROM audit_logs;
```

### 5-2. 최신 로그 50건
```sql
SELECT timestamp, severity_text, body, attributes
FROM audit_logs
ORDER BY timestamp DESC
LIMIT 50;
```

### 5-3. 심각도별 집계
```sql
SELECT
    severity_text,
    count() AS cnt
FROM audit_logs
GROUP BY severity_text
ORDER BY cnt DESC;
```

### 5-4. log_category / log_type 필터
```sql
SELECT
    timestamp,
    attributes['log_category'] AS log_category,
    attributes['log_type']     AS log_type,
    attributes['user_id']      AS user_id,
    body
FROM audit_logs
WHERE attributes['log_category'] = 'audit'
ORDER BY timestamp DESC
LIMIT 50;
```

### 5-5. 특정 사용자 활동 조회
```sql
SELECT timestamp, attributes['log_type'] AS log_type, body
FROM audit_logs
WHERE attributes['user_id'] = 'user123'
ORDER BY timestamp DESC
LIMIT 100;
```

### 5-6. 시간대별 로그 집계
```sql
SELECT
    toStartOfMinute(timestamp) AS minute,
    attributes['log_type']     AS log_type,
    count()                    AS cnt
FROM audit_logs
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute, log_type
ORDER BY minute DESC;
```

---

## 6. MinIO 원본 직접 조회 (s3() 함수)

테이블 없이 MinIO 파일을 직접 쿼리할 때 사용합니다.

### 파일 목록 확인
```sql
SELECT _file, _path, _size
FROM s3(
    'http://minio-service.monitoring:9000/audit-log/audit/year=*/month=*/day=*/hour=*/minute=*/*.json',
    'minioadmin', 'minioadmin',
    'JSONAsString'
)
ORDER BY _path DESC
LIMIT 20;
```

### 원본 JSON 확인
```sql
SELECT json
FROM s3(
    'http://minio-service.monitoring:9000/audit-log/audit/year=*/month=*/day=*/hour=*/minute=*/*.json',
    'minioadmin', 'minioadmin',
    'JSONAsString'
)
LIMIT 3;
```

### 파싱 조회 (속성 Map 포함)
```sql
SELECT
    timestamp,
    attrs['log_category'] AS log_category,
    attrs['log_type']     AS log_type,
    attrs['user_id']      AS user_id,
    body
FROM (
    SELECT
        fromUnixTimestamp64Nano(toInt64OrZero(JSONExtractString(lr, 'timeUnixNano'))) AS timestamp,
        JSONExtractString(JSONExtractRaw(lr, 'body'), 'stringValue')                   AS body,
        mapFromArrays(
            arrayMap(a -> JSONExtractString(a, 'key'), JSONExtractArrayRaw(lr, 'attributes')),
            arrayMap(a -> JSONExtractString(JSONExtractRaw(a, 'value'), 'stringValue'), JSONExtractArrayRaw(lr, 'attributes'))
        ) AS attrs
    FROM (
        SELECT JSONExtractArrayRaw(json, 'resourceLogs') AS rls
        FROM s3(
            'http://minio-service.monitoring:9000/audit-log/audit/year=*/month=*/day=*/hour=*/minute=*/*.json',
            'minioadmin', 'minioadmin',
            'JSONAsString'
        )
    )
    ARRAY JOIN rls                                    AS rl
    ARRAY JOIN JSONExtractArrayRaw(rl, 'scopeLogs')  AS sl
    ARRAY JOIN JSONExtractArrayRaw(sl, 'logRecords') AS lr
)
WHERE attrs['log_category'] = 'audit'
ORDER BY timestamp DESC
LIMIT 50;
```

> **주의**: `ARRAY JOIN`을 Map에 직접 적용하면 `Tuple(key, value)`로 풀려 `[]` 접근이 불가합니다.  
> 반드시 서브쿼리에서 `mapFromArrays(...) AS attrs`로 만든 후 외부 쿼리에서 `attrs['key']`로 접근하세요.

---

## 7. 테이블 재설정 (초기화)

설정 오류나 경로 변경 시 전체 재생성합니다.

```sql
DROP TABLE IF EXISTS audit_logs_mv;
DROP TABLE IF EXISTS audit_logs_queue;
DROP TABLE IF EXISTS audit_logs;
```

이후 **3. 테이블 생성** 섹션을 순서대로 재실행합니다.

---

## 8. 트러블슈팅

| 증상 | 원인 | 해결 |
|---|---|---|
| `Code: 139` ZooKeeper 없음 | Keeper 미설정 | `keeper.xml` ConfigMap 확인, Pod 재시작 |
| `system.zookeeper` 0 rows | Keeper 미동작 | `kubectl rollout restart deployment/clickhouse -n monitoring` |
| `Code: 497` Named Collection 권한 없음 | users.xml 설정 누락 | `named_collection_control=1` 확인 후 재배포 |
| `Code: 36` region/use_environment_credentials 오류 | 잘못된 Named Collection 키 | `region` 제거, `false` → `0` 변경 |
| `Code: 47` 컬럼 없음 (`data`) | `JSONAsString` 포맷 시 컬럼명은 `json` | `data` → `json`으로 변경 |
| `Code: 43` arrayElement 타입 오류 | `ARRAY JOIN`을 Map에 직접 사용 | 서브쿼리에서 Map 먼저 생성 후 외부에서 `[]` 접근 |
| s3() 0 rows | 경로 패턴 불일치 | `_file`, `_path`로 실제 경로 확인 후 패턴 수정 |

