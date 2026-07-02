# OTel Collector → ClickHouse → Minio (TTL MOVE)

OTel Collector가 수신한 logs / traces / metrics / sessions를 **ClickHouse에 저장** 
- ClickHouse는 30일 데이터를 로컬 디스크(hot)에 보관
- TTL 만료 또는 디스크 여유 30% 미만 시 Object Storage(cold)로 이관
- hot/cold 데이터를 **동일 테이블로 투명하게 조회**

## 아키텍처

```
앱 / 브라우저
    │  OTLP (gRPC :4317 / HTTP :4318)
    ▼
OTel Collector
    │  clickhouse exporter (TCP :9000)
    ▼
otel_logs / otel_traces / otel_metrics_* / hyperdx_sessions  ← Distributed 테이블
    │  rand() 샤딩
    ▼
otel_logs_local / ... (ReplicatedMergeTree, 각 shard)
    │  TTL MOVE: 30일 경과 OR 디스크 여유 30% 미만
    ▼
Object Storage (MinIO clickhouse-cold / Azure Blob clickhouse-cold)
```

## 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
| --- | --- | --- |
| OTel Collector | OTLP 수신 → ClickHouse 기입 | `:4317` (gRPC), `:4318` (HTTP) |
| ClickHouse | 3-shard 클러스터, 데이터 수신 및 tiered storage | `clickhouse-clickhouse-headless:9000` |
| MinIO (minio overlay) | cold tier 오브젝트 스토리지 | `http://minio-service:9000` |
| Azure Blob Storage (azureblob overlay) | cold tier 오브젝트 스토리지 | `https://<account>.blob.core.windows.net` |
| Grafana | ClickHouse 조회 UI | `http://grafana-service:3000` |
| HyperDX | 통합 Observability UI | `http://hyperdx.node-01` |

## ClickHouse 테이블 구조

| 테이블 | 엔진 | 역할 |
|--------|------|------|
| `otel_logs_local` | ReplicatedMergeTree | 실제 데이터 저장 (hot→cold 이관) |
| `otel_logs` | Distributed | INSERT 라우터 + 전체 shard 조회 |
| `otel_traces_local` | ReplicatedMergeTree | — |
| `otel_traces` | Distributed | — |
| `otel_metrics_gauge_local` | ReplicatedMergeTree | — |
| `otel_metrics_gauge` | Distributed | — |
| *(sum / histogram / exp_histogram / summary 동일)* | — | — |
| `hyperdx_sessions_local` | ReplicatedMergeTree | HyperDX Session Replay |
| `hyperdx_sessions` | Distributed | — |
| `otel_traces_trace_id_ts` | VIEW | TraceId 기반 시간 범위 조회 |

### 스토리지 정책 (tiered)

```xml
<tiered>
  <volumes>
    <hot><disk>default</disk></hot>   <!-- 로컬 디스크 -->
    <cold><disk>s3_cold / azure_cold</disk></cold>
  </volumes>
  <move_factor>0.3</move_factor>     <!-- 여유공간 30% 미만 시 cold 이동 -->
</tiered>
```

TTL: `toDateTime(Timestamp) + INTERVAL 30 DAY TO VOLUME 'cold'`

**이동 조건**
1. 파티션(일 단위) 내 가장 최신 행의 Timestamp + 30일 경과
2. 로컬 디스크 여유공간 < 30%

## OTel Collector 설정

### Session Replay 라우팅 (Optional 구성)

`LogAttributes["sessionId"]` 속성 유무로 일반 로그와 세션 로그를 분리

```
로그 (sessionId 없음) → filter/no_session → clickhouse/otel → otel_logs
로그 (sessionId 있음) → filter/session_only → clickhouse/sessions → hyperdx_sessions
```

### Batch 작업 설정

```yaml
batch:
  send_batch_size: 10000   # 한 번에 10k 행 묶어 INSERT 빈도 감소
  timeout: 5s

clickhouse/otel:
  num_consumers: 4         # 동시 연결 제한 (too_many_parts 방지)
  queue_size: 5000         # burst 흡수용 큐
  max_elapsed_time: 600s   # 장애 시 10분간 재시도
```

## 저장 방식 용량 비교

### OTel → S3 원본 JSON vs ClickHouse TTL MOVE

| 방식 | 포맷 | 상대 크기 |
|------|------|----------|
| OTel Collector → S3 직접 저장 | 원본 JSON (텍스트, 행 단위) | **22x** |
| ClickHouse TTL MOVE → S3 | 컬럼형 바이너리 + ZSTD 압축 | **1x** (기준) |
| ClickHouse 비압축 원본 | — | 17x |

실측 기준 (otel_logs, 약 2,800행):

```
원본 JSON 추정:  ~4.1 MiB
ClickHouse ZSTD: ~186 KiB  → 약 22배
```

### 압축률 차이

```
[원본 JSON - 행 단위]
{"Timestamp":"2024-01-01T00:00:01Z","ServiceName":"dice","SeverityText":"INFO","Body":"GET /roll",...}
{"Timestamp":"2024-01-01T00:00:02Z","ServiceName":"dice","SeverityText":"INFO","Body":"GET /roll",...}
→ 필드명 반복, 텍스트 형식

[ClickHouse - 컬럼 단위 + ZSTD]
Timestamp 컬럼   : Delta 인코딩 → 차이값만 저장 (시계열 특화)
ServiceName 컬럼 : LowCardinality → 딕셔너리 인코딩 (반복값 1바이트)
SeverityText 컬럼: LowCardinality → 동일
→ 컬럼별 연속 동일값 압축 후 ZSTD 최종 압축
```

### 실제 압축 비율 확인 쿼리

```sql
SELECT
  table,
  sum(rows) AS rows,
  formatReadableSize(sum(data_compressed_bytes))   AS compressed,
  formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
  round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS compress_ratio
FROM system.parts
WHERE database = currentDatabase() AND table LIKE '%_local' AND active = 1
GROUP BY table ORDER BY table;
```

### 운영 환경 예상 절감 효과 (로그 1TB/월 기준)

| 방식 | S3 저장량 | 비고 |
|------|----------|------|
| OTel 원본 JSON → S3 | ~1,000 GB | 텍스트, 필드명 포함 |
| ClickHouse TTL MOVE | **~45 GB** | 컬럼형 ZSTD, 약 22배 절감 |

> 압축률은 데이터 특성(반복도, 카디널리티)에 따라 달라지며, 실제 환경에서는 10배~30배 범위 예상

## 배포

### minio overlay

```bash
kubectl apply -k manifests/overlays/minio
```

### azureblob overlay

```bash
kubectl apply -k manifests/overlays/azureblob
```

배포 후 초기화 확인:

```bash
kubectl wait --for=condition=complete job/clickhouse-init -n monitoring --timeout=180s
kubectl logs job/clickhouse-init -n monitoring
```

`clickhouse-init` Job 수행 내용:
1. ClickHouse Keeper + 3개 shard 준비 대기
2. `cluster.sql` 적용 (테이블 재생성)
3. 클러스터 topology 및 테이블 생성 검증

## 검증 쿼리

### 1. 클러스터 및 테이블 확인

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
-- 3 shard 확인
SELECT uniqExact(shard_num) AS shards FROM system.clusters WHERE cluster='default';

-- 테이블 목록 (14 테이블 + 1 VIEW)
SELECT name, engine, storage_policy
FROM system.tables
WHERE database = currentDatabase()
  AND (name LIKE 'otel_%' OR name LIKE 'hyperdx_%')
ORDER BY name;"
```

### 2. 데이터 수집 확인

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
SELECT table, sum(rows) AS rows, min(min_time) AS oldest, max(max_time) AS newest
FROM system.parts
WHERE database = currentDatabase() AND table LIKE '%_local' AND active = 1
GROUP BY table ORDER BY table;"
```

### 3. 3개 샤드 균등 분산 확인

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
SELECT hostName() AS shard, table, sum(rows) AS rows
FROM clusterAllReplicas('default', system.parts)
WHERE database = currentDatabase() AND table LIKE '%_local' AND active = 1
GROUP BY shard, table ORDER BY table, shard;"
```

### 4. TTL MOVE (cold 이관) 확인

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
-- hot/cold 디스크별 파트 분포
SELECT disk_name, table, count() AS parts, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = currentDatabase() AND table LIKE '%_local' AND active = 1
GROUP BY disk_name, table ORDER BY disk_name, table;"
```

cold로 이관 전 강제 트리거:

```sql
-- 특정 테이블 TTL 즉시 적용
ALTER TABLE otel_logs_local MATERIALIZE TTL;

-- 특정 파티션 강제 merge (TTL 재평가)
OPTIMIZE TABLE otel_logs_local PARTITION '2024-01-01' FINAL;
```

### 5. 스토리지 정책 확인

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
SELECT policy_name, volume_name, disks, move_factor
FROM system.storage_policies WHERE policy_name = 'tiered'
FORMAT Vertical;"
```

### 6. 데이터 조회 (hot + cold 통합)

```bash
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
-- 최근 1시간 로그
SELECT count() FROM otel_logs WHERE Timestamp >= now() - INTERVAL 1 HOUR;

-- 최근 1시간 트레이스
SELECT count() FROM otel_traces WHERE Timestamp >= now() - INTERVAL 1 HOUR;

-- 메트릭 서비스별 집계
SELECT ServiceName, MetricName, count()
FROM otel_metrics_gauge
WHERE TimeUnix >= now() - INTERVAL 1 HOUR
GROUP BY ServiceName, MetricName
ORDER BY count() DESC LIMIT 10;"
```

### 7. OTel Collector 상태 확인

```bash
# 최근 에러 확인
kubectl logs -n monitoring -l app.kubernetes.io/name=opentelemetry-collector --tail=20

# ClickHouse INSERT 통계
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse \
  --query "
SELECT event, value FROM system.events
WHERE event IN ('InsertQuery','InsertedRows','InsertedBytes','RejectedInserts','DelayedInserts')
ORDER BY event;"
```

## MinIO 버킷 구조

```text
clickhouse-cold/          ← ClickHouse TTL MOVE 대상 (유일한 필수 버킷)
hyperdx-sessions/         ← HyperDX Session Replay 사용 시
```

cold 버킷 내 파일은 ClickHouse 내부 포맷(해시 경로)으로 저장되며, ClickHouse를 통해서만 조회된다.

```bash
# MinIO 내 cold 데이터 확인
kubectl exec -n monitoring <minio-pod> -- \
  mc ls --recursive local/clickhouse-cold/ | wc -l
```

## 로컬 접속

```bash
# ClickHouse HTTP API
kubectl port-forward -n monitoring svc/clickhouse-clickhouse-headless 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'

# ClickHouse CLI (파드 직접)
kubectl exec -n monitoring clickhouse-clickhouse-0-0-0 -- \
  clickhouse-client --user default --password clickhouse
```
