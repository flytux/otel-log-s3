# OTel Collector -> Object Storage -> ClickHouse

OTel Collector는 수신한 logs / traces / metrics / sessions를 object storage에 원본 JSON으로 저장한다.  
ClickHouse는 별도 적재 없이 `s3()` / `azureBlobStorage()` 함수를 사용한 **뷰(View)** 로 object storage를 직접 조회한다.

## 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
| --- | --- | --- |
| OTel Collector | OTLP 수신 후 object storage 저장 | `otel-collector-service:4317`, `:4318` |
| MinIO (minio overlay) | 원본 저장소 (로컬 S3 호환) | `http://minio-service:9000` |
| Azure Blob Storage (azureblob overlay) | 원본 저장소 | `https://<account>.blob.core.windows.net` |
| ClickHouse | 3-shard 클러스터, s3/azureBlobStorage 뷰 조회 | `http://clickhouse-service:8123` |
| Grafana | ClickHouse 조회 UI | `http://grafana-service:3000` |
| HyperDX | ClickHouse 조회 UI | `http://hyperdx.node-01` |

## 버킷/컨테이너 구조

```text
otel-logs/logs/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-traces/traces/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-metrics/metrics/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
hyperdx-sessions/sessions/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
```

## ClickHouse 구조

`manifests/base/clickhouse-version.yaml`이 ClickHouse 이미지 버전을 관리하고, overlay별 `cluster.sql`이 각 저장소 연동 방식을 정의한다.

- ClickHouse 3-shard 클러스터 (`clickhouse-0`, `1`, `2`)
- 로컬 테이블이나 Distributed 테이블 없이 **뷰만** 생성한다.
- 각 뷰는 쿼리 시점에 object storage를 직접 스캔한다.
- `cluster.sql`은 3개 shard 모두에 동일하게 적용된다.

## Overlay별 차이

| | minio | azureblob |
|---|---|---|
| 저장소 함수 | `s3()` | `azureBlobStorage()` |
| 인증 | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` 환경변수 | Workload Identity (AccountKey 없는 connection string) |
| 추가 리소스 | MinIO Deployment / Service / PVC / 버킷 초기화 Job | — |

### azureblob 인증

`azureBlobStorage()` connection string에 AccountKey를 포함하지 않는다.

```
DefaultEndpointsProtocol=https;AccountName=<account>;EndpointSuffix=core.windows.net
```

ClickHouse 파드에 Azure Workload Identity가 설정되면 자동으로 `DefaultAzureCredential`로 인증된다.  
`AZURE_STORAGE_ACCOUNT_NAME` 환경변수를 `clickhouse-init` Job에 주입하면 `run.sh`가 `__AZURE_STORAGE_ACCOUNT_NAME__` 플레이스홀더를 치환한다.

## 조회 뷰

UI가 사용하는 최종 뷰는 아래와 같다. 모든 뷰는 쿼리 시점에 object storage 전체를 와일드카드 패턴으로 스캔한다.

| 뷰 | 데이터 |
|---|---|
| `otel_logs` | logs |
| `otel_traces` | traces |
| `otel_traces_trace_id_ts` | trace ID 기반 시간 범위 조회 |
| `hyperdx_sessions` | RUM sessions |
| `otel_metrics_gauge` | gauge metrics |
| `otel_metrics_sum` | sum metrics |
| `otel_metrics_histogram` | histogram metrics |
| `otel_metrics_exp_histogram` | exponential histogram metrics |
| `otel_metrics_summary` | summary metrics |

## 배포

### minio overlay

```bash
kubectl apply -k manifests/overlays/minio
```

### azureblob overlay

```bash
kubectl apply -k manifests/overlays/azureblob
```

배포 후 확인:

```bash
kubectl wait --for=condition=complete job/clickhouse-init --timeout=180s
kubectl logs job/clickhouse-init
```

`clickhouse-init` Job은 아래를 수행한다.

1. ClickHouse Keeper와 3개 shard 준비 대기
2. 각 shard에 `cluster.sql` 적용 (뷰 및 UDF 생성)
3. cluster shard 수와 9개 뷰 생성 여부 검증

## 성능 특성

- 모든 쿼리가 object storage를 실시간 스캔하므로 **쿼리 범위가 넓을수록 비용이 증가**한다.
- 파티션 경로(`year=*/month=*/...`)를 활용한 **시간 범위 필터**를 반드시 사용해야 한다.
- ClickHouse 로컬 테이블이 없으므로 초기 배포 시 backfill 대기 시간이 없다.

## 로컬 확인

```bash
kubectl port-forward svc/clickhouse-service 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'
```

```sql
-- 1) 클러스터가 3 shard인지 확인
SELECT
  uniqExact(shard_num) AS uniq_shards,
  groupArrayDistinct(concat(toString(shard_num), ':', host_name)) AS members
FROM system.clusters
WHERE cluster = 'default';

-- 2) 뷰 목록 확인
SELECT name, engine
FROM system.tables
WHERE database = currentDatabase()
  AND (name LIKE 'otel_%' OR name LIKE 'hyperdx_%')
ORDER BY name;

-- 3) logs 조회 (시간 범위 필수)
SELECT count()
FROM otel_logs
WHERE Timestamp >= now64(9) - INTERVAL 1 HOUR;

-- 4) traces 조회
SELECT count()
FROM otel_traces
WHERE Timestamp >= now64(9) - INTERVAL 1 HOUR;

-- 5) metrics 조회
SELECT ServiceName, MetricName, count()
FROM otel_metrics_gauge
WHERE TimeUnix >= now64(9) - INTERVAL 1 HOUR
GROUP BY ServiceName, MetricName
ORDER BY count() DESC
LIMIT 10;
```

