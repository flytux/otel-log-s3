# OTel Collector → MinIO(S3) → ClickHouse S3Queue

OTel Collector는 **MinIO(S3)에만 저장**하고, ClickHouse가 **S3Queue로 logs / traces / metrics를 bucket별로 읽어 적재**하는 구성이다. HyperDX와 Grafana는 S3를 직접 읽지 않고 **ClickHouse를 조회**한다.

## 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
| --- | --- | --- |
| OTel Collector | OTLP 수신 후 MinIO bucket별 저장 | `otel-collector-service:4317`, `:4318` |
| MinIO | S3 호환 원본 저장소 | `http://minio-service:9000` |
| ClickHouse | S3Queue 기반 적재·분석 | `http://clickhouse-service:8123` |
| Grafana | ClickHouse 조회 UI | `http://grafana-service:3000` |
| HyperDX | ClickHouse 기반 조회 UI | `http://hyperdx.node-01` |

## 버킷 구조

```text
otel-logs/logs/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-traces/traces/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-metrics/metrics/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
hyperdx-sessions/sessions/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
```

## 배포

```bash
kubectl apply -k manifests
# 또는 namespace 지정
kubectl apply -n observability -k manifests
```

- `minio-bucket-bootstrap` Job이 `otel-logs`, `otel-traces`, `otel-metrics` bucket을 자동 생성하고, `minio-hyperdx-sessions-bootstrap` Job이 `hyperdx-sessions` bucket을 추가로 생성한다.
- `clickhouse-init` Job이 ClickHouse 공식 이미지 `clickhouse/clickhouse-server:25.7.5` 기준 DDL을 자동 적용하고 결과를 검증한다.
- Grafana는 시작 시 `grafana-clickhouse-datasource` 플러그인을 설치하고 ClickHouse 데이터소스를 자동 provisioning 한다.
- HyperDX는 MongoDB와 함께 배포되고, 기본 ClickHouse connection / source를 자동 bootstrap 한다.

## 데이터 흐름

1. Collector가 logs / traces / metrics를 각각 다른 MinIO bucket prefix에 JSON으로 기록한다.
2. ClickHouse `S3Queue`가 새 객체를 감지한다.
3. Materialized View가 OTLP JSON을 분석용 테이블로 정규화한다.
4. Grafana / HyperDX는 ClickHouse만 조회한다.

Collector는 ClickHouse로 직접 쓰지 않는다.

## ClickHouse 스키마

이번 정비에서는 기존 snake_case + Grafana 호환 view 중심 구조를 없애고, **Grafana ClickHouse datasource / OTel ClickHouse exporter가 기대하는 표준 컬럼명**에 맞춰 정리했다.

### Logs

- 테이블: `otel_logs`
- 핵심 컬럼:
  - `Timestamp`, `TimestampTime`
  - `TraceId`, `SpanId`, `TraceFlags`
  - `SeverityText`, `SeverityNumber`
  - `ServiceName`, `Body`
  - `ResourceAttributes`, `ScopeAttributes`, `LogAttributes`

### Traces

- 테이블: `otel_traces`
- 핵심 컬럼:
  - `Timestamp`, `TraceId`, `SpanId`, `ParentSpanId`
  - `TraceState`, `SpanName`, `SpanKind`, `ServiceName`
  - `SpanAttributes`, `ResourceAttributes`
  - `Duration`, `StatusCode`, `StatusMessage`
  - `Events.*`, `Links.*`
- 보조 테이블: `otel_traces_trace_id_ts`
  - Grafana trace drill-down 시 trace ID별 시작/종료 시각 범위를 빠르게 찾기 위한 lookup 테이블
- 세션 연동:
  - `__hdx_materialized_rum.sessionId` materialized column으로 HyperDX session ↔ trace 연결

### Session Replay

- 테이블: `hyperdx_sessions`
- 핵심 컬럼:
  - `Timestamp`, `TimestampTime`
  - `TraceId`, `SpanId`, `TraceFlags`
  - `SeverityText`, `SeverityNumber`
  - `ServiceName`, `Body`
  - `ResourceAttributes`, `ScopeAttributes`, `LogAttributes`
  - `__hdx_materialized_rum.sessionId`
  - `__hdx_materialized_type`
- 용도:
  - 브라우저 HyperDX SDK가 보낸 replay/log payload를 별도 bucket에서 읽어 세션 UI 전용 source로 사용

### Metrics

기존 단일 `otel_metrics` 테이블 대신 OTLP metric type별 테이블로 분리했다.

- `otel_metrics_gauge`
- `otel_metrics_sum`
- `otel_metrics_histogram`
- `otel_metrics_exp_histogram`
- `otel_metrics_summary`

공통 컬럼은 `ResourceAttributes`, `Scope*`, `ScopeDroppedAttrCount`, `ServiceName`, `MetricName`, `Attributes`, `StartTimeUnix`, `TimeUnix` 기준으로 맞췄고, 타입별 값 컬럼만 분리했다.

## ClickHouse 초기화와 검증

```bash
kubectl rollout status deployment/clickhouse
kubectl wait --for=condition=complete job/clickhouse-init --timeout=180s
kubectl logs job/clickhouse-init
```

`clickhouse-init` Job은 아래를 수행한다.

1. ClickHouse와 Keeper 응답 대기
2. S3Queue / MergeTree / Materialized View 생성
3. 생성된 객체 수와 engine 타입 검증
4. 실패 시 Job 자체 실패

## 주의사항

이번 스키마 정비는 **구조를 정리하기 위해 DROP + CREATE 방식**으로 DDL을 적용한다. 즉, `clickhouse-init` Job을 다시 실행하면 기존 `otel_*` 테이블 데이터는 재생성된다.

기존 Job을 다시 실행하려면:

```bash
kubectl delete job clickhouse-init
kubectl apply -f manifests/clickhouse.yaml
```

## Grafana

Grafana ClickHouse 데이터소스는 아래 기본 테이블을 사용한다.

- logs: `otel_logs`
- traces: `otel_traces`
- trace lookup: `otel_traces_trace_id_ts`

기존 `otel_traces_grafana` compatibility view는 더 이상 필요하지 않다.

## HyperDX

`manifests/hyperdx.yaml`은 아래 리소스를 함께 생성한다.

- `hyperdx-mongodb` Deployment / Service / PVC
- `hyperdx` Deployment / Service
- `hyperdx-ingress`
- `hyperdx-config` ConfigMap
- `hyperdx-secret` Secret

HyperDX는 기존 ClickHouse를 그대로 사용하고, 첫 기동 시 아래 source를 자동 등록한다.

- Logs → `otel_logs`
- Traces → `otel_traces`
- Metrics → `otel_metrics_gauge`, `otel_metrics_sum`, `otel_metrics_histogram`, `otel_metrics_summary`, `otel_metrics_exp_histogram`
- Sessions → `hyperdx_sessions`

접속 주소:

```text
http://hyperdx.node-01
```

기본 manifest에는 예시 ingestion key가 들어 있으므로, 실제 운영 전에는 `hyperdx-secret`의 `HYPERDX_API_KEY`를 바꾸는 것이 좋다.

## Session Replay 샘플

- 주소: `http://dice.node-01`
- 구성:
  - `/` → 정적 브라우저 샘플(`dice-web`)
  - `/rolldice` → 기존 `dice-service`
- 동작:
  - 페이지가 HyperDX 컨테이너 이미지에 포함된 SDK를 `/assets/hyperdx-browser.js`로 복사해서 사용하므로 air-gapped 환경에서도 외부 CDN 없이 구동 가능
  - 페이지가 랜덤 `userId` / `userName` / `userEmail`을 sessionStorage 기준으로 초기화하고 화면 상단에 크게 표시
  - 페이지가 `@hyperdx/browser`를 `http://otelcol.node-01` OTLP HTTP endpoint로 초기화
  - 1초마다 `/rolldice`를 호출하면서 fetch trace / network event / session replay를 생성
  - Collector는 `dice-web` 서비스 로그만 `hyperdx-sessions` bucket으로 별도 저장

브라우저 샘플은 현재 `hyperdx-secret`에 들어 있는 `HYPERDX_API_KEY`를 그대로 사용한다.

## 예시 쿼리

### Logs

```sql
SELECT Timestamp, ServiceName, SeverityText, Body
FROM otel_logs
WHERE Timestamp >= now() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100;
```

### Traces

```sql
SELECT Timestamp, TraceId, SpanId, SpanName, ServiceName, Duration, StatusCode
FROM otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100;
```

### Metric sums

```sql
SELECT TimeUnix, ServiceName, MetricName, Attributes, Value
FROM otel_metrics_sum
WHERE MetricName = 'calls'
ORDER BY TimeUnix DESC
LIMIT 100;
```

## 샘플 데이터 소스

- `manifests/dice-service.yaml`
  - Java agent로 logs / traces / metrics 전송
  - `dice-caller` sidecar가 `/rolldice`를 1초마다 호출
- `manifests/log-gen.yaml`
  - `telemetrygen logs`
  - `telemetrygen traces`
  - `telemetrygen metrics`

## 검증용 로컬 접속

```bash
kubectl port-forward svc/clickhouse-service 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'
```
