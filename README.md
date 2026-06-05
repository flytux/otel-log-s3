# OTel Collector -> MinIO(S3) -> ClickHouse hybrid query

OTel Collector는 계속해서 **MinIO(S3)** 에 원본을 저장하고, ClickHouse는 **3 shard-local MergeTree + Distributed + S3 direct view** 구조로 최근 30일은 로컬에서 빠르게 조회하고 그보다 오래된 구간은 S3 원본을 함께 읽는다.

## 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
| --- | --- | --- |
| OTel Collector | OTLP 수신 후 MinIO bucket별 저장 | `otel-collector-service:4317`, `:4318` |
| MinIO | S3 호환 원본 저장소 | `http://minio-service:9000` |
| ClickHouse | 3-shard 로컬 적재 + S3 보강 조회 | `http://clickhouse-service:8123` |
| Grafana | ClickHouse 조회 UI | `http://grafana-service:3000` |
| HyperDX | ClickHouse 조회 UI | `http://hyperdx.node-01` |

## 버킷 구조

```text
otel-logs/logs/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-traces/traces/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
otel-metrics/metrics/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
hyperdx-sessions/sessions/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/*.json
```

## ClickHouse 구조

`manifests/clickhouse-cluster.yaml`은 다음 구조를 만들고, `manifests/clickhouse-version.yaml`이 ClickHouse 이미지 버전을, `manifests/clickhouse-azureblobstore-version.yaml`이 Azure Blob Storage 관련 버전을 별도로 관리한다. 세 파일은 분리되어 있어 Kustomize에서 필요에 따라 개별 선택이 가능하다.

1. `clickhouse` StatefulSet 3개 pod (`clickhouse-0..2`)  
2. shard마다 `*_local` MergeTree 테이블 보유  
3. `*_local_dist` Distributed 테이블로 3 shard 조회 분산  
4. `*_s3` view가 MinIO 원본을 직접 파싱  
5. 최종 노출 객체(`otel_logs`, `otel_traces`, `otel_metrics_*`, `hyperdx_sessions`)는 **최근 30일 local + 30일 초과 S3** 를 `UNION ALL` 로 합쳐 제공

즉 UI는 기존 테이블명을 그대로 쓰지만, 내부적으로는 다음처럼 동작한다.

- 최근 30일: shard-local MergeTree + Distributed
- 30일 초과: S3 direct view
- 기간이 30일 경계를 걸치면 두 소스를 함께 조회

## 적재 방식

- `clickhouse-0`만 S3Queue ingest 노드로 동작한다.
- `otel_logs_queue`, `otel_traces_queue`, `otel_metrics_queue`, `hyperdx_sessions_queue` 는 **오직 ingest 노드에만** 생성된다.
- queue의 materialized view는 직접 `*_local_dist` 로 insert 해서 3 shard에 분산 적재한다.
- queue 생성 시 `last_processed_path`를 **현재 시각 기준 30일 전 직전 경로**로 잡아 초기 적재 범위를 최근 30일로 제한한다.
- 이후 새로 들어오는 S3 객체는 계속 로컬 shard로 적재된다.

이렇게 해야 S3Queue를 3개 shard에서 동시에 돌릴 때 생길 수 있는 **중복 소비**를 피할 수 있다.

## 보존 정책

모든 `*_local` 테이블은 30일 TTL을 가진다.

- logs / traces / sessions: `TTL Timestamp + INTERVAL 30 DAY DELETE`
- metrics: `TTL TimeUnix + INTERVAL 30 DAY DELETE`

따라서 로컬 디스크에는 최근 30일만 남고, 오래된 데이터는 MinIO 원본만 남는다.

## 조회 테이블

UI가 사용하는 최종 객체는 아래와 같다.

- logs: `otel_logs`
- traces: `otel_traces`
- trace lookup: `otel_traces_trace_id_ts`
- sessions: `hyperdx_sessions`
- metrics:
  - `otel_metrics_gauge`
  - `otel_metrics_sum`
  - `otel_metrics_histogram`
  - `otel_metrics_exp_histogram`
  - `otel_metrics_summary`

`otel_traces_trace_id_ts`는 recent local trace + older S3 trace를 합쳐 trace ID별 시작/종료 시각을 계산하는 view다.

## 배포

```bash
kubectl apply -k manifests
```

배포 후 확인:

```bash
kubectl rollout status statefulset/clickhouse
kubectl wait --for=condition=complete job/clickhouse-init --timeout=180s
kubectl logs job/clickhouse-init
```

`clickhouse-init` Job은 아래를 수행한다.

1. keeper와 3개 ClickHouse shard 준비 대기
2. 각 shard에 local / Distributed / S3 hybrid schema 적용
3. ingest 노드에만 S3Queue 및 materialized view 생성
4. cluster shard 수와 주요 객체 생성 여부 검증

## 성능 특성

- 최근 30일 쿼리는 대부분 local shard에서 처리되므로 빠르다.
- 30일 초과 범위를 포함한 쿼리는 S3를 직접 스캔하므로 느릴 수 있다.
- 기간 조건을 최근 30일 내로 제한하면 S3 스캔 없이 local 경로만 사용된다.
- 긴 기간 trace lookup 역시 오래된 구간은 S3를 읽는다.

즉, 이 구성은 **최근 데이터는 빠르게**, **오래된 데이터는 원본 보존 우선**으로 설계된 하이브리드 구조다.

## 로컬 확인

```bash
kubectl port-forward svc/clickhouse-service 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'
```
