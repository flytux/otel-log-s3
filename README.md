# OTel Collector -> MinIO(S3) -> ClickHouse hybrid query

OTel Collector는 계속해서 **MinIO(S3)** 에 원본을 저장하고, ClickHouse는 **3 shard-local MergeTree + Distributed + archive view** 구조로 최근 30일은 로컬에서 빠르게 조회하고 그보다 오래된 구간은 archive 원본을 함께 읽는다.

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
4. `*_archive` view가 archive 원본을 직접 파싱  
5. 최종 노출 객체 중 logs / metrics / sessions 는 **최근 30일 local + 30일 초과 archive** 를 `UNION ALL` 로 합쳐 제공하고, traces 는 UI 안정성을 위해 `otel_traces`(최근 30일 local)와 `otel_traces_hybrid`(최근 30일 local + 30일 초과 archive)로 분리해 제공

즉 UI는 기존 테이블명을 그대로 쓰지만, 내부적으로는 다음처럼 동작한다.

- 최근 30일: shard-local MergeTree + Distributed
- 30일 초과: archive view
- 기간이 30일 경계를 걸치면 두 소스를 함께 조회

## 적재 방식

- `clickhouse-0`만 S3Queue ingest 노드로 동작한다.
- `otel_logs_queue`, `otel_traces_queue`, `otel_metrics_queue`, `hyperdx_sessions_queue` 는 **오직 ingest 노드에만** 생성된다.
- queue의 materialized view는 직접 `*_local_dist` 로 insert 해서 3 shard에 분산 적재한다.
- queue 생성 시 `last_processed_path`를 **현재 시각 기준 30일 전 직전 경로**로 잡아 초기 적재 범위를 최근 30일로 제한한다.
- 이후 새로 들어오는 archive 객체는 계속 로컬 shard로 적재된다.

이렇게 해야 S3Queue를 3개 shard에서 동시에 돌릴 때 생길 수 있는 **중복 소비**를 피할 수 있다.

## 보존 정책

모든 `*_local` 테이블은 30일 TTL을 가진다.

- logs / traces / sessions: `TTL Timestamp + INTERVAL 30 DAY DELETE`
- metrics: `TTL TimeUnix + INTERVAL 30 DAY DELETE`

따라서 로컬 디스크에는 최근 30일만 남고, 오래된 데이터는 archive 원본만 남는다.

## 조회 테이블

UI가 사용하는 최종 객체는 아래와 같다.

- logs: `otel_logs`
- traces (UI-safe recent): `otel_traces`
- trace lookup (UI-safe recent): `otel_traces_trace_id_ts`
- traces (hybrid archive): `otel_traces_hybrid`
- trace lookup (hybrid archive): `otel_traces_hybrid_trace_id_ts`
- sessions: `hyperdx_sessions`
- metrics:
  - `otel_metrics_gauge`
  - `otel_metrics_sum`
  - `otel_metrics_histogram`
  - `otel_metrics_exp_histogram`
  - `otel_metrics_summary`

`otel_traces`와 `otel_traces_trace_id_ts`는 HyperDX/기본 UI가 최근 데이터만 안정적으로 보도록 local trace만 읽는다.
오래된 trace까지 함께 조회해야 하면 `otel_traces_hybrid`와 `otel_traces_hybrid_trace_id_ts`를 사용한다.

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
2. 각 shard에 local / Distributed / archive hybrid schema 적용
3. ingest 노드에만 S3Queue 및 materialized view 생성
4. cluster shard 수와 주요 객체 생성 여부 검증

## 성능 특성

- 최근 30일 쿼리는 대부분 local shard에서 처리되므로 빠르다.
- 30일 초과 범위를 포함한 쿼리는 archive를 직접 스캔하므로 느릴 수 있다.
- 기간 조건을 최근 30일 내로 제한하면 archive 스캔 없이 local 경로만 사용된다.
- 오래된 trace lookup이 필요하면 `otel_traces_hybrid_trace_id_ts`가 archive를 함께 읽는다.

즉, 이 구성은 **최근 데이터는 빠르게**, **오래된 데이터는 원본 보존 우선**으로 설계된 하이브리드 구조다.

## 로컬 확인

```bash
kubectl port-forward svc/clickhouse-service 8123:8123
curl 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT version()'
```
```sql
kubectl -n monitoring exec clickhouse-clickhouse-0-0-0 -- clickhouse-client --user default --password clickhouse --query "<SQL>"

 -- 1) 클러스터가 3 shard인지 확인
 SELECT
   uniqExact(shard_num) AS uniq_shards,
   groupArrayDistinct(concat(toString(shard_num), ':', host_name)) AS members
 FROM system.clusters
 WHERE cluster = 'default';

 -- 2) 로컬 테이블 TTL(30일) 확인
 SHOW CREATE TABLE otel_logs_local;

 -- 3) S3Queue 시작 경계(약 30일 전) 확인
 SHOW CREATE TABLE otel_logs_queue;

 -- 4) 뷰 구성이 기대한 대로 생성됐는지 확인
 SHOW CREATE TABLE otel_logs;

 SHOW CREATE TABLE otel_traces;
 SHOW CREATE TABLE otel_traces_hybrid;

 -- 5) 각 shard 로컬 분산 상태 확인
 SELECT hostName() AS host, count() AS rows FROM otel_logs_local;

 SELECT hostName() AS host, count() AS rows FROM otel_traces_local;

 -- 6) 아무 shard에서든 전체 데이터가 보이는지 확인
 -- cutoff는 고정값으로 넣어야 샤드별 비교가 정확합니다.
 SELECT
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS logs_hybrid_before_cutoff
 FROM otel_logs;

 SELECT
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS traces_recent_before_cutoff
 FROM otel_traces;

 SELECT
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS traces_hybrid_before_cutoff
 FROM otel_traces_hybrid;

 -- 7) 현재 시점에 30일 초과 데이터가 실제로 있는지 확인
 SELECT 'local' AS tier, count() AS rows
 FROM otel_logs_local
 WHERE Timestamp < now64(9) - INTERVAL 30 DAY

 UNION ALL
 SELECT 'hybrid' AS tier, count()
 FROM otel_logs
 WHERE Timestamp < now64(9) - INTERVAL 30 DAY

 UNION ALL
 SELECT 'archive' AS tier, count()
 FROM otel_logs_archive
 WHERE Timestamp < now64(9) - INTERVAL 30 DAY;

 -- 8) 현재 적재가 실제로 진행 중인지 대략 확인
 SELECT * FROM
 (
   SELECT 'otel_logs_local' AS name, count() AS rows FROM otel_logs_local
   UNION ALL
   SELECT 'otel_traces_local', count() FROM otel_traces_local
   UNION ALL
   SELECT 'otel_metrics_gauge_local', count() FROM otel_metrics_gauge_local
   UNION ALL
   SELECT 'otel_metrics_sum_local', count() FROM otel_metrics_sum_local
 ) ORDER BY name;
```
