# OTel Collector -> Object Storage -> ClickHouse

OTel Collector는 계속해서 object storage에 원본을 저장하고, ClickHouse는 **3 shard-local MergeTree + Distributed** 구조로 조회를 처리한다. Azure Blob overlay에서는 logs / traces 를 ClickHouse로 전량 적재해, 조회 시 Azure Blob의 원본 JSON을 다시 직접 파싱하지 않는다.

## 구성 요소

| 컴포넌트 | 역할 | 엔드포인트 |
| --- | --- | --- |
| OTel Collector | OTLP 수신 후 object storage 컨테이너별 저장 | `otel-collector-service:4317`, `:4318` |
| Azure Blob Storage | 원본 저장소 | `<storage-account-url>` |
| ClickHouse | 3-shard 로컬 적재 + 분산 조회 | `http://clickhouse-service:8123` |
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

`manifests/base/clickhouse-version.yaml`이 ClickHouse 이미지 버전을 관리하고, overlay별 SQL/patch가 각 저장소 연동 방식을 정의한다.

1. `clickhouse` StatefulSet 3개 pod (`clickhouse-0..2`)  
2. shard마다 `*_local` MergeTree 테이블 보유  
3. `*_local_dist` Distributed 테이블로 3 shard 조회 분산  
4. logs / traces 는 AzureQueue가 object storage 원본을 읽어 ClickHouse 테이블로 계속 적재  
5. metrics / sessions 는 최근 30일만 local에 유지하고, 오래된 구간은 archive view를 통해 직접 읽음

즉 UI는 기존 테이블명을 그대로 쓰지만, 내부적으로는 다음처럼 동작한다.

- logs / traces: 전체 기간을 shard-local MergeTree + Distributed에서 조회
- metrics / sessions: 최근 30일은 local, 30일 초과는 archive view 조회

## 적재 방식

- `clickhouse-0`만 AzureQueue ingest 노드로 동작한다.
- `otel_logs_queue`, `otel_traces_queue`, `otel_metrics_queue`, `hyperdx_sessions_queue` 는 **오직 ingest 노드에만** 생성된다.
- queue의 materialized view는 직접 `*_local_dist` 로 insert 해서 3 shard에 분산 적재한다.
- Azure Blob 인증은 SQL에서 `use_workload_identity = 1`과 `client_id` / `tenant_id`를 함께 설정해 Workload Identity를 사용한다.
- logs / traces queue는 `last_processed_path` 없이 시작해 초기 배포 시 저장된 전체 이력을 backfill한다.
- metrics / sessions queue는 `last_processed_path`를 **현재 시각 기준 30일 전 직전 경로**로 잡아 초기 적재 범위를 최근 30일로 제한한다.
- 이후 새로 들어오는 archive 객체는 계속 로컬 shard로 적재된다.

이렇게 해야 AzureQueue를 3개 shard에서 동시에 돌릴 때 생길 수 있는 **중복 소비**를 피할 수 있다.

## 보존 정책

logs / traces 는 `*_local` 테이블에 전체 이력을 유지하고, metrics / sessions 만 30일 TTL을 가진다.

- sessions: `TTL Timestamp + INTERVAL 30 DAY DELETE`
- metrics: `TTL TimeUnix + INTERVAL 30 DAY DELETE`

따라서 logs / traces 조회는 항상 ClickHouse 테이블에서 처리되고, metrics / sessions 만 오래된 데이터가 archive 원본으로 남는다.

## 조회 테이블

UI가 사용하는 최종 객체는 아래와 같다.

- logs: `otel_logs`
- traces: `otel_traces`
- trace lookup: `otel_traces_trace_id_ts`
- traces (compat alias): `otel_traces_hybrid`
- trace lookup (compat alias): `otel_traces_hybrid_trace_id_ts`
- sessions: `hyperdx_sessions`
- metrics:
  - `otel_metrics_gauge`
  - `otel_metrics_sum`
  - `otel_metrics_histogram`
  - `otel_metrics_exp_histogram`
  - `otel_metrics_summary`

`otel_logs`, `otel_traces`, `otel_traces_hybrid`는 모두 ClickHouse에 적재된 shard-local 데이터를 읽는다. `otel_traces_hybrid*`는 기존 조회 경로와의 호환성을 위해 유지한다.

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
3. ingest 노드에만 AzureQueue 및 materialized view 생성
4. cluster shard 수와 주요 객체 생성 여부 검증

## 성능 특성

- logs / traces 쿼리는 전체 기간에 대해 local shard에서 처리되므로 archive JSON 재파싱 비용이 없다.
- 초기 배포나 schema 재적용 시에는 logs / traces 전체 backfill 때문에 ingest 시간이 더 오래 걸릴 수 있다.
- metrics / sessions 는 30일 초과 범위를 조회하면 archive 직접 스캔 비용이 남아 있다.

즉, 이 구성은 **logs / traces 는 조회 성능 우선으로 전량 적재**, **metrics / sessions 는 원본 보존 우선 하이브리드**로 분리한 구조다.

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

 -- 2) logs / traces 테이블이 TTL 없이 유지되는지 확인
 SHOW CREATE TABLE otel_logs_local;
 SHOW CREATE TABLE otel_traces_local;

 -- 3) queue bootstrap 정책 확인
 SHOW CREATE TABLE otel_logs_queue;
 SHOW CREATE TABLE otel_traces_queue;
 SHOW CREATE TABLE hyperdx_sessions_queue;

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
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS logs_before_cutoff
 FROM otel_logs;

 SELECT
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS traces_before_cutoff
 FROM otel_traces;

 SELECT
   countIf(Timestamp < toDateTime64('2026-06-07 03:39:00', 9, 'UTC')) AS traces_hybrid_before_cutoff
 FROM otel_traces_hybrid;

 -- 7) logs / traces 의 30일 초과 데이터가 실제로 로컬에 적재됐는지 확인
 SELECT 'logs_local' AS tier, count() AS rows
 FROM otel_logs_local
 WHERE Timestamp < now64(9) - INTERVAL 30 DAY

 UNION ALL
 SELECT 'traces_local' AS tier, count()
 FROM otel_traces_local
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
