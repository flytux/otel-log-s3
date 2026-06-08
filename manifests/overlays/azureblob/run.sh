#!/bin/sh
set -eu

hosts="clickhouse-clickhouse-0-0-0.clickhouse-clickhouse-headless clickhouse-clickhouse-1-0-0.clickhouse-clickhouse-headless clickhouse-clickhouse-2-0-0.clickhouse-clickhouse-headless"
ingest_host="clickhouse-clickhouse-0-0-0.clickhouse-clickhouse-headless"
user="default"
password="clickhouse"
opts="--user $user --password $password"

wait_host() {
  host="$1"
  until clickhouse-client --host "$host" $opts --query "SELECT 1" >/dev/null 2>&1; do
    echo "Check host" "$host"
    sleep 2
  done
}

query_host() {
  host="$1"
  clickhouse-client --host "$host" $opts --query "$2"
}

render_cluster_sql() {
  sed -e "s#__AZURE_STORAGE_ACCOUNT_NAME__#${AZURE_STORAGE_ACCOUNT_NAME:-}#g" /scripts/cluster.sql
}

apply_file() {
  host="$1"
  render_cluster_sql | clickhouse-client --host "$host" $opts --multiquery
}

echo "Waiting for ClickHouse Keeper..."
until clickhouse-client --host "$ingest_host" $opts --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do
  sleep 2
done

echo "Waiting for ClickHouse shards..."
for host in $hosts; do
  wait_host "$host"
done

echo "Applying cluster schema..."
for host in $hosts; do
  apply_file "$host" /scripts/cluster.sql
done

echo "Verifying cluster topology..."
shard_count="$(query_host "$ingest_host" "SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster = 'default'")"
if [ "$shard_count" != "3" ]; then
  echo "Expected 3 shard entries in default cluster but found $shard_count"
  exit 1
fi

echo "Verifying OTel tables..."
table_count="$(query_host "$ingest_host" "
  SELECT count()
  FROM system.tables
  WHERE database = currentDatabase()
    AND name IN (
      'otel_logs_local',
      'otel_traces_local',
      'otel_metrics_gauge_local',
      'otel_metrics_sum_local',
      'otel_metrics_histogram_local',
      'otel_metrics_exp_histogram_local',
      'otel_metrics_summary_local'
    )
    AND engine LIKE '%ReplicatedMergeTree%'
")"
if [ "$table_count" != "7" ]; then
  echo "Expected 7 OTel tables but found $table_count"
  query_host "$ingest_host" "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name"
  exit 1
fi

echo "Verifying storage policy..."
policy_count="$(query_host "$ingest_host" "
  SELECT count()
  FROM system.tables
  WHERE database = currentDatabase()
    AND name IN ('otel_logs_local', 'otel_traces_local', 'otel_metrics_gauge_local')
    AND storage_policy = 'tiered'
")"
if [ "$policy_count" != "3" ]; then
  echo "Storage policy 'tiered' not applied to tables (found $policy_count/3)"
  query_host "$ingest_host" "SELECT name, storage_policy FROM system.tables WHERE database = currentDatabase() AND name LIKE 'otel_%'"
  exit 1
fi

echo "Done."
