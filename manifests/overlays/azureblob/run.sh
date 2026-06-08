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
    echo "Check host" $host
    sleep 2
  done
}

query_host() {
  host="$1"
  clickhouse-client --host "$host" $opts --query "$2"
}

apply_file() {
  host="$1"
  file="$2"
  clickhouse-client --host "$host" $opts --multiquery < "$file"
}

echo "Waiting for ClickHouse Keeper..."
until clickhouse-client --host "$ingest_host" $opts --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do
  sleep 2
done

echo "Waiting for ClickHouse shards..."
for host in $hosts; do
  wait_host "$host"
done

sed \
  -e "s#__AZURE_STORAGE_ACCOUNT_URL__#${AZURE_STORAGE_ACCOUNT_URL:-}#g" \
  -e "s#__AZURE_STORAGE_ACCOUNT_NAME__#${AZURE_STORAGE_ACCOUNT_NAME:-}#g" \
  -e "s#__AZURE_CLIENT_ID__#${AZURE_CLIENT_ID:-}#g" \
  -e "s#__AZURE_TENANT_ID__#${AZURE_TENANT_ID:-}#g" \
  /scripts/cluster.sql > /tmp/cluster.sql

echo "Applying cluster schema..."
for host in $hosts; do
  apply_file "$host" /tmp/cluster.sql
done

echo "Verifying cluster topology..."
shard_count="$(query_host "$ingest_host" "SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster = 'default'")"
if [ "$shard_count" != "3" ]; then
  echo "Expected 3 shard entries in default cluster but found $shard_count"
  exit 1
fi

echo "Verifying S3 query views..."
object_count="$(query_host "$ingest_host" "
  SELECT count()
  FROM system.tables
  WHERE database = currentDatabase()
    AND name IN (
      'otel_logs',
      'otel_traces',
      'otel_traces_trace_id_ts',
      'hyperdx_sessions',
      'otel_metrics_gauge',
      'otel_metrics_sum',
      'otel_metrics_histogram',
      'otel_metrics_exp_histogram',
      'otel_metrics_summary'
    )
")"
if [ "$object_count" != "9" ]; then
  echo "Expected 9 S3 query views but found $object_count"
  query_host "$ingest_host" "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND (name LIKE 'otel_%' OR name LIKE 'hyperdx_%') ORDER BY name"
  exit 1
fi

echo "Done."
