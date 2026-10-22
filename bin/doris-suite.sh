#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
readonly doris_version=26.3.0
readonly doris_commit=3a4cae3a56afd5b9a8cbf901aeff129d73a57377
readonly source_root="$repo_root/.flink-suite/doris-$doris_version"
readonly reports="$repo_root/streamfusion-doris/target/doris-suite"
readonly mode="${1:-tests}"
case "$mode" in
  tests|adapters|upstream|benchmark|incremental) ;;
  *) echo "usage: $0 [tests|adapters|upstream|benchmark|incremental]" >&2; exit 64 ;;
esac
if [[ $# -gt 0 ]]; then shift; fi
if [[ "$mode" == incremental && "${SF_DORIS_REUSE:-false}" != true ]]; then
  echo "Incremental upstream tests require SF_DORIS_REUSE=true and a Row Binlog fixture compatible with connector 26.3.0." >&2
  exit 64
fi
mkdir -p "$reports"

if [[ ! -d "$source_root/.git" ]]; then
  git clone --depth 1 --branch "$doris_version" \
    https://github.com/apache/doris-flink-connector.git "$source_root"
fi
if [[ "$(git -C "$source_root" rev-parse HEAD)" != "$doris_commit" ]]; then
  echo "Doris upstream tests must use the pinned $doris_version release." >&2
  exit 1
fi
if [[ -n "$(git -C "$source_root" status --porcelain)" ]]; then
  echo "Doris upstream test checkout must be unchanged." >&2
  exit 1
fi
python3 dev/flink-suite/prepare_doris_tests.py --source-root "$source_root" \
  --output-root "$repo_root/streamfusion-doris/target/upstream-test-sources"
cp streamfusion-doris/target/upstream-test-sources/manifest.json "$reports/upstream-source-manifest.json"

container_name="streamfusion-doris-$RANDOM-$$"
started=false
cleanup() {
  if [[ "$started" == true ]]; then
    docker logs "$container_name" > "$reports/doris.log" 2>&1 || true
    docker rm --force "$container_name" > /dev/null || true
  fi
}
trap cleanup EXIT
if [[ "${SF_DORIS_REUSE:-false}" != true ]]; then
  docker build -t streamfusion-doris-fixture:4.1.4.1 dev/doris
  docker run --detach --name "$container_name" \
    -p 127.0.0.1:8030:8030 -p 127.0.0.1:9030:9030 -p 127.0.0.1:8040:8040 \
    -p 127.0.0.1:9060:9060 -p 127.0.0.1:9611:9611 -p 127.0.0.1:8050:8050 \
    -e FE_HEAP=1024m \
    -e 'FE_CONFIG_EXTRA=arrow_flight_sql_port = 9611' \
    -e $'BE_CONFIG_EXTRA=arrow_flight_sql_port = 8050\nmem_limit = 3G\nstorage_page_cache_limit = 256M' \
    streamfusion-doris-fixture:4.1.4.1 > "$reports/container-id"
  started=true
  healthy=false
  for attempt in $(seq 1 120); do
    state="$(docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' "$container_name")"
    if [[ "$state" == 'running healthy' ]]; then healthy=true; break; fi
    if [[ "$state" != running* ]]; then break; fi
    sleep 2
  done
  if [[ "$healthy" != true ]]; then
    echo "Doris did not become healthy: $state" >&2
    exit 1
  fi
  docker exec "$container_name" mysql -uroot -h127.0.0.1 -P9030 \
    -e "SET PROPERTY FOR 'root' 'max_user_connections' = '1024';"
fi

if [[ "$mode" == benchmark ]]; then
  SF_DORIS_BENCH=true mvn -B -ntp -Pbench,doris,doris-it -pl streamfusion-doris -am test \
    -Dsf.testForks=1 -Dnative.build.skip="${SF_DORIS_NATIVE_BUILD_SKIP:-false}" \
    -Dsurefire.failIfNoSpecifiedTests=false -Dtest=DorisConnectorBenchmark \
    '-Dsf.extraJvmArgs=-Xmx2g -Dsf.doris.benchmark.profile=release -Duser.timezone=UTC' "$@" \
    2>&1 | tee "$reports/benchmark.log"
  exit
fi

profiles=doris,doris-it
upstream_profiles=doris,doris-upstream
if [[ "${SF_DORIS_RELEASE:-false}" == true ]]; then
  profiles=bench,$profiles
  upstream_profiles=bench,$upstream_profiles
fi
build_args=(-B -ntp -P"$profiles" -pl streamfusion-doris -am
  -Dsf.testForks=1 -Dnative.build.skip="${SF_DORIS_NATIVE_BUILD_SKIP:-false}"
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g)
if [[ "$mode" != upstream && "$mode" != incremental ]]; then
  mvn "${build_args[@]}" test \
    '-Dtest=Doris*Test,DorisSqlIT' 2>&1 | tee "$reports/adapter-tests.log"
fi
if [[ "$mode" == adapters ]]; then exit; fi

mvn -B -ntp -f dev/flink-suite/agent/pom.xml package -DskipTests \
  -Dmaven.compiler.source=17 -Dmaven.compiler.target=17
readonly agent="$repo_root/dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
upstream_classes=(
  org.apache.doris.flink.source.DorisSourceITCase
  org.apache.doris.flink.sink.DorisSinkITCase
  org.apache.doris.flink.catalog.DorisCatalogITCase
  org.apache.doris.flink.lookup.DorisLookupTableITCase
  org.apache.doris.flink.table.DorisRowDataJdbcLookupFunctionITCase)
if [[ "$mode" == incremental ]]; then
  upstream_classes=(org.apache.doris.flink.source.DorisIncrementalSourceITCase
    org.apache.doris.flink.source.DorisIncrementalSourceFailoverITCase)
fi
readonly selection="$(IFS=,; echo "${upstream_classes[*]}")"
readonly upstream_run="$(mktemp -d "$reports/upstream.XXXXXX")"
if [[ "$mode" != incremental ]]; then
  mvn -B -ntp -P"$upstream_profiles" -pl streamfusion-doris -am test \
    -Dsf.testForks=1 -Dnative.build.skip="${SF_DORIS_NATIVE_BUILD_SKIP:-false}" \
    -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.test.failure.ignore=true \
    -Ddoris.upstream.reports="$upstream_run/stock-baseline" \
    -Dtest='DorisSourceITCase#testTableSourceTimestampFilterAndProjectionPushDown' \
    '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=UTC' \
    -Dcustomer_env=true -Ddoris_host=127.0.0.1 -Ddoris_query_port=9030 \
    -Ddoris_http_port=8030 -Ddoris_user=root -Ddoris_passwd= \
    2>&1 | tee "$reports/stock-timestamp-baseline.log"
fi
readonly agent_args="-Xmx2g -javaagent:$agent -Dstreamfusion.flink-suite.native-reports=$upstream_run/native-reports -Dstreamfusion.flink-suite.sql-inventory=$upstream_run/sql-inventory -Duser.timezone=UTC"
mvn -B -ntp -P"$upstream_profiles" -pl streamfusion-doris -am test \
  -Dsf.testForks=1 -Dnative.build.skip="${SF_DORIS_NATIVE_BUILD_SKIP:-false}" \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.test.failure.ignore=true \
  -Ddoris.upstream.reports="$upstream_run/surefire-reports" \
  -Dtest="$selection" -Dsf.extraJvmArgs="$agent_args" \
  -Dcustomer_env=true -Ddoris_host=127.0.0.1 -Ddoris_query_port=9030 \
  -Ddoris_http_port=8030 -Ddoris_user=root -Ddoris_passwd= \
  2>&1 | tee "$reports/upstream-tests.log"

if ! grep -q 'StreamFusion enabled for upstream Flink streaming planner tests' "$reports/upstream-tests.log"; then
  echo "The upstream test run did not activate StreamFusion." >&2
  exit 1
fi
audit_classes=()
if [[ "$mode" != incremental ]]; then
  audit_classes+=(--require-contract-prefix org.apache.doris.flink.source.DorisSourceITCase)
  expected_timestamp="$(python3 dev/flink-suite/doris_timestamp_baseline.py \
    "$upstream_run/stock-baseline" "$upstream_run/surefire-reports")"
  if [[ -n "$expected_timestamp" ]]; then audit_classes+=(--xfail "$expected_timestamp"); fi
fi
for test_class in "${upstream_classes[@]}"; do audit_classes+=(--require-test-class "$test_class"); done
python3 dev/flink-suite/summarize.py "$upstream_run/surefire-reports" \
  --sql-inventory "$upstream_run/sql-inventory" --native-reports "$upstream_run/native-reports" --flink-line 2.2 \
  --audit-output "$upstream_run/execution-audit.json" \
  "${audit_classes[@]}"
