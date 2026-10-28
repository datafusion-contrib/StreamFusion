#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
readonly connector_version=1.2.16
readonly connector_commit=0657e0dbf24951ecad1bb9ee1800bfa86e7991a5
readonly image='starrocks/allin1-ubuntu:4.1.6@sha256:5cbf09c4c788d2ca41a8e38599b650f04dbc6af0de51ebda0952770fb8333e5d'
readonly source_root="$repo_root/.flink-suite/starrocks-connector-$connector_version"
readonly reports="$repo_root/streamfusion-starrocks/target/starrocks-suite"
readonly contracts="$repo_root/dev/flink-suite/agent/src/main/resources/native-execution-flink1.18.tsv"
readonly upstream_sink_test=com.starrocks.connector.flink.it.sink.StarRocksSinkITTest
readonly upstream_kafka_test=com.starrocks.connector.flink.it.sink.kafka.KafkaToStarRocksITTest
readonly mode="${1:-tests}"
case "$mode" in
  tests|adapters|upstream|benchmark|fixture) ;;
  *) echo "usage: $0 [tests|adapters|upstream|benchmark|fixture]" >&2; exit 64 ;;
esac
if [[ $# -gt 0 ]]; then shift; fi
mkdir -p "$reports"

container_name="streamfusion-starrocks-$RANDOM-$$"
if [[ "$mode" == fixture ]]; then container_name=streamfusion-starrocks-fixture; fi
started=false
stop_fixture() {
  if [[ "$started" == true ]]; then
    docker logs "$container_name" > "$reports/starrocks.log" 2>&1 || true
    docker rm --force "$container_name" > /dev/null || true
    started=false
  fi
}
cleanup() {
  if [[ "$mode" != fixture ]]; then stop_fixture; fi
}
trap cleanup EXIT
# The released connector ITs start their own server on fixed host ports, so the upstream phase
# runs without this fixture. SF_STARROCKS_REUSE keeps an already running fixture for the others.
if [[ "$mode" != upstream && "${SF_STARROCKS_REUSE:-false}" != true ]]; then
  docker run --detach --name "$container_name" \
    -p 127.0.0.1:8030:8030 -p 127.0.0.1:9030:9030 -p 127.0.0.1:8040:8040 -p 127.0.0.1:9060:9060 \
    --memory "${SF_STARROCKS_MEMORY:-6g}" "$image" > "$reports/container-id"
  started=true
  healthy=false
  for attempt in $(seq 1 120); do
    state="$(docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' "$container_name")"
    if [[ "$state" == 'running healthy' ]] && docker exec "$container_name" \
        mysql -uroot -h127.0.0.1 -P9030 -N -e 'SHOW BACKENDS' 2>/dev/null | grep -q true; then
      healthy=true
      break
    fi
    if [[ "$state" != running* ]]; then break; fi
    sleep 2
  done
  if [[ "$healthy" != true ]]; then
    echo "StarRocks did not become healthy: $state" >&2
    exit 1
  fi
fi
if [[ "$mode" == fixture ]]; then
  echo "StarRocks fixture $container_name is running; remove it with: docker rm -f $container_name"
  exit
fi

native_skip="${SF_STARROCKS_NATIVE_BUILD_SKIP:-false}"
if [[ "$mode" == benchmark ]]; then
  SF_STARROCKS_BENCH=true mvn -B -ntp -Pbench,flink-1.18,starrocks,starrocks-it \
    -pl streamfusion-starrocks -am test -Dsf.testForks=1 -Dnative.build.skip="$native_skip" \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=StarRocksEncodeBenchmark,StarRocksConnectorBenchmark \
    '-Dsf.extraJvmArgs=-Xmx4g -Duser.timezone=UTC -Dsf.starrocks.benchmark.profile=release' \
    "$@" 2>&1 | tee "$reports/benchmark.log"
  exit
fi

if [[ "$mode" != upstream ]]; then
  mvn -B -ntp -Pflink-1.18,starrocks,starrocks-it -pl streamfusion-starrocks -am test \
    -Dsf.testForks=1 -Dnative.build.skip="$native_skip" -Dsurefire.failIfNoSpecifiedTests=false \
    '-Dtest=StarRocks*Test,StarRocksSqlIT' '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=UTC' \
    2>&1 | tee "$reports/adapter-tests.log"
fi
if [[ "$mode" == adapters ]]; then exit; fi

stop_fixture
for port in 8030 9030 8040 9060 8060; do
  if (exec 3<> "/dev/tcp/127.0.0.1/$port") 2> /dev/null; then
    echo "Port $port is in use. The released StarRocks ITs start the pinned image on fixed ports;" \
      "stop the process or fixture holding it first." >&2
    exit 1
  fi
done

if [[ ! -d "$source_root/.git" ]]; then
  git clone --depth 1 --branch "v$connector_version" \
    https://github.com/StarRocks/starrocks-connector-for-apache-flink.git "$source_root"
fi
if [[ "$(git -C "$source_root" rev-parse HEAD)" != "$connector_commit" ]]; then
  echo "StarRocks upstream tests must use the pinned v$connector_version release." >&2
  exit 1
fi
if [[ -n "$(git -C "$source_root" status --porcelain)" ]]; then
  echo "StarRocks upstream test checkout must be unchanged." >&2
  exit 1
fi
python3 dev/flink-suite/prepare_starrocks_tests.py --source-root "$source_root" \
  --output-root "$repo_root/streamfusion-starrocks/target/upstream-test-sources" \
  --resource-root "$repo_root/streamfusion-starrocks/target/upstream-test-resources"
cp streamfusion-starrocks/target/upstream-test-sources/manifest.json \
  "$reports/upstream-source-manifest.json"

mvn -B -ntp -f dev/flink-suite/agent/pom.xml package -DskipTests \
  -Dmaven.compiler.source=17 -Dmaven.compiler.target=17
readonly agent="$repo_root/dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
readonly upstream_run="$(mktemp -d "$reports/upstream.XXXXXX")"
readonly upstream_server="-Dit.starrocks.image=$image"
readonly agent_args="-Xmx2g -javaagent:$agent -Dstreamfusion.flink-suite.flink-line=1.18 -Dstreamfusion.flink-suite.native-reports=$upstream_run/native-reports -Dstreamfusion.flink-suite.sql-inventory=$upstream_run/sql-inventory -Duser.timezone=UTC $upstream_server"
mvn -B -ntp -Pflink-1.18,starrocks,starrocks-upstream -pl streamfusion-starrocks -am test \
  -Dsf.testForks=1 -Dnative.build.skip="$native_skip" -Dsurefire.failIfNoSpecifiedTests=false \
  -Dmaven.test.failure.ignore=true -Dstarrocks.upstream.reports="$upstream_run/surefire-reports" \
  -Dtest="$upstream_sink_test,$upstream_kafka_test" -Dsf.extraJvmArgs="$agent_args" \
  2>&1 | tee "$reports/upstream-tests.log"

if ! grep -q 'StreamFusion enabled for upstream Flink streaming planner tests' "$reports/upstream-tests.log"; then
  echo "The upstream test run did not activate StreamFusion." >&2
  exit 1
fi
python3 dev/flink-suite/summarize.py "$upstream_run/surefire-reports" --contracts "$contracts" \
  --sql-inventory "$upstream_run/sql-inventory" --native-reports "$upstream_run/native-reports" \
  --flink-line 1.18 --audit-output "$upstream_run/execution-audit.json" \
  --require-contract-prefix "$upstream_sink_test#" --require-test-class "$upstream_sink_test" \
  --require-contract-prefix "$upstream_kafka_test#" --require-test-class "$upstream_kafka_test"
