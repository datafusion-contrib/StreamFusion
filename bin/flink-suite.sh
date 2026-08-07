#!/usr/bin/env bash

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FLINK_VERSION="${FLINK_VERSION:-2.2.1}"
readonly FLINK_TAG="release-${FLINK_VERSION}"
readonly KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION:-5.0.0}"
readonly KAFKA_CONNECTOR_TAG="v${KAFKA_CONNECTOR_VERSION}"
readonly SUITE_ROOT="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}"
readonly FLINK_ROOT="${SUITE_ROOT}/flink-${FLINK_VERSION}"
readonly KAFKA_CONNECTOR_ROOT="${SUITE_ROOT}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}"
readonly STREAMFUSION_BUILD_ROOT="${SUITE_ROOT}/streamfusion-source"
readonly AGENT_ROOT="${REPO_ROOT}/dev/flink-suite/agent"
readonly AGENT_JAR="${AGENT_ROOT}/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
readonly CLASSPATH_FILE="${SUITE_ROOT}/streamfusion-classpath.txt"
readonly MAVEN_SETTINGS="${REPO_ROOT}/dev/flink-suite/settings.xml"
readonly SUITE_MAVEN_REPO="${SUITE_ROOT}/m2"
readonly UNSHADED_PLANNER_JAR="${SUITE_ROOT}/flink-table-planner-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_PLANNER_POM="${SUITE_ROOT}/flink-table-planner-${FLINK_VERSION}-effective.pom"
readonly UNSHADED_BRIDGE_JAR="${SUITE_ROOT}/flink-table-calcite-bridge-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_BRIDGE_POM="${SUITE_ROOT}/flink-table-calcite-bridge-${FLINK_VERSION}-effective.pom"
readonly UNSHADED_SQL_PARSER_JAR="${SUITE_ROOT}/flink-sql-parser-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_SQL_PARSER_POM="${SUITE_ROOT}/flink-sql-parser-${FLINK_VERSION}-effective.pom"
readonly SUITE_MODE="${1:-runtime}"
readonly FLINK_MODULE_CONFIG="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Djunit.platform.reflection.search.useLegacySemantics=true -javaagent:${AGENT_JAR}"
readonly FORMAT_MODULES="flink-formats/flink-json,flink-formats/flink-csv,flink-formats/flink-avro,flink-formats/flink-avro-confluent-registry,flink-formats/flink-protobuf"
readonly KAFKA_SQL_TESTS="org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaTableITCase,org.apache.flink.streaming.connectors.kafka.table.KafkaChangelogTableITCase,org.apache.flink.streaming.connectors.kafka.table.KafkaTableITCase,org.apache.flink.streaming.connectors.kafka.table.UpsertKafkaTableITCase"
readonly ROCKSDB_STATE_SQL_TESTS="org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.DeduplicateITCase,org.apache.flink.table.planner.runtime.stream.sql.GroupWindowITCase,org.apache.flink.table.planner.runtime.stream.sql.IntervalJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.JoinITCase,org.apache.flink.table.planner.runtime.stream.sql.OverAggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.RankITCase,org.apache.flink.table.planner.runtime.stream.sql.TemporalJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowAggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowDeduplicateITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowRankITCase,org.apache.flink.table.planner.runtime.stream.table.AggregateITCase,org.apache.flink.table.planner.runtime.stream.table.JoinITCase,org.apache.flink.table.planner.runtime.stream.table.OverAggregateITCase,org.apache.flink.table.planner.runtime.stream.table.RetractionITCase"
TEST_SELECTOR_ARGS=()
if [[ -n "${FLINK_SUITE_TEST:-}" ]]; then
  TEST_SELECTOR_ARGS=("-Dtest=${FLINK_SUITE_TEST}")
fi

case "${SUITE_MODE}" in
  runtime)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="flink-table/flink-table-planner"
    REPORT_ROOT="${FLINK_ROOT}/flink-table/flink-table-planner/target/surefire-reports"
    ;;
  diagnostic)
    TEST_GOAL="integration-test"
    TEST_MODULES="flink-table/flink-table-planner"
    REPORT_ROOT="${FLINK_ROOT}/flink-table/flink-table-planner/target/surefire-reports"
    ;;
  state)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="flink-table/flink-table-planner"
    REPORT_ROOT="${FLINK_ROOT}/flink-table/flink-table-planner/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      TEST_SELECTOR_ARGS=("-Dtest=${ROCKSDB_STATE_SQL_TESTS}")
    fi
    ;;
  formats)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="${FORMAT_MODULES}"
    REPORT_ROOT="${FLINK_ROOT}/flink-formats"
    ;;
  kafka)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="flink-connector-kafka"
    REPORT_ROOT="${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      TEST_SELECTOR_ARGS=("-Dtest=${KAFKA_SQL_TESTS}")
    fi
    ;;
  all)
    "${BASH_SOURCE[0]}" formats || exit $?
    FLINK_SUITE_REUSE_BUILD=true "${BASH_SOURCE[0]}" runtime || exit $?
    "${BASH_SOURCE[0]}" kafka
    exit $?
    ;;
  *)
    echo "Usage: $0 [runtime|diagnostic|state|formats|kafka|all]" >&2
    exit 2
    ;;
esac

mkdir -p "${SUITE_ROOT}"
if [[ ! -d "${FLINK_ROOT}/.git" ]]; then
  git clone --depth 1 --branch "${FLINK_TAG}" https://github.com/apache/flink.git "${FLINK_ROOT}" || exit $?
fi

if [[ -n "$(git -C "${FLINK_ROOT}" status --short)" ]]; then
  echo "The upstream Flink checkout is not clean: ${FLINK_ROOT}" >&2
  echo "Use a new FLINK_SUITE_ROOT or clean that disposable checkout manually." >&2
  exit 2
fi

if [[ "${SUITE_MODE}" == "kafka" ]]; then
  if [[ ! -d "${KAFKA_CONNECTOR_ROOT}/.git" ]]; then
    git clone --depth 1 --branch "${KAFKA_CONNECTOR_TAG}" \
      https://github.com/apache/flink-connector-kafka.git "${KAFKA_CONNECTOR_ROOT}" || exit $?
  fi
  if [[ -n "$(git -C "${KAFKA_CONNECTOR_ROOT}" status --short)" ]]; then
    echo "The upstream Kafka connector checkout is not clean: ${KAFKA_CONNECTOR_ROOT}" >&2
    echo "Use a new FLINK_SUITE_ROOT or clean that disposable checkout manually." >&2
    exit 2
  fi
fi

if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" == "true" ]]; then
  for required in "${AGENT_JAR}" "${CLASSPATH_FILE}" "${UNSHADED_PLANNER_JAR}"; do
    if [[ ! -f "${required}" ]]; then
      echo "Cannot reuse the suite build; missing artifact: ${required}" >&2
      exit 2
    fi
  done
  if [[ "${SUITE_MODE}" == "formats" ]] \
      && [[ ! -f "${FLINK_ROOT}/flink-formats/flink-csv/target/test-classes/org/apache/flink/formats/csv/TableCsvFormatITCase.class" ]]; then
    echo "Cannot reuse the format-suite build; run bin/flink-suite.sh formats once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${SUITE_MODE}" == "kafka" ]] \
      && [[ ! -f "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/target/test-classes/org/apache/flink/streaming/connectors/kafka/table/KafkaTableITCase.class" ]]; then
    echo "Cannot reuse the Kafka-suite build; run bin/flink-suite.sh kafka once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  echo "Reusing the existing Flink suite and StreamFusion build artifacts..."
else
  echo "Building the test-JVM planner injection agent..."
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${AGENT_ROOT}/pom.xml" package || exit $?

  echo "Building the pinned Flink planner and its reactor dependencies..."
  "${FLINK_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -pl flink-table/flink-table-planner -am -DskipTests -Dfast install || exit $?

  echo "Installing the untouched planner classes for StreamFusion's source-suite build..."
  jar --create --file "${UNSHADED_SQL_PARSER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-sql-parser/target/classes" . || exit $?
  "${FLINK_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -pl flink-table/flink-sql-parser \
    help:effective-pom -Doutput="${UNSHADED_SQL_PARSER_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_SQL_PARSER_JAR}" \
    -DpomFile="${UNSHADED_SQL_PARSER_POM}" || exit $?
  jar --create --file "${UNSHADED_BRIDGE_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-calcite-bridge/target/classes" . || exit $?
  "${FLINK_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -pl flink-table/flink-table-calcite-bridge \
    help:effective-pom -Doutput="${UNSHADED_BRIDGE_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_BRIDGE_JAR}" \
    -DpomFile="${UNSHADED_BRIDGE_POM}" || exit $?
  jar --create --file "${UNSHADED_PLANNER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-planner/target/classes" . || exit $?
  "${FLINK_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -pl flink-table/flink-table-planner \
    help:effective-pom -Doutput="${UNSHADED_PLANNER_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_PLANNER_JAR}" \
    -DpomFile="${UNSHADED_PLANNER_POM}" || exit $?

  echo "Copying StreamFusion sources into the isolated suite build..."
  mkdir -p "${STREAMFUSION_BUILD_ROOT}"
  rsync -a --delete \
    --exclude='.git' \
    --exclude='.flink-suite' \
    --exclude='target' \
    "${REPO_ROOT}/" "${STREAMFUSION_BUILD_ROOT}/" || exit $?

  echo "Building and installing StreamFusion and its supported connector/format modules against the source-suite planner..."
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -Dstreamfusion.flink-source-suite \
    -f "${STREAMFUSION_BUILD_ROOT}/pom.xml" \
    -pl :streamfusion-core,:streamfusion-kafka,:streamfusion-json,:streamfusion-csv,:streamfusion-raw,:streamfusion-avro,:streamfusion-avro-confluent-registry,:streamfusion-protobuf \
    -am -DskipTests clean install || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -f "${REPO_ROOT}/dev/flink-suite/classpath-pom.xml" \
    dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" || exit $?

  if [[ "${SUITE_MODE}" == "formats" ]]; then
    echo "Compiling the untouched upstream Flink format integration tests..."
    "${FLINK_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
      -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
      -pl "${FORMAT_MODULES}" -am -Dfast -DskipTests process-test-classes || exit $?
  fi

  if [[ "${SUITE_MODE}" == "kafka" ]]; then
    echo "Compiling the untouched upstream Kafka connector SQL integration tests..."
    "${KAFKA_CONNECTOR_ROOT}/mvnw" -B -ntp -s "${MAVEN_SETTINGS}" \
      -f "${KAFKA_CONNECTOR_ROOT}/pom.xml" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
      -Dflink.version="${FLINK_VERSION}" -pl flink-connector-kafka \
      -DskipTests test-compile || exit $?
  fi
fi
readonly STREAMFUSION_CLASSPATH="$(tr ':' ',' < "${CLASSPATH_FILE}")"

echo "Running the upstream Flink ${SUITE_MODE} suite with StreamFusion enabled..."
mkdir -p "${REPORT_ROOT}"
if [[ "${SUITE_MODE}" == "formats" ]]; then
  find "${REPORT_ROOT}" -type f -path '*/target/surefire-reports/*' -delete
else
  find "${REPORT_ROOT}" -mindepth 1 -maxdepth 1 -type f -delete
fi
MAVEN_TEST_ARGS=(
  -B -ntp -s "${MAVEN_SETTINGS}" \
  -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
  -pl "${TEST_MODULES}" \
  -Dmaven.test.additionalClasspath="${STREAMFUSION_CLASSPATH}" \
  -Dstreamfusion.logFallbackReasons=true \
  -Dstreamfusion.native.development=true \
  -Dfast \
  -Djunit.jupiter.execution.parallel.enabled=false \
  -Dflink.forkCountUnitTest="${FLINK_SUITE_UNIT_FORKS:-2}" \
  -Dflink.forkCountITCase="${FLINK_SUITE_IT_FORKS:-1}"
)
if [[ "${SUITE_MODE}" == "state" ]]; then
  MAVEN_TEST_ARGS+=("-Dstreamfusion.flink-suite.native-rocksdb=true")
fi
if [[ "${SUITE_MODE}" == "kafka" ]]; then
  MAVEN_TEST_ARGS+=(
    -f "${KAFKA_CONNECTOR_ROOT}/pom.xml"
    -Dflink.version="${FLINK_VERSION}"
    -Dflink.surefire.baseArgLine="${FLINK_MODULE_CONFIG}"
  )
else
  MAVEN_TEST_ARGS+=(
    -f "${FLINK_ROOT}/pom.xml"
    -Dsurefire.module.config="${FLINK_MODULE_CONFIG}"
  )
fi
if [[ ${#TEST_SELECTOR_ARGS[@]} -gt 0 ]]; then
  MAVEN_TEST_ARGS+=("${TEST_SELECTOR_ARGS[@]}")
  if [[ "${SUITE_MODE}" == "formats" ]]; then
    MAVEN_TEST_ARGS+=("-Dsurefire.failIfNoSpecifiedTests=false")
  fi
fi
MAVEN_TEST_ARGS+=("${TEST_GOAL}")
if [[ "${SUITE_MODE}" == "kafka" ]]; then
  "${KAFKA_CONNECTOR_ROOT}/mvnw" "${MAVEN_TEST_ARGS[@]}"
else
  "${FLINK_ROOT}/mvnw" "${MAVEN_TEST_ARGS[@]}"
fi
readonly TEST_STATUS=$?

if [[ "${SUITE_MODE}" == "state" && ${TEST_STATUS} -eq 0 ]]; then
  for required_marker in \
    "StreamFusion enabled for upstream Flink streaming planner tests" \
    "StreamFusion upstream state suite exercised Flink heap backend" \
    "StreamFusion upstream state suite initialized native memory backend" \
    "StreamFusion upstream state suite installed native RocksDB backend"; do
    if ! grep -RqsF "${required_marker}" "${REPORT_ROOT}"; then
      echo "The upstream state suite did not prove: ${required_marker}" >&2
      exit 1
    fi
  done
fi

SUMMARY_ARGS=("${REPORT_ROOT}")
if [[ "${SUITE_MODE}" == "runtime" || "${SUITE_MODE}" == "diagnostic" ]]; then
  SUMMARY_ARGS+=(--xfail "org.apache.flink.table.planner.runtime.batch.sql.CalcITCase#testCurrentDate")
fi
python3 "${REPO_ROOT}/dev/flink-suite/summarize.py" "${SUMMARY_ARGS[@]}"
readonly SUMMARY_STATUS=$?

if [[ ${TEST_STATUS} -ne 0 && ${SUMMARY_STATUS} -ne 0 ]]; then
  exit "${TEST_STATUS}"
fi
exit "${SUMMARY_STATUS}"
