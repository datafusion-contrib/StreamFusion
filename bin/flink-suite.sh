#!/usr/bin/env bash

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FLINK_VERSION="${FLINK_VERSION:-2.2.1}"
readonly FLINK_TAG="release-${FLINK_VERSION}"
case "${FLINK_VERSION}" in
  2.2.0|2.2.1)
    FLINK_LINE=2.2
    STREAMFUSION_LINE_PROFILES=()
    STREAMFUSION_ARTIFACT_SUFFIX=""
    KAFKA_DEFAULT_VERSION=5.0.0
    PAIMON_FLINK_PROFILE=flink2
    ;;
  1.18.1)
    FLINK_LINE=1.18
    STREAMFUSION_LINE_PROFILES=(-Pflink-1.18)
    STREAMFUSION_ARTIFACT_SUFFIX=-flink1.18
    KAFKA_DEFAULT_VERSION=3.2.0
    PAIMON_FLINK_PROFILE=flink1
    ;;
  *) echo "Unsupported Flink suite version: ${FLINK_VERSION}" >&2; exit 2 ;;
esac
readonly FLINK_LINE STREAMFUSION_ARTIFACT_SUFFIX KAFKA_DEFAULT_VERSION PAIMON_FLINK_PROFILE
readonly KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION:-${KAFKA_DEFAULT_VERSION}}"
# The published 3.2.0 source archive matches the final candidate; no v3.2.0 tag exists.
if [[ "${KAFKA_CONNECTOR_VERSION}" == "3.2.0" ]]; then
  KAFKA_CONNECTOR_TAG=v3.2.0-rc1
else
  KAFKA_CONNECTOR_TAG="v${KAFKA_CONNECTOR_VERSION}"
fi
readonly KAFKA_CONNECTOR_TAG
readonly PAIMON_VERSION="${PAIMON_VERSION:-2.0.0}"
# Paimon publishes its releases from the final release-candidate tag; 2.0.0 is release-2.0.0-rc10.
readonly PAIMON_TAG="${PAIMON_TAG:-release-${PAIMON_VERSION}-rc10}"
readonly DELTA_VERSION="${DELTA_VERSION:-4.4.0}"
readonly SUITE_BASE_ROOT="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}"
readonly SUITE_ROOT="${SUITE_BASE_ROOT}/${FLINK_LINE}"
readonly FLINK_ROOT="${SUITE_ROOT}/flink-${FLINK_VERSION}"
readonly KAFKA_CONNECTOR_ROOT="${SUITE_ROOT}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}"
readonly PAIMON_ROOT="${SUITE_ROOT}/paimon-${PAIMON_VERSION}"
readonly PAIMON_MODULE="paimon-flink/paimon-flink-common"
readonly DELTA_ROOT="${SUITE_ROOT}/delta-${DELTA_VERSION}"
readonly DELTA_TEST_POM="${REPO_ROOT}/dev/flink-suite/delta/pom.xml"
readonly DELTA_TEST_OUTPUT="${SUITE_ROOT}/delta-tests/target"
readonly STREAMFUSION_BUILD_ROOT="${SUITE_ROOT}/streamfusion-source"
readonly AGENT_ROOT="${REPO_ROOT}/dev/flink-suite/agent"
readonly AGENT_OUTPUT="${SUITE_ROOT}/agent/target"
readonly AGENT_JAR="${AGENT_OUTPUT}/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
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
CONTRACT_SUFFIX="${STREAMFUSION_ARTIFACT_SUFFIX}"
NATIVE_STATE_SUITE=false
if [[ "${SUITE_MODE}" == "state" ]]; then NATIVE_STATE_SUITE=true; fi
if [[ "${FLINK_LINE}" == "1.18" && "${SUITE_MODE}" == "state" ]]; then
  CONTRACT_SUFFIX="${CONTRACT_SUFFIX}-state"
fi
readonly CONTRACT_FILE="${AGENT_ROOT}/src/main/resources/native-execution${CONTRACT_SUFFIX}.tsv"
readonly NATIVE_REPORT_ROOT="${SUITE_ROOT}/native-execution/${SUITE_MODE}"
readonly DIAGNOSTIC_ROOT="${SUITE_ROOT}/diagnostics/${SUITE_MODE}"
readonly FLINK_MODULE_CONFIG="-Dstreamfusion.flink-suite.native-rocksdb=${NATIVE_STATE_SUITE} -Dstreamfusion.flink-suite.flink-line=${FLINK_LINE} -Duser.timezone=UTC -Djava.library.path=${STREAMFUSION_BUILD_ROOT}/native/target/debug --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Djunit.platform.reflection.search.useLegacySemantics=true -javaagent:${AGENT_JAR}"
readonly CONNECTOR_MODULE_CONFIG="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true ${FLINK_MODULE_CONFIG}"
# Paimon 2.0 compiles its Flink-1 shared sources against 1.20.1, including APIs
# guarded by version adapters. Execute the compiled tests on the requested host API.
PAIMON_COMPILE_VERSION="${FLINK_VERSION}"
if [[ "${FLINK_LINE}" == "1.18" ]]; then PAIMON_COMPILE_VERSION=1.20.1; fi
readonly PAIMON_BUILD_ARGS=("-P${PAIMON_FLINK_PROFILE}" "-Dtest.flink.main.version=${FLINK_LINE}" "-Dpaimon-flink-common.flink.version=${PAIMON_COMPILE_VERSION}" "-Dtest.flink.version=${FLINK_VERSION}" -Dspotless.check.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true)
readonly FORMAT_MODULES="flink-formats/flink-json,flink-formats/flink-csv,flink-formats/flink-avro,flink-formats/flink-avro-confluent-registry,flink-formats/flink-protobuf"
readonly ORC_MODULE="flink-formats/flink-orc"
readonly ORC_SQL_TESTS="org.apache.flink.orc.OrcFsStreamingSinkITCase,org.apache.flink.orc.OrcFileSystemITCase"
readonly PARQUET_MODULE="flink-formats/flink-parquet"
readonly PARQUET_SINK_TESTS="org.apache.flink.formats.parquet.ParquetFsStreamingSinkITCase,org.apache.flink.formats.parquet.ParquetTimestampITCase"
readonly PAIMON_SQL_TESTS="org.apache.paimon.flink.AppendOnlyTableITCase,org.apache.paimon.flink.AppendTableITCase,org.apache.paimon.flink.BatchFileStoreITCase,org.apache.paimon.flink.ComputedColumnAndWatermarkTableITCase,org.apache.paimon.flink.ContinuousFileStoreITCase,org.apache.paimon.flink.ReadWriteTableITCase,org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase,org.apache.paimon.flink.CompositePkAndMultiPartitionedTableITCase,org.apache.paimon.flink.FullCompactionFileStoreITCase,org.apache.paimon.flink.FlinkJobRecoveryITCase,org.apache.paimon.flink.RescaleBucketITCase,org.apache.paimon.flink.ScanBucketITCase,org.apache.paimon.flink.KeyOnlyDeletesITCase,org.apache.paimon.flink.FirstRowITCase,org.apache.paimon.flink.CoordinatorCommitITCase"
readonly PAIMON_ISOLATED_TESTS=(
  "org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase#testStandAloneLookupJobRandom"
  "org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase#testStandAloneFullCompactJobRandom"
)
readonly KAFKA_SQL_TESTS="org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaTableITCase,org.apache.flink.streaming.connectors.kafka.table.KafkaChangelogTableITCase,org.apache.flink.streaming.connectors.kafka.table.KafkaTableITCase,org.apache.flink.streaming.connectors.kafka.table.UpsertKafkaTableITCase"
readonly ROCKSDB_STATE_SQL_TESTS="org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.DeduplicateITCase,org.apache.flink.table.planner.runtime.stream.sql.GroupWindowITCase,org.apache.flink.table.planner.runtime.stream.sql.IntervalJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.JoinITCase,org.apache.flink.table.planner.runtime.stream.sql.OverAggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.RankITCase,org.apache.flink.table.planner.runtime.stream.sql.TemporalJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowAggregateITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowDeduplicateITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowJoinITCase,org.apache.flink.table.planner.runtime.stream.sql.WindowRankITCase,org.apache.flink.table.planner.runtime.stream.table.AggregateITCase,org.apache.flink.table.planner.runtime.stream.table.JoinITCase,org.apache.flink.table.planner.runtime.stream.table.OverAggregateITCase,org.apache.flink.table.planner.runtime.stream.table.RetractionITCase"
TEST_SELECTOR_ARGS=()
FORMAT_COMPILE_MODULES=""
if [[ -n "${FLINK_SUITE_TEST:-}" ]]; then
  TEST_SELECTOR_ARGS=("-Dtest=${FLINK_SUITE_TEST}")
fi

case "${SUITE_MODE}" in
  config)
    printf '%s\n' "flink.version=${FLINK_VERSION}" "flink.line=${FLINK_LINE}" \
      "kafka.version=${KAFKA_CONNECTOR_VERSION}" "kafka.tag=${KAFKA_CONNECTOR_TAG}" \
      "paimon.profile=${PAIMON_FLINK_PROFILE}" \
      "suite.root=${SUITE_ROOT}" "streamfusion.source=${STREAMFUSION_BUILD_ROOT}" \
      "maven.repo=${SUITE_MAVEN_REPO}" "agent.jar=${AGENT_JAR}" \
      "classpath=${CLASSPATH_FILE}" "contracts=${CONTRACT_FILE}"
    exit 0
    ;;
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
    FORMAT_COMPILE_MODULES="${FORMAT_MODULES},${PARQUET_MODULE},${ORC_MODULE}"
    REPORT_ROOT="${FLINK_ROOT}/flink-formats"
    ;;
  parquet)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="${PARQUET_MODULE}"
    FORMAT_COMPILE_MODULES="${PARQUET_MODULE}"
    REPORT_ROOT="${FLINK_ROOT}/flink-formats/flink-parquet/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      TEST_SELECTOR_ARGS=("-Dtest=${PARQUET_SINK_TESTS}")
    fi
    ;;
  orc)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="${ORC_MODULE}"
    FORMAT_COMPILE_MODULES="${ORC_MODULE}"
    REPORT_ROOT="${FLINK_ROOT}/flink-formats/flink-orc/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      TEST_SELECTOR_ARGS=("-Dtest=${ORC_SQL_TESTS}")
    fi
    ;;
  kafka)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="flink-connector-kafka"
    REPORT_ROOT="${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      TEST_SELECTOR_ARGS=("-Dtest=${KAFKA_SQL_TESTS}")
    fi
    ;;
  paimon)
    TEST_GOAL="surefire:test@integration-tests"
    TEST_MODULES="${PAIMON_MODULE}"
    REPORT_ROOT="${PAIMON_ROOT}/${PAIMON_MODULE}/target/surefire-reports"
    if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
      paimon_selector="${PAIMON_SQL_TESTS}"
      for isolated_test in "${PAIMON_ISOLATED_TESTS[@]}"; do
        paimon_selector+=",!${isolated_test}"
      done
      TEST_SELECTOR_ARGS=("-Dtest=${paimon_selector}")
    fi
    ;;
  delta)
    if [[ "${FLINK_LINE}" != "2.2" ]]; then
      echo "Delta acceleration is not admitted on Flink ${FLINK_LINE}; no suite payload is available." >&2
      exit 2
    fi
    TEST_GOAL="test"
    TEST_MODULES=":streamfusion-upstream-delta-tests"
    REPORT_ROOT="${DELTA_TEST_OUTPUT}/surefire-reports"
    ;;
  all)
    "${BASH_SOURCE[0]}" formats || exit $?
    FLINK_SUITE_REUSE_BUILD=true "${BASH_SOURCE[0]}" parquet || exit $?
    FLINK_SUITE_REUSE_BUILD=true "${BASH_SOURCE[0]}" orc || exit $?
    FLINK_SUITE_REUSE_BUILD=true "${BASH_SOURCE[0]}" runtime || exit $?
    FLINK_SUITE_REUSE_BUILD=true "${BASH_SOURCE[0]}" state || exit $?
    "${BASH_SOURCE[0]}" paimon || exit $?
    if [[ "${FLINK_LINE}" == "2.2" ]]; then
      "${BASH_SOURCE[0]}" delta || exit $?
    fi
    "${BASH_SOURCE[0]}" kafka
    exit $?
    ;;
  *)
    echo "Usage: $0 [config|runtime|diagnostic|state|formats|parquet|orc|kafka|paimon|delta|all]" >&2
    exit 2
    ;;
esac

flink_mvn() {
  (cd "${FLINK_ROOT}" && ./mvnw "$@")
}

kafka_mvn() {
  (cd "${KAFKA_CONNECTOR_ROOT}" && {
    if [[ -x ./mvnw ]]; then ./mvnw "$@"; else mvn "$@"; fi
  })
}

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

if [[ "${SUITE_MODE}" == "paimon" ]]; then
  if [[ ! -d "${PAIMON_ROOT}/.git" ]]; then
    git clone --depth 1 --branch "${PAIMON_TAG}" \
      https://github.com/apache/paimon.git "${PAIMON_ROOT}" || exit $?
  fi
  if [[ -n "$(git -C "${PAIMON_ROOT}" status --short)" ]]; then
    echo "The upstream Paimon checkout is not clean: ${PAIMON_ROOT}" >&2
    echo "Use a new FLINK_SUITE_ROOT or clean that disposable checkout manually." >&2
    exit 2
  fi
fi

if [[ "${SUITE_MODE}" == "delta" ]]; then
  if [[ ! -d "${DELTA_ROOT}/.git" ]]; then
    git clone --depth 1 --branch "v${DELTA_VERSION}" \
      https://github.com/delta-io/delta.git "${DELTA_ROOT}" || exit $?
  fi
  if [[ -n "$(git -C "${DELTA_ROOT}" status --short)" ]]; then
    echo "The upstream Delta checkout is not clean: ${DELTA_ROOT}" >&2
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
  if [[ "${SUITE_MODE}" == "parquet" ]] \
      && { [[ ! -f "${FLINK_ROOT}/flink-formats/flink-parquet/target/test-classes/org/apache/flink/formats/parquet/ParquetFsStreamingSinkITCase.class" ]] \
        || ! grep -q 'streamfusion-parquet' "${CLASSPATH_FILE}"; }; then
    echo "Cannot reuse the Parquet-suite build; run bin/flink-suite.sh parquet once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${SUITE_MODE}" == "orc" ]] \
      && { [[ ! -f "${FLINK_ROOT}/${ORC_MODULE}/target/test-classes/org/apache/flink/orc/OrcFsStreamingSinkITCase.class" ]] \
        || ! grep -q 'streamfusion-orc' "${CLASSPATH_FILE}"; }; then
    echo "Cannot reuse the ORC-suite build; run bin/flink-suite.sh orc once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${SUITE_MODE}" == "kafka" ]] \
      && [[ ! -f "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/target/test-classes/org/apache/flink/streaming/connectors/kafka/table/KafkaTableITCase.class" ]]; then
    echo "Cannot reuse the Kafka-suite build; run bin/flink-suite.sh kafka once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${SUITE_MODE}" == "paimon" ]] \
      && { [[ ! -f "${PAIMON_ROOT}/${PAIMON_MODULE}/target/test-classes/org/apache/paimon/flink/ReadWriteTableITCase.class" ]] \
        || ! grep -q 'streamfusion-paimon' "${CLASSPATH_FILE}"; }; then
    echo "Cannot reuse the Paimon-suite build; run bin/flink-suite.sh paimon once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  echo "Reusing the existing Flink suite and StreamFusion build artifacts..."
else
  echo "Building the test-JVM planner injection agent..."
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${AGENT_ROOT}/pom.xml" "-Dsuite.agent.output=${AGENT_OUTPUT}" package || exit $?

  echo "Building the pinned Flink planner and its reactor dependencies..."
  flink_mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -pl flink-table/flink-table-planner -am -DskipTests -Dfast install || exit $?

  echo "Installing the untouched planner classes for StreamFusion's source-suite build..."
  jar --create --file "${UNSHADED_SQL_PARSER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-sql-parser/target/classes" . || exit $?
  flink_mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
    -pl flink-table/flink-sql-parser \
    help:effective-pom -Doutput="${UNSHADED_SQL_PARSER_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_SQL_PARSER_JAR}" \
    -DpomFile="${UNSHADED_SQL_PARSER_POM}" || exit $?
  jar --create --file "${UNSHADED_BRIDGE_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-calcite-bridge/target/classes" . || exit $?
  flink_mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
    -pl flink-table/flink-table-calcite-bridge \
    help:effective-pom -Doutput="${UNSHADED_BRIDGE_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_BRIDGE_JAR}" \
    -DpomFile="${UNSHADED_BRIDGE_POM}" || exit $?
  jar --create --file "${UNSHADED_PLANNER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-planner/target/classes" . || exit $?
  flink_mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
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

  # Connector modules do not bind the runtime module's Maven-side Cargo execution. Build the
  # development DSO explicitly so the suite cannot accidentally package or load an excluded,
  # stale native/target left by an earlier run.
  echo "Building the StreamFusion development native library for the source suite..."
  (
    cd "${STREAMFUSION_BUILD_ROOT}/native" &&
      cargo build --workspace
  ) || exit $?

  echo "Building and installing StreamFusion and its supported connector/format modules against the source-suite planner..."
  streamfusion_profiles="paimon"
  streamfusion_modules="streamfusion-core,streamfusion-kafka,streamfusion-json,streamfusion-csv,streamfusion-raw,streamfusion-avro,streamfusion-avro-confluent-registry,streamfusion-protobuf,streamfusion-parquet,streamfusion-orc,streamfusion-paimon"
  if [[ "${SUITE_MODE}" == "delta" ]]; then
    streamfusion_profiles+=",delta"
    streamfusion_modules+=",streamfusion-delta"
  fi
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -Dstreamfusion.flink-source-suite "-P${streamfusion_profiles}" "${STREAMFUSION_LINE_PROFILES[@]}" \
    "-Dflink.version=${FLINK_VERSION}" -Dnative.build.skip=true \
    -f "${STREAMFUSION_BUILD_ROOT}/pom.xml" \
    -pl "${streamfusion_modules}" \
    -am -DskipTests clean install || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -f "${REPO_ROOT}/dev/flink-suite/classpath-pom.xml" "${STREAMFUSION_LINE_PROFILES[@]}" \
    dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" || exit $?

  if [[ "${SUITE_MODE}" == "formats" || "${SUITE_MODE}" == "parquet" || "${SUITE_MODE}" == "orc" ]]; then
    echo "Compiling the untouched upstream Flink format integration tests..."
    flink_mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
      -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
      -pl "${FORMAT_COMPILE_MODULES}" \
      -am -Dfast -DskipTests process-test-classes || exit $?
  fi

  if [[ "${SUITE_MODE}" == "kafka" ]]; then
    echo "Compiling the untouched upstream Kafka connector SQL integration tests..."
    kafka_mvn -B -ntp -s "${MAVEN_SETTINGS}" \
      -f "${KAFKA_CONNECTOR_ROOT}/pom.xml" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
      -Dflink.version="${FLINK_VERSION}" -pl flink-connector-kafka \
      -DskipTests test-compile || exit $?
  fi

  if [[ "${SUITE_MODE}" == "paimon" ]]; then
    echo "Building the pinned Paimon Flink connector and compiling its untouched SQL integration tests..."
    mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${PAIMON_ROOT}/pom.xml" \
      -Dmaven.repo.local="${SUITE_MAVEN_REPO}" "${PAIMON_BUILD_ARGS[@]}" \
      -pl "${PAIMON_MODULE}" -am -DskipTests install || exit $?
  fi
fi
python3 "${REPO_ROOT}/bin/check-flink-suite-classpath.py" "${CLASSPATH_FILE}" "${FLINK_LINE}" || exit $?
STREAMFUSION_CLASSPATH="$(tr ':' ',' < "${CLASSPATH_FILE}")"
if [[ "${SUITE_MODE}" != "paimon" ]]; then
  # Paimon's provided connector API is supplied by its own suite. Do not install its optional
  # planner extension into unrelated Flink test JVMs that do not have that connector.
  STREAMFUSION_CLASSPATH="$(python3 - "${STREAMFUSION_CLASSPATH}" <<'PY'
import sys
print(','.join(p for p in sys.argv[1].split(',') if not any(part == 'streamfusion-paimon' or part.startswith('streamfusion-paimon-flink') for part in p.split('/'))))
PY
)"
fi
if [[ "${SUITE_MODE}" == "paimon" ]]; then
  if [[ "${FLINK_LINE}" == "1.18" ]]; then
    # Common tests compile against 1.20. The released 1.18 runtime also replaces helpers
    # with different host signatures; appending its JAR cannot override target/classes.
    PAIMON_RUNTIME_JAR="${SUITE_MAVEN_REPO}/org/apache/paimon/paimon-flink-${FLINK_LINE}/${PAIMON_VERSION}/paimon-flink-${FLINK_LINE}-${PAIMON_VERSION}.jar"
    if [[ ! -f "${PAIMON_RUNTIME_JAR}" ]]; then
      echo "Missing released Paimon compatibility artifact: ${PAIMON_RUNTIME_JAR}" >&2
      exit 2
    fi
    TEST_MODULES=":paimon-flink-common"
  fi
  # Paimon declares the planner's test-jar before the planner itself, which places stock
  # calcite-core ahead of Flink's patched Calcite classes in Surefire's resolved classpath. Drop the
  # resolved calcite-core and append it instead, so the planner's copies win as in Flink's own build.
  CALCITE_CORE_JAR="$(find "${SUITE_MAVEN_REPO}/org/apache/calcite/calcite-core" -name 'calcite-core-*.jar' \
    ! -name '*-sources.jar' ! -name '*-tests.jar' | head -n 1)"
  if [[ -z "${CALCITE_CORE_JAR}" ]]; then
    echo "The isolated suite repository holds no calcite-core jar for the Paimon suite classpath." >&2
    exit 2
  fi
  STREAMFUSION_CLASSPATH="${STREAMFUSION_CLASSPATH},${CALCITE_CORE_JAR}"
fi
readonly STREAMFUSION_CLASSPATH

echo "Running the upstream Flink ${SUITE_MODE} suite with StreamFusion enabled..."
mkdir -p "${NATIVE_REPORT_ROOT}"
mkdir -p "${DIAGNOSTIC_ROOT}"
find "${DIAGNOSTIC_ROOT}" -maxdepth 1 -type f -delete
find "${NATIVE_REPORT_ROOT}" -maxdepth 1 -type f -name '*.tsv' -delete
mkdir -p "${REPORT_ROOT}"
if [[ "${SUITE_MODE}" == "formats" ]]; then
  find "${REPORT_ROOT}" -type f -path '*/target/surefire-reports/*' -delete
elif [[ "${SUITE_MODE}" == "paimon" ]]; then
  find "${REPORT_ROOT}" -type f -delete
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
  -Dstreamfusion.flink-suite.native-reports="${NATIVE_REPORT_ROOT}" \
  -Dfast \
  -Dmaven.test.failure.ignore=false \
  -Djunit.jupiter.execution.parallel.enabled=false \
  -Dflink.forkCountUnitTest="${FLINK_SUITE_UNIT_FORKS:-2}" \
  -Dflink.forkCountITCase="${FLINK_SUITE_IT_FORKS:-1}"
)
if [[ "${SUITE_MODE}" == "runtime" || "${SUITE_MODE}" == "diagnostic" ]]; then
  MAVEN_TEST_ARGS+=(
    -Dmaven.ext.class.path="${AGENT_JAR}"
    -Dstreamfusion.flink-suite.maven-result="${DIAGNOSTIC_ROOT}/maven-result.tsv"
  )
fi
if [[ "${SUITE_MODE}" == "state" ]]; then
  MAVEN_TEST_ARGS+=("-Dstreamfusion.flink-suite.native-rocksdb=true")
fi
if [[ "${SUITE_MODE}" == "kafka" ]]; then
  MAVEN_TEST_ARGS+=(
    -f "${KAFKA_CONNECTOR_ROOT}/pom.xml"
    -Dflink.version="${FLINK_VERSION}"
    -Dflink.surefire.baseArgLine="${FLINK_MODULE_CONFIG}"
  )
elif [[ "${SUITE_MODE}" == "delta" ]]; then
  MAVEN_TEST_ARGS+=(
    -f "${DELTA_TEST_POM}"
    -Dflink.version="${FLINK_VERSION}"
    -Ddelta.version="${DELTA_VERSION}"
    -Ddelta.source.root="${DELTA_ROOT}"
    -Ddelta.test.output="${DELTA_TEST_OUTPUT}"
    -Ddelta.test.jvm.args="${CONNECTOR_MODULE_CONFIG}"
  )
elif [[ "${SUITE_MODE}" == "paimon" ]]; then
  PAIMON_TEST_POM="${PAIMON_ROOT}/pom.xml"
  if [[ "${FLINK_LINE}" == "1.18" ]]; then
    PAIMON_TEST_POM="${DIAGNOSTIC_ROOT}/paimon-runtime-pom.xml"
    python3 "${REPO_ROOT}/dev/flink-suite/prepare_paimon_runtime_pom.py" \
      "${PAIMON_ROOT}/${PAIMON_MODULE}/pom.xml" "${PAIMON_RUNTIME_JAR}" \
      "${PAIMON_TEST_POM}" || exit $?
  fi
  MAVEN_TEST_ARGS+=(
    -f "${PAIMON_TEST_POM}"
    "${PAIMON_BUILD_ARGS[@]}"
    "-Dpaimon-flink-common.flink.version=${FLINK_VERSION}"
    -Dflink.forkCount="${FLINK_SUITE_IT_FORKS:-1}"
    -Djunit.jupiter.execution.timeout.default=10m
    -Dsurefire.timeout=1800
    -Dlog4j.configurationFile="${REPO_ROOT}/dev/flink-suite/paimon-log4j2.properties"
    -Dstreamfusion.flink-suite.diagnostics="${DIAGNOSTIC_ROOT}"
    -Dmaven.test.dependency.excludes=org.apache.calcite:calcite-core
    -DextraJavaTestArgs="${CONNECTOR_MODULE_CONFIG}"
  )
else
  MAVEN_TEST_ARGS+=(
    -f "${FLINK_ROOT}/pom.xml"
    -Dsurefire.module.config="${FLINK_MODULE_CONFIG}"
  )
fi
if [[ ${#TEST_SELECTOR_ARGS[@]} -gt 0 ]]; then
  MAVEN_TEST_ARGS+=("${TEST_SELECTOR_ARGS[@]}")
  if [[ "${SUITE_MODE}" == "formats" || "${SUITE_MODE}" == "parquet" || "${SUITE_MODE}" == "orc" || "${SUITE_MODE}" == "paimon" ]]; then
    MAVEN_TEST_ARGS+=("-Dsurefire.failIfNoSpecifiedTests=false")
  fi
fi
MAVEN_TEST_ARGS+=("${TEST_GOAL}")
if [[ "${SUITE_MODE}" == "kafka" ]]; then
  kafka_mvn "${MAVEN_TEST_ARGS[@]}"
elif [[ "${SUITE_MODE}" == "delta" ]]; then
  mvn "${MAVEN_TEST_ARGS[@]}"
elif [[ "${SUITE_MODE}" == "paimon" ]]; then
  mvn "${MAVEN_TEST_ARGS[@]}"
  paimon_status=$?
  if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
    # Cancelled upstream compactors can kill their shared MiniCluster during cleanup. Run each
    # unchanged standalone compaction test in its own fork to contain that lifecycle race.
    mkdir -p "${REPORT_ROOT}/shared-cluster"
    for report in "${REPORT_ROOT}/"*PrimaryKeyFileStoreTableITCase*; do
      if [[ -f "${report}" ]]; then
        mv "${report}" "${REPORT_ROOT}/shared-cluster/" || exit $?
      fi
    done
    for isolated_test in "${PAIMON_ISOLATED_TESTS[@]}"; do
      echo "Running unchanged ${isolated_test} in a separate JVM..."
      mvn "${MAVEN_TEST_ARGS[@]}" "-Dtest=${isolated_test}" -Dsurefire.failIfNoSpecifiedTests=true
      isolated_status=$?
      if [[ ${isolated_status} -ne 0 ]]; then paimon_status=${isolated_status}; fi
      isolated_reports="${REPORT_ROOT}/${isolated_test#*#}"
      mkdir -p "${isolated_reports}"
      for report in "${REPORT_ROOT}/"*PrimaryKeyFileStoreTableITCase*; do
        if [[ -f "${report}" ]]; then
          mv "${report}" "${isolated_reports}/" || exit $?
        fi
      done
    done
  fi
  # Keep either invocation's nonzero exit status for the common report checks below.
  (exit "${paimon_status}")
else
  flink_mvn "${MAVEN_TEST_ARGS[@]}"
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

if [[ ( "${SUITE_MODE}" == "parquet" || "${SUITE_MODE}" == "orc" ) && ${TEST_STATUS} -eq 0 ]]; then
  codec_name="Parquet"
  if [[ "${SUITE_MODE}" == "orc" ]]; then codec_name="Orc"; fi
  readonly PARQUET_MARKER="StreamFusion upstream ${codec_name} suite created native ${codec_name} sink writer"
  if ! grep -RqsF "${PARQUET_MARKER}" "${REPORT_ROOT}"; then
    echo "The upstream ${codec_name} suite did not prove: ${PARQUET_MARKER}" >&2
    exit 1
  fi
fi

if [[ "${SUITE_MODE}" == "paimon" && ${TEST_STATUS} -eq 0 ]]; then
  for required_marker in \
    "StreamFusion upstream Paimon suite wrote a native Paimon bundle" \
    "StreamFusion upstream Paimon suite wrote a native Paimon level-0 file" \
    "StreamFusion upstream Paimon suite merged a native snapshot batch"; do
    if ! grep -RqsF "${required_marker}" "${REPORT_ROOT}"; then
      echo "The upstream Paimon suite did not prove: ${required_marker}" >&2
      exit 1
    fi
  done
fi

SUMMARY_ARGS=("${REPORT_ROOT}" --contracts "${CONTRACT_FILE}" --native-reports "${NATIVE_REPORT_ROOT}" --process-exit "${TEST_STATUS}")
if [[ "${SUITE_MODE}" == "runtime" || "${SUITE_MODE}" == "diagnostic" ]]; then
  SUMMARY_ARGS+=(--maven-result "${DIAGNOSTIC_ROOT}/maven-result.tsv")
  if [[ "${FLINK_LINE}" == "2.2" ]]; then
    SUMMARY_ARGS+=(--xfail "org.apache.flink.table.planner.runtime.batch.sql.CalcITCase#testCurrentDate")
  fi
  if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
    SUMMARY_ARGS+=(--require-contract-prefix org.apache.flink.)
  fi
fi
if [[ "${SUITE_MODE}" == "delta" && -z "${FLINK_SUITE_TEST:-}" ]]; then
  SUMMARY_ARGS+=(
    --require-contract-prefix io.delta.
    --require-test 'io.delta.flink.sink.sql.FlinkSqlTest#testGroupedAggregationPreservesEachRow'
  )
fi
if [[ "${SUITE_MODE}" == "state" && -z "${FLINK_SUITE_TEST:-}" ]]; then
  for state_test in ${ROCKSDB_STATE_SQL_TESTS//,/ }; do
    SUMMARY_ARGS+=(--require-test-class "${state_test}")
    if awk -F '\t' -v prefix="${state_test}#" 'index($1, prefix) == 1 { found=1 } END { exit !found }' "${CONTRACT_FILE}"; then
      SUMMARY_ARGS+=(--require-contract-prefix "${state_test}#")
    fi
  done
fi
python3 "${REPO_ROOT}/dev/flink-suite/summarize.py" "${SUMMARY_ARGS[@]}"
exit $?
