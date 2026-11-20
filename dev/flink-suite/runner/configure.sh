#!/usr/bin/env bash

readonly FLINK_VERSION="${FLINK_VERSION:-2.2.1}"
readonly FLINK_TAG="release-${FLINK_VERSION}"
case "${FLINK_VERSION}" in
  2.2.0|2.2.1)
    FLINK_LINE=2.2
    FLINK_TARGET_PROFILE=java11-target
    STREAMFUSION_LINE_PROFILES=()
    STREAMFUSION_ARTIFACT_SUFFIX=""
    KAFKA_DEFAULT_VERSION=5.0.0
    PAIMON_FLINK_PROFILE=flink2
    PAIMON_DEFAULT_VERSION=2.1.0
    ;;
  1.18.1)
    FLINK_LINE=1.18
    FLINK_TARGET_PROFILE=""
    STREAMFUSION_LINE_PROFILES=(-Pflink-1.18)
    STREAMFUSION_ARTIFACT_SUFFIX=-flink1.18
    KAFKA_DEFAULT_VERSION=3.2.0
    PAIMON_FLINK_PROFILE=flink1
    PAIMON_DEFAULT_VERSION=2.0.0
    ;;
  *) echo "Unsupported Flink suite version: ${FLINK_VERSION}" >&2; exit 2 ;;
esac
readonly FLINK_LINE FLINK_TARGET_PROFILE STREAMFUSION_ARTIFACT_SUFFIX KAFKA_DEFAULT_VERSION PAIMON_FLINK_PROFILE
readonly KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION:-${KAFKA_DEFAULT_VERSION}}"
# The published 3.2.0 source archive matches the final candidate; no v3.2.0 tag exists.
if [[ "${KAFKA_CONNECTOR_VERSION}" == "3.2.0" ]]; then
  KAFKA_CONNECTOR_TAG=v3.2.0-rc1
else
  KAFKA_CONNECTOR_TAG="v${KAFKA_CONNECTOR_VERSION}"
fi
readonly KAFKA_CONNECTOR_TAG
readonly PAIMON_VERSION="${PAIMON_VERSION:-${PAIMON_DEFAULT_VERSION}}"
readonly PAIMON_TAG="${PAIMON_TAG:-release-${PAIMON_VERSION}}"
readonly DELTA_VERSION="${DELTA_VERSION:-4.4.0}"
readonly SUITE_BASE_ROOT="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}"
readonly SUITE_ROOT="${SUITE_BASE_ROOT}/${FLINK_LINE}"
readonly FLINK_ROOT="${SUITE_ROOT}/flink-${FLINK_VERSION}"
readonly KAFKA_CONNECTOR_ROOT="${SUITE_ROOT}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}"
readonly PAIMON_ROOT="${SUITE_ROOT}/paimon-${PAIMON_VERSION}"
readonly PAIMON_MODULE="paimon-flink/paimon-flink-common"
readonly PAIMON_LINE_MODULE="paimon-flink/paimon-flink-${FLINK_LINE}"
readonly PAIMON_118_SHARED_TESTS="${REPO_ROOT}/dev/flink-suite/paimon-flink118-shared-tests.txt"
readonly PAIMON_118_LINE_CLASSES="org.apache.paimon.flink.AppendTableITCase,org.apache.paimon.flink.ContinuousFileStoreITCase,org.apache.paimon.flink.RemoveOrphanFilesActionITCase,org.apache.paimon.flink.procedure.ProcedurePositionalArgumentsITCase,org.apache.paimon.flink.iceberg.Flink118IcebergITCase"
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
readonly SUITE_MAVEN_USER_HOME="${SUITE_ROOT}/maven-wrapper"
readonly UNSHADED_PLANNER_JAR="${SUITE_ROOT}/flink-table-planner-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_PLANNER_POM="${SUITE_ROOT}/flink-table-planner-${FLINK_VERSION}-effective.pom"
readonly UNSHADED_BRIDGE_JAR="${SUITE_ROOT}/flink-table-calcite-bridge-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_BRIDGE_POM="${SUITE_ROOT}/flink-table-calcite-bridge-${FLINK_VERSION}-effective.pom"
readonly UNSHADED_SQL_PARSER_JAR="${SUITE_ROOT}/flink-sql-parser-${FLINK_VERSION}-unshaded.jar"
readonly UNSHADED_SQL_PARSER_POM="${SUITE_ROOT}/flink-sql-parser-${FLINK_VERSION}-effective.pom"
readonly SUITE_MODE="${1:-runtime}"
RUNTIME_AUDIT_SCOPE=""
if [[ "${FLINK_SUITE_RUNTIME_AUDIT:-false}" == "true" ]]; then
  if [[ "${SUITE_MODE}" != "runtime" ]]; then
    echo "FLINK_SUITE_RUNTIME_AUDIT requires the runtime suite." >&2
    exit 2
  fi
  RUNTIME_AUDIT_SCOPE="${REPO_ROOT}/dev/flink-suite/runtime-route-scope-${FLINK_LINE}.json"
  if [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
    FLINK_SUITE_TEST="$(python3 - "${RUNTIME_AUDIT_SCOPE}" <<'PYTHON'
import json, sys
scope = json.load(open(sys.argv[1]))
print(','.join(dict.fromkeys(case['test'] for case in scope['cases'])))
PYTHON
)" || exit $?
  fi
fi
readonly RUNTIME_AUDIT_SCOPE
if [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then
  case "${FLINK_SUITE_SHARD}" in
    1|2|3|4) ;;
    *) echo "FLINK_SUITE_SHARD must be 1, 2, 3 or 4." >&2; exit 2 ;;
  esac
  if [[ "${SUITE_MODE}" != "runtime" || -n "${FLINK_SUITE_TEST:-}" ]]; then
    echo "FLINK_SUITE_SHARD requires a full runtime suite, without FLINK_SUITE_TEST." >&2
    exit 2
  fi
fi
CONTRACT_SUFFIX="${STREAMFUSION_ARTIFACT_SUFFIX}"
NATIVE_STATE_SUITE=false
if [[ "${SUITE_MODE}" == "state" ]]; then NATIVE_STATE_SUITE=true; fi
if [[ "${FLINK_LINE}" == "1.18" && "${SUITE_MODE}" == "state" ]]; then
  CONTRACT_SUFFIX="${CONTRACT_SUFFIX}-state"
fi
readonly CONTRACT_FILE="${AGENT_ROOT}/src/main/resources/native-execution${CONTRACT_SUFFIX}.tsv"
readonly NATIVE_REPORT_ROOT="${SUITE_ROOT}/native-execution/${SUITE_MODE}"
readonly DIAGNOSTIC_ROOT="${SUITE_ROOT}/diagnostics/${SUITE_MODE}"
SQL_INVENTORY_CONFIG=""
if [[ "${FLINK_SUITE_SQL_INVENTORY:-false}" == "true" || -n "${RUNTIME_AUDIT_SCOPE}" ]]; then
  SQL_INVENTORY_CONFIG="-Dstreamfusion.flink-suite.sql-inventory=${DIAGNOSTIC_ROOT}/sql-inventory"
fi
readonly FLINK_MODULE_CONFIG="${SQL_INVENTORY_CONFIG} -Dstreamfusion.flink-suite.native-rocksdb=${NATIVE_STATE_SUITE} -Dstreamfusion.flink-suite.flink-line=${FLINK_LINE} -Duser.timezone=UTC -Djava.library.path=${STREAMFUSION_BUILD_ROOT}/native/target/debug --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Djunit.platform.reflection.search.useLegacySemantics=true -javaagent:${AGENT_JAR}"
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
  "org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase#testNoChangelogProducerStreamingRandom"
  "org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase#testFullCompactionChangelogProducerStreamingRandom"
  "org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase#testLookupChangelogProducerStreamingRandom"
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
  prepare)
    TEST_MODULES="flink-table/flink-table-planner"
    FORMAT_COMPILE_MODULES="${FORMAT_MODULES},${PARQUET_MODULE},${ORC_MODULE}"
    ;;
  config)
    printf '%s\n' "flink.version=${FLINK_VERSION}" "flink.line=${FLINK_LINE}" \
      "flink.target.profile=${FLINK_TARGET_PROFILE}" \
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
    TEST_MODULES=":flink-connector-kafka"
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
      if [[ "${FLINK_LINE}" == "1.18" ]]; then
        paimon_selector="$(paste -sd, "${PAIMON_118_SHARED_TESTS}")"
      else
        for isolated_test in "${PAIMON_ISOLATED_TESTS[@]}"; do
          paimon_selector+=",!${isolated_test}"
        done
      fi
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
      "${BASH_SOURCE[0]}" iceberg || exit $?
    fi
    "${BASH_SOURCE[0]}" kafka
    exit $?
    ;;
  *)
    echo "Usage: $0 [config|prepare|runtime|diagnostic|state|formats|parquet|orc|kafka|paimon|delta|iceberg|all]" >&2
    exit 2
    ;;
esac

