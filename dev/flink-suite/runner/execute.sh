#!/usr/bin/env bash

if [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then
  shard_selector="$(python3 "${REPO_ROOT}/dev/flink-suite/runtime_shards.py" select \
    --plan "${SUITE_ROOT}/runtime-shards.json" --shard "${FLINK_SUITE_SHARD}")" || exit $?
  TEST_SELECTOR_ARGS=("-Dtest=${shard_selector}")
fi
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
fi
if [[ "${SUITE_MODE}" == "kafka" && "${FLINK_LINE}" == "1.18" ]]; then
  # Kafka 3.2 otherwise isolates the planner in planner-loader while the agent and native
  # planner live in the test JVM. Use the same unshaded planner as the other upstream suites.
  STREAMFUSION_CLASSPATH="${UNSHADED_PLANNER_JAR},${STREAMFUSION_CLASSPATH}"
fi
if [[ "${SUITE_MODE}" == "paimon" || ( "${SUITE_MODE}" == "kafka" && "${FLINK_LINE}" == "1.18" ) ]]; then
  # These connector fixtures resolve stock Calcite before the planner. Put it after the
  # planner's patched classes, preserving the ordering used by Flink's own test build.
  CALCITE_CORE_JAR="$(find "${SUITE_MAVEN_REPO}/org/apache/calcite/calcite-core" -name 'calcite-core-*.jar' \
    ! -name '*-sources.jar' ! -name '*-tests.jar' | head -n 1)"
  if [[ -z "${CALCITE_CORE_JAR}" ]]; then
    echo "The isolated suite repository holds no calcite-core jar for the connector suite classpath." >&2
    exit 2
  fi
  STREAMFUSION_CLASSPATH="${STREAMFUSION_CLASSPATH},${CALCITE_CORE_JAR}"
fi
readonly STREAMFUSION_CLASSPATH

echo "Running the upstream Flink ${SUITE_MODE} suite with StreamFusion enabled..."
mkdir -p "${NATIVE_REPORT_ROOT}"
mkdir -p "${DIAGNOSTIC_ROOT}"
if [[ -n "${SQL_INVENTORY_CONFIG}" ]]; then
  mkdir -p "${DIAGNOSTIC_ROOT}/sql-inventory"
  find "${DIAGNOSTIC_ROOT}/sql-inventory" -type f -name '*.json' -delete
fi
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
  -B -ntp -e -s "${MAVEN_SETTINGS}" \
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
if [[ "${SUITE_MODE}" == "runtime" && "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" == "true" ]]; then
  MAVEN_TEST_ARGS+=(-o -gs "${MAVEN_SETTINGS}")
fi
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
    -f "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/pom.xml"
    -Dflink.version="${FLINK_VERSION}"
    -Dflink.surefire.baseArgLine="${FLINK_MODULE_CONFIG}"
  )
  if [[ "${FLINK_LINE}" == "1.18" ]]; then
    MAVEN_TEST_ARGS+=(
      -Dmaven.test.dependency.excludes=org.apache.flink:flink-table-planner-loader,org.apache.calcite:calcite-core
    )
  fi
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
    -Dstreamfusion.flink-suite.retain-failed-warehouse="${FLINK_SUITE_RETAIN_FAILED_WAREHOUSE:-false}"
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
  if [[ "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" != "true" ]]; then
    prepare_delta_maven || exit $?
  fi
  mvn -o -gs "${MAVEN_SETTINGS}" "${MAVEN_TEST_ARGS[@]}"
elif [[ "${SUITE_MODE}" == "paimon" ]]; then
  mvn "${MAVEN_TEST_ARGS[@]}"
  paimon_status=$?
  if [[ -z "${FLINK_SUITE_TEST:-}" && "${FLINK_LINE}" == "1.18" ]]; then
    line_pom="${DIAGNOSTIC_ROOT}/paimon-line-pom.xml"
    line_reports="${PAIMON_ROOT}/${PAIMON_LINE_MODULE}/target/surefire-reports"
    python3 "${REPO_ROOT}/dev/flink-suite/prepare_paimon_runtime_pom.py" \
      "${PAIMON_ROOT}/${PAIMON_LINE_MODULE}/pom.xml" "${PAIMON_RUNTIME_JAR}" \
      "${line_pom}" || exit $?
    mkdir -p "${line_reports}"
    find "${line_reports}" -type f -delete
    echo "Running Paimon's complete unchanged Flink 1.18 module in separate JVMs..."
    line_args=("${MAVEN_TEST_ARGS[@]}")
    for index in "${!line_args[@]}"; do
      case "${line_args[index]}" in
        -f) line_args[index+1]="${line_pom}" ;;
        -pl) line_args[index+1]=:streamfusion-upstream-paimon-line-tests ;;
        -Dtest=*) line_args[index]="-Dtest=${PAIMON_118_LINE_CLASSES}" ;;
        -Dsurefire.failIfNoSpecifiedTests=*) line_args[index]=-Dsurefire.failIfNoSpecifiedTests=true ;;
      esac
    done
    mvn "${line_args[@]}"
    line_status=$?
    line_summary=("${line_reports}" --contracts "${CONTRACT_FILE}" --process-exit "${line_status}"
      --audit-output "${DIAGNOSTIC_ROOT}/paimon-line-execution-audit.json")
    while IFS= read -r line_test; do
      line_summary+=(--require-test-class "${line_test}")
    done < <(printf '%s\n' "${PAIMON_118_LINE_CLASSES}" | tr ',' '\n')
    python3 "${REPO_ROOT}/dev/flink-suite/summarize.py" "${line_summary[@]}"
    line_status=$?
    if [[ ${line_status} -ne 0 ]]; then paimon_status=${line_status}; fi
    # Two upstream classes share names with common fixtures; retain both sets of XML.
    mkdir -p "${REPORT_ROOT}/flink-1.18"
    cp -R "${line_reports}/." "${REPORT_ROOT}/flink-1.18/" || exit $?
  elif [[ -z "${FLINK_SUITE_TEST:-}" ]]; then
    # Cancellation and injected I/O failures can kill the shared MiniCluster during cleanup.
    # Keep each affected upstream test unchanged and contain its failure in a separate fork.
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
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} "${MAVEN_TEST_ARGS[@]}"
fi
readonly TEST_STATUS=$?

