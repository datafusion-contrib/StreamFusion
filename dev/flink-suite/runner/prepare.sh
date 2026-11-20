#!/usr/bin/env bash

flink_mvn() {
  (cd "${FLINK_ROOT}" && MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" ./mvnw "$@")
}

kafka_mvn() {
  (cd "${KAFKA_CONNECTOR_ROOT}" && {
    if [[ -x ./mvnw ]]; then
      MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" ./mvnw "$@"
    else
      flink_mvn "$@"
    fi
  })
}

prepare_kafka_maven() {
  local wrapper_root="${FLINK_ROOT}"
  if [[ -x "${KAFKA_CONNECTOR_ROOT}/mvnw" ]]; then
    wrapper_root="${KAFKA_CONNECTOR_ROOT}"
  fi
  (
    cd "${wrapper_root}" &&
      MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
        --maven ./mvnw --graph --repository "${SUITE_MAVEN_REPO}" \
        --pom "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/pom.xml" --define "flink.version=${FLINK_VERSION}"
  )
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

# Kafka 3.2 has no wrapper to prepare. Its suite uses the prepared Flink Maven.
if [[ "${SUITE_MODE}" == "kafka" ||
      ( "${SUITE_MODE}" == "prepare" && "${KAFKA_CONNECTOR_VERSION}" != "3.2.0" ) ]]; then
  if [[ ! -d "${KAFKA_CONNECTOR_ROOT}/.git" ]]; then
    git clone --depth 1 --branch "${KAFKA_CONNECTOR_TAG}" \
      https://github.com/apache/flink-connector-kafka.git "${KAFKA_CONNECTOR_ROOT}" || exit $?
  fi
  if [[ -n "$(git -C "${KAFKA_CONNECTOR_ROOT}" status --short)" ]]; then
    echo "The upstream Kafka connector checkout is not clean: ${KAFKA_CONNECTOR_ROOT}" >&2
    echo "Use a new FLINK_SUITE_ROOT or clean that disposable checkout manually." >&2
    exit 2
  fi
  if [[ "${SUITE_MODE}" == "prepare" && -x "${KAFKA_CONNECTOR_ROOT}/mvnw" ]]; then
    echo "Preparing the released Kafka Maven wrapper and connector model..."
    (
      cd "${KAFKA_CONNECTOR_ROOT}" &&
        MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
          --maven ./mvnw --model-only --repository "${SUITE_MAVEN_REPO}" \
          --pom "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/pom.xml" --define "flink.version=${FLINK_VERSION}"
    ) || exit $?
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

if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" == "true" || "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" == "true" ]]; then
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
  if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" == "true" && "${SUITE_MODE}" == "kafka" ]] \
      && [[ ! -f "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/target/test-classes/org/apache/flink/streaming/connectors/kafka/table/KafkaTableITCase.class" ]]; then
    echo "Cannot reuse the Kafka-suite build; run bin/flink-suite.sh kafka once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" == "true" && "${SUITE_MODE}" == "paimon" ]] \
      && { [[ ! -f "${PAIMON_ROOT}/${PAIMON_MODULE}/target/test-classes/org/apache/paimon/flink/ReadWriteTableITCase.class" ]] \
        || ! grep -q 'streamfusion-paimon' "${CLASSPATH_FILE}"; }; then
    echo "Cannot reuse the Paimon-suite build; run bin/flink-suite.sh paimon once without FLINK_SUITE_REUSE_BUILD." >&2
    exit 2
  fi
  if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" == "true" && "${SUITE_MODE}" == "paimon" && "${FLINK_LINE}" == "1.18" ]] \
      && [[ ! -f "${PAIMON_ROOT}/${PAIMON_LINE_MODULE}/target/test-classes/org/apache/paimon/flink/procedure/ProcedurePositionalArgumentsITCase.class" ]]; then
    echo "Cannot reuse the Paimon 1.18 suite before its version-specific tests have been compiled." >&2
    exit 2
  fi
  echo "Reusing the existing Flink suite and StreamFusion build artifacts..."
else
  echo "Building the test-JVM planner injection agent..."
  python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
    --pom "${AGENT_ROOT}/pom.xml" --resolve-project || exit $?
  # Surefire discovers the provider and project-version launcher during execution.
  python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
    --artifact org.apache.maven.surefire:surefire-junit-platform:3.5.3 \
    --artifact org.junit.platform:junit-platform-launcher:1.10.2 || exit $?
  mvn -B -ntp -o -s "${MAVEN_SETTINGS}" -gs "${MAVEN_SETTINGS}" \
    -f "${AGENT_ROOT}/pom.xml" "-Dsuite.agent.output=${AGENT_OUTPUT}" package || exit $?

  echo "Building the pinned Flink planner and its reactor dependencies..."
  (
    cd "${FLINK_ROOT}" &&
      MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
        --maven "${FLINK_ROOT}/mvnw" --pom "${FLINK_ROOT}/pom.xml" \
        --repository "${SUITE_MAVEN_REPO}" --profile "${FLINK_TARGET_PROFILE}" \
        --projects flink-table/flink-table-planner --also-make --model-only
  ) || exit $?
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -T "${FLINK_SUITE_BUILD_THREADS:-1}" \
    -pl flink-table/flink-table-planner -am -DskipTests -Dfast install || exit $?

  if [[ -n "${FORMAT_COMPILE_MODULES}" ]]; then
    echo "Compiling the untouched upstream Flink format integration tests..."
    format_goal=process-test-classes
    if [[ "${SUITE_MODE}" == "prepare" ]]; then format_goal=install; fi
    flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
      -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
      -T "${FLINK_SUITE_BUILD_THREADS:-1}" \
      -pl "${FORMAT_COMPILE_MODULES}" \
      -am -Dfast -DskipTests "${format_goal}" || exit $?
  fi

  echo "Installing the untouched planner classes for StreamFusion's source-suite build..."
  jar --create --file "${UNSHADED_SQL_PARSER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-sql-parser/target/classes" . || exit $?
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
    -pl flink-table/flink-sql-parser \
    help:effective-pom -Doutput="${UNSHADED_SQL_PARSER_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_SQL_PARSER_JAR}" \
    -DpomFile="${UNSHADED_SQL_PARSER_POM}" || exit $?
  jar --create --file "${UNSHADED_BRIDGE_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-calcite-bridge/target/classes" . || exit $?
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -Didea.version=streamfusion-suite \
    -pl flink-table/flink-table-calcite-bridge \
    help:effective-pom -Doutput="${UNSHADED_BRIDGE_POM}" || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    install:install-file \
    -Dfile="${UNSHADED_BRIDGE_JAR}" \
    -DpomFile="${UNSHADED_BRIDGE_POM}" || exit $?
  jar --create --file "${UNSHADED_PLANNER_JAR}" \
    -C "${FLINK_ROOT}/flink-table/flink-table-planner/target/classes" . || exit $?
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -s "${MAVEN_SETTINGS}" -f "${FLINK_ROOT}/pom.xml" \
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
  python3 "${REPO_ROOT}/dev/flink-suite/native_build.py" \
    --source "${STREAMFUSION_BUILD_ROOT}" \
    --target "${FLINK_SUITE_NATIVE_TARGET_DIR:-${STREAMFUSION_BUILD_ROOT}/native/target}" || exit $?

  echo "Building and installing StreamFusion and its supported connector/format modules against the source-suite planner..."
  streamfusion_profiles="paimon"
  streamfusion_modules="streamfusion-core,streamfusion-kafka,streamfusion-json,streamfusion-csv,streamfusion-raw,streamfusion-avro,streamfusion-avro-confluent-registry,streamfusion-protobuf,streamfusion-parquet,streamfusion-orc,streamfusion-paimon"
  if [[ "${SUITE_MODE}" == "delta" || ( "${SUITE_MODE}" == "prepare" && "${FLINK_LINE}" == "2.2" ) ]]; then
    streamfusion_profiles+=",delta"
    streamfusion_modules+=",streamfusion-delta"
    streamfusion_profiles+=",iceberg"
    streamfusion_modules+=",streamfusion-iceberg"
  fi
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -Dstreamfusion.flink-source-suite "-P${streamfusion_profiles}" "${STREAMFUSION_LINE_PROFILES[@]}" \
    "-Dflink.version=${FLINK_VERSION}" -Dnative.build.skip=true -Dmaven.javadoc.skip=true \
    -f "${STREAMFUSION_BUILD_ROOT}/pom.xml" \
    -pl "${streamfusion_modules}" \
    -am -DskipTests clean install || exit $?
  mvn -B -ntp -s "${MAVEN_SETTINGS}" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
    -f "${REPO_ROOT}/dev/flink-suite/classpath-pom.xml" "${STREAMFUSION_LINE_PROFILES[@]}" \
    dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" || exit $?

fi

if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" != "true" ]]; then
  if [[ "${SUITE_MODE}" == "kafka" ]]; then
    echo "Resolving the upstream Kafka connector dependency descriptors before compilation..."
    prepare_kafka_maven || exit $?
    echo "Compiling the untouched upstream Kafka connector SQL integration tests..."
    kafka_mvn -B -ntp -s "${MAVEN_SETTINGS}" \
      -f "${KAFKA_CONNECTOR_ROOT}/flink-connector-kafka/pom.xml" -Dmaven.repo.local="${SUITE_MAVEN_REPO}" \
      -Dflink.version="${FLINK_VERSION}" \
      -DskipTests test-compile || exit $?
  fi

  if [[ "${SUITE_MODE}" == "paimon" ]]; then
    echo "Building the pinned Paimon Flink connector and compiling its untouched SQL integration tests..."
    mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${PAIMON_ROOT}/pom.xml" \
      -Dmaven.repo.local="${SUITE_MAVEN_REPO}" "${PAIMON_BUILD_ARGS[@]}" \
      -pl "${PAIMON_MODULE}" -am -DskipTests install || exit $?
    if [[ "${FLINK_LINE}" == "1.18" ]]; then
      # Compile version-specific tests without replacing the released production JAR.
      mvn -B -ntp -s "${MAVEN_SETTINGS}" -f "${PAIMON_ROOT}/pom.xml" \
        -Dmaven.repo.local="${SUITE_MAVEN_REPO}" "${PAIMON_BUILD_ARGS[@]}" \
        -pl "${PAIMON_LINE_MODULE}" -DskipTests test-compile || exit $?
    fi
  fi
fi

prepare_delta_maven() {
  python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" --resolve-project \
    --pom "${DELTA_TEST_POM}" --repository "${SUITE_MAVEN_REPO}" \
    --define "flink.version=${FLINK_VERSION}" --define "delta.version=${DELTA_VERSION}" || return $?
  python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" --repository "${SUITE_MAVEN_REPO}" \
    --artifact org.apache.maven.surefire:surefire-junit-platform:3.5.3 \
    --artifact org.junit.platform:junit-platform-launcher:1.11.4
}

python3 "${REPO_ROOT}/bin/check-flink-suite-classpath.py" "${CLASSPATH_FILE}" "${FLINK_LINE}" || exit $?
if [[ "${SUITE_MODE}" == "prepare" ]]; then
  python3 "${REPO_ROOT}/bin/check-flink-suite-unshaded.py" \
    "${SUITE_ROOT}" "${SUITE_MAVEN_REPO}" "${FLINK_VERSION}" || exit $?
  echo "Preparing the dynamically resolved upstream Surefire runtime..."
  test_runtime_surefire_version=3.2.2
  test_runtime_platform_version=1.11.4
  if [[ "${FLINK_LINE}" == "1.18" ]]; then
    test_runtime_surefire_version=3.0.0-M5
    test_runtime_platform_version=1.9.1
  fi
  test_runtime_artifacts=(
    --artifact "org.apache.maven.surefire:surefire-junit-platform:${test_runtime_surefire_version}"
    --artifact "org.junit.platform:junit-platform-launcher:${test_runtime_platform_version}"
    --artifact "org.apache.httpcomponents:httpclient:4.4.1"
    --artifact "org.apache.paimon:paimon-vfs-hadoop:${PAIMON_VERSION}"
    --artifact "org.apache.maven.plugins:maven-remote-resources-plugin:1.7.0"
  )
  if [[ "${FLINK_LINE}" == "2.2" ]]; then
    test_runtime_artifacts+=(
      --artifact "org.apache.maven.surefire:surefire-junit-platform:3.2.5"
      --artifact "org.junit.platform:junit-platform-launcher:1.13.3"
      --artifact "org.apache.maven.plugins:maven-surefire-plugin:3.5.3"
      --artifact "org.apache.maven.plugins:maven-resources-plugin:3.5.0"
    )
  fi
  if [[ "${KAFKA_CONNECTOR_VERSION}" == "3.2.0" ]]; then
    # Kafka 3.2 loads this published parent before it can compile any of its fixtures.
    test_runtime_artifacts+=(--artifact "org.apache.flink:flink-connector-parent:1.0.0:pom")
  fi
  (
    cd "${FLINK_ROOT}" &&
      MAVEN_USER_HOME="${SUITE_MAVEN_USER_HOME}" python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" \
        --maven "${FLINK_ROOT}/mvnw" \
        --repository "${SUITE_MAVEN_REPO}" \
        "${test_runtime_artifacts[@]}"
  ) || exit $?
  if [[ "${FLINK_LINE}" == "2.2" ]]; then
    echo "Preparing the modern Delta test dependencies before transferring the Maven repository..."
    prepare_delta_maven || exit $?
  fi
  echo "Verifying the prepared upstream test runtime without downloads..."
  flink_mvn ${FLINK_TARGET_PROFILE:+-P${FLINK_TARGET_PROFILE}} -B -ntp -o -s "${MAVEN_SETTINGS}" \
    -Dmaven.repo.local="${SUITE_MAVEN_REPO}" -pl flink-table/flink-table-planner -Dfast \
    '-Dtest=org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testSimpleProject' \
    -DskipTests=false -Dmaven.test.skip=false \
    -Dmaven.test.failure.ignore=false -Dsurefire.failIfNoSpecifiedTests=true \
    -Dflink.forkCountITCase=1 surefire:test@integration-tests || exit $?
  python3 "${REPO_ROOT}/dev/flink-suite/runtime_shards.py" plan \
    --classes "${FLINK_ROOT}/flink-table/flink-table-planner/target/test-classes" \
    --baseline "${REPO_ROOT}/dev/flink-suite/runtime-durations-${FLINK_LINE}.tsv" \
    --contracts "${CONTRACT_FILE}" \
    --output "${SUITE_ROOT}/runtime-shards.json" || exit $?
  echo "Shared Flink ${FLINK_VERSION} and StreamFusion build is ready."
  exit 0
fi
