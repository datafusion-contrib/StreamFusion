#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_VERSION="${FLINK_VERSION:-2.2.1}"
if [[ "${FLINK_VERSION}" != 2.2.* ]]; then
  echo "The released Iceberg adapter and suite target Flink 2.2 only." >&2
  exit 2
fi
SUITE_BASE="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}"
SUITE_ROOT="${SUITE_BASE}/2.2/iceberg"
STOCK_POM="${REPO_ROOT}/dev/flink-suite/iceberg/pom.xml"
MAVEN_SETTINGS="${REPO_ROOT}/dev/flink-suite/settings.xml"
# The shared Flink repository replaces the released planner with an unshaded build.
# Iceberg's stock and native comparison both require the published planner.
ICEBERG_MAVEN_REPO="${SUITE_BASE}/iceberg-m2"

if [[ ${1:-test} == prepare ]]; then
  resolver=(python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py"
    --repository "${ICEBERG_MAVEN_REPO}")
  "${resolver[@]}" --resolve-project --pom "${STOCK_POM}" --define "flink.version=${FLINK_VERSION}"
  "${resolver[@]}" --pom "${REPO_ROOT}/pom.xml" \
    --profile iceberg,iceberg-upstream --projects streamfusion-iceberg --also-make \
    --define "flink.version=${FLINK_VERSION}"
  "${resolver[@]}" \
    --artifact org.apache.maven.surefire:surefire-junit-platform:3.5.3 \
    --artifact org.apache.maven.surefire:surefire-junit-platform:3.2.5 \
    --artifact org.junit.platform:junit-platform-launcher:1.11.4 \
    --artifact org.junit.platform:junit-platform-launcher:1.10.2
  exit
fi

mkdir -p "${SUITE_ROOT}"
maven=(mvn -B -ntp)
if [[ "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" == true ]]; then
  maven+=(-o -s "${MAVEN_SETTINGS}" -gs "${MAVEN_SETTINGS}"
    "-Dmaven.repo.local=${ICEBERG_MAVEN_REPO}")
fi

# The upstream test artifacts are published alongside production 1.12.0; do not build Iceberg.
"${maven[@]}" -f "${STOCK_POM}" \
  "-Dflink.version=${FLINK_VERSION}" \
  "-Diceberg.test.output=${SUITE_ROOT}/stock/target" test

native_options=()
payload="${SUITE_BASE}/2.2/streamfusion-source/native/target/debug"
if [[ "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" == true ]]; then
  for library in streamfusion streamfusion_iceberg; do
    if [[ ! -f "${payload}/lib${library}.so" && ! -f "${payload}/lib${library}.dylib" ]]; then
      echo "Prepared Iceberg suite payload is missing: ${library}" >&2
      exit 2
    fi
  done
  native_options=(-Dnative.build.skip=true "-Dnative.resource.directory=${payload}"
    "-Dsf.extraJvmArgs=-Djava.library.path=${payload}")
fi
"${maven[@]}" -f "${REPO_ROOT}/pom.xml" -Piceberg,iceberg-upstream \
  -pl streamfusion-iceberg -am "-Dflink.version=${FLINK_VERSION}" \
  -Dmaven.javadoc.skip=true "${native_options[@]}" test

python3 "${REPO_ROOT}/dev/flink-suite/iceberg/verify_reports.py" \
  "${SUITE_ROOT}/stock/target/surefire-reports" \
  "${REPO_ROOT}/streamfusion-iceberg/target/surefire-reports"
mkdir -p "${SUITE_ROOT}/native/target"
cp -R "${REPO_ROOT}/streamfusion-iceberg/target/surefire-reports" "${SUITE_ROOT}/native/target/"
