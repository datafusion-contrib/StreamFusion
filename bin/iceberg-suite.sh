#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_VERSION="${FLINK_VERSION:-2.2.1}"
if [[ "${FLINK_VERSION}" != 2.2.* ]]; then
  echo "The released Iceberg adapter and suite target Flink 2.2 only." >&2
  exit 2
fi
SUITE_ROOT="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}/2.2/iceberg"
mkdir -p "${SUITE_ROOT}"

# The upstream test artifacts are published alongside production 1.12.0; do not build Iceberg.
mvn -B -ntp -f "${REPO_ROOT}/dev/flink-suite/iceberg/pom.xml" \
  "-Dflink.version=${FLINK_VERSION}" \
  "-Diceberg.test.output=${SUITE_ROOT}/stock/target" test

native_options=()
payload="${FLINK_SUITE_ROOT:-${REPO_ROOT}/.flink-suite}/2.2/streamfusion-source/native/target/debug"
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
mvn -B -ntp -f "${REPO_ROOT}/pom.xml" -Piceberg,iceberg-upstream \
  -pl streamfusion-iceberg -am "-Dflink.version=${FLINK_VERSION}" \
  -Dmaven.javadoc.skip=true "${native_options[@]}" test

python3 "${REPO_ROOT}/dev/flink-suite/iceberg/verify_reports.py" \
  "${SUITE_ROOT}/stock/target/surefire-reports" \
  "${REPO_ROOT}/streamfusion-iceberg/target/surefire-reports"
mkdir -p "${SUITE_ROOT}/native/target"
cp -R "${REPO_ROOT}/streamfusion-iceberg/target/surefire-reports" "${SUITE_ROOT}/native/target/"
