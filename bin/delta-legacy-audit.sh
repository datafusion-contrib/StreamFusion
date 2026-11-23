#!/usr/bin/env bash
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly AUDIT_ROOT="${DELTA_AUDIT_ROOT:-${REPO_ROOT}/.flink-suite/delta-legacy-1.18}"
readonly DELTA_ROOT="${AUDIT_ROOT}/delta-3.3.3"
readonly AUDIT_POM="${REPO_ROOT}/dev/flink-suite/delta-legacy/pom.xml"
readonly MAVEN_SETTINGS="${REPO_ROOT}/dev/flink-suite/settings.xml"
readonly DELTA_COMMIT=0ce5dd9f7d02cbc1cd74b7333045784f440636a1

mkdir -p "${AUDIT_ROOT}"
if [[ ! -d "${DELTA_ROOT}/.git" ]]; then
  git clone --depth 1 --filter=blob:none --sparse --branch v3.3.3 \
    https://github.com/delta-io/delta.git "${DELTA_ROOT}"
  git -C "${DELTA_ROOT}" sparse-checkout set connectors/flink
fi
if [[ "$(git -C "${DELTA_ROOT}" rev-parse HEAD)" != "${DELTA_COMMIT}" ]] \
    || [[ -n "$(git -C "${DELTA_ROOT}" status --porcelain)" ]]; then
  echo "The Delta audit requires a clean canonical v3.3.3 checkout: ${DELTA_ROOT}" >&2
  exit 2
fi

python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" --offline-first --pom "${AUDIT_POM}" --resolve-project
python3 "${REPO_ROOT}/bin/ci-maven-dependencies.py" --offline-first \
  --artifact org.apache.maven.surefire:surefire-junit-platform:3.5.3 \
  --artifact org.junit.platform:junit-platform-launcher:1.10.2

mvn -B -ntp -o -s "${MAVEN_SETTINGS}" -gs "${MAVEN_SETTINGS}" -f "${AUDIT_POM}" \
  "-Ddelta.source.root=${DELTA_ROOT}" "-Daudit.output=${AUDIT_ROOT}/target" test

if [[ -n "$(git -C "${DELTA_ROOT}" status --porcelain)" ]]; then
  echo "The Delta audit changed its upstream source checkout." >&2
  exit 2
fi
