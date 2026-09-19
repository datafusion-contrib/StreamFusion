#!/usr/bin/env bash
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly AUDIT_ROOT="${DELTA_AUDIT_ROOT:-${REPO_ROOT}/.flink-suite/delta-legacy-1.18}"
readonly DELTA_ROOT="${AUDIT_ROOT}/delta-3.3.3"
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

mvn -B -ntp -f "${REPO_ROOT}/dev/flink-suite/delta-legacy/pom.xml" \
  "-Ddelta.source.root=${DELTA_ROOT}" "-Daudit.output=${AUDIT_ROOT}/target" test

if [[ -n "$(git -C "${DELTA_ROOT}" status --porcelain)" ]]; then
  echo "The Delta audit changed its upstream source checkout." >&2
  exit 2
fi
