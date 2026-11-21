#!/usr/bin/env bash

set -uo pipefail
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${1:-runtime}" == iceberg ]]; then
  exec "${REPO_ROOT}/bin/iceberg-suite.sh"
fi

for phase in configure prepare execute audit; do
  source "${REPO_ROOT}/dev/flink-suite/runner/${phase}.sh" || exit $?
done
