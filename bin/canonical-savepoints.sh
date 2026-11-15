#!/usr/bin/env bash
set -euo pipefail

# The upstream permission-failure fixture must be able to make its directory unwritable.
if [[ "${EUID}" == 0 && -r /proc/self/status ]]; then
  capability_bound="$(awk '$1 == "CapBnd:" { print $2 }' /proc/self/status)"
  if (( (16#${capability_bound} & 6) != 0 )); then
    if ! command -v setpriv >/dev/null; then
      echo "Run savepoint tests as a non-root user, or install util-linux for setpriv." >&2
      exit 2
    fi
    exec setpriv --bounding-set=-dac_override,-dac_read_search "${BASH_SOURCE[0]}" "$@"
  fi
fi

readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly requested="${1:-all}"
if [[ $# -gt 0 ]]; then shift; fi
case "${requested}" in
  sql|runtime) suites=("${requested}") ;;
  all) suites=(sql runtime) ;;
  *) echo "Usage: $0 [sql|runtime|all] [Maven arguments...]" >&2; exit 2 ;;
esac

mvn -B -ntp -f "${repo_root}/dev/flink-suite/agent/pom.xml" test
mvn -B -ntp -f "${repo_root}/dev/flink-suite/agent/pom.xml" \
  -Dstreamfusion.flink-suite.canonical=true -Dtest=CanonicalRestoreExecutionTest package
status=0
for suite in "${suites[@]}"; do
  selectors="${CANONICAL_UPSTREAM_TESTS:-$(paste -sd, "${repo_root}/dev/canonical-savepoints/${suite}-tests.txt")}"
  for engine in stock native; do
    profiles=canonical-savepoints
    if [[ "${engine}" == stock ]]; then profiles="${profiles},canonical-stock"; fi
    reports="${repo_root}/dev/canonical-savepoints/target/reports/${engine}/${suite}"
    native_reports="${repo_root}/dev/canonical-savepoints/target/native-execution/${suite}"
    mkdir -p "${reports}" "${native_reports}"
    find "${reports}" -maxdepth 1 -type f -delete
    if [[ "${engine}" == native ]]; then
      find "${native_reports}" -maxdepth 1 -type f -name '*.tsv' -delete
    fi
    if mvn -B -ntp -f "${repo_root}/pom.xml" -P"${profiles}" \
        -pl dev/canonical-savepoints -am -Dsf.runtime.tests.skip=true \
        "-Dcanonical.suite=${suite}" "-Dcanonical.upstream.tests=${selectors}" "$@" test; then
      test_status=0
    else
      test_status=$?
    fi
    summary=("${reports}" --process-exit "${test_status}" --audit-output "${reports}/execution-audit.json")
    if [[ "${engine}" == native ]]; then
      summary+=(--contracts "${repo_root}/dev/flink-suite/agent/src/main/resources/canonical-native-execution.tsv"
        --native-reports "${native_reports}")
    fi
    IFS=',' read -r -a selected <<< "${selectors}"
    for selector in "${selected[@]}"; do
      selected_class="${selector%%#*}"
      if [[ "${selected_class}" == *"*"* || "${selected_class}" == *"?"* || "${selected_class}" == "!"* ]]; then continue; fi
      summary+=(--require-test-class "${selected_class}")
      if [[ "${engine}" == native ]] && awk -F '\t' -v name="${selected_class}#testRestore" \
          '$1 == name { found=1 } END { exit !found }' \
          "${repo_root}/dev/flink-suite/agent/src/main/resources/canonical-native-execution.tsv"; then
        summary+=(--require-contract-prefix "${selected_class}#")
      fi
    done
    if ! python3 "${repo_root}/dev/flink-suite/summarize.py" "${summary[@]}"; then
      status=1
    fi
  done
done
exit "${status}"
