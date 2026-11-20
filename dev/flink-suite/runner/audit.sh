#!/usr/bin/env bash

INVENTORY_STATUS=0
if [[ -n "${SQL_INVENTORY_CONFIG}" ]]; then
  python3 "${REPO_ROOT}/dev/flink-suite/sql_inventory.py" \
    --reports "${REPORT_ROOT}" --evidence "${DIAGNOSTIC_ROOT}/sql-inventory" \
    --line "${FLINK_LINE}" --suite "${SUITE_MODE}" \
    --revision "$(git -C "${REPO_ROOT}" rev-parse HEAD)" \
    --output "${SUITE_ROOT}/sql-inventory/${SUITE_MODE}"
  INVENTORY_STATUS=$?
fi

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

SUMMARY_ARGS=("${REPORT_ROOT}" --contracts "${CONTRACT_FILE}" --native-reports "${NATIVE_REPORT_ROOT}" --process-exit "${TEST_STATUS}"
  --audit-output "${DIAGNOSTIC_ROOT}/execution-audit.json")
if [[ -n "${SQL_INVENTORY_CONFIG}" ]]; then
  SUMMARY_ARGS+=(--sql-inventory "${DIAGNOSTIC_ROOT}/sql-inventory" --flink-line "${FLINK_LINE}")
fi
if [[ -n "${RUNTIME_AUDIT_SCOPE}" ]]; then
  SUMMARY_ARGS+=(--runtime-route-scope "${RUNTIME_AUDIT_SCOPE}")
fi
if [[ "${SUITE_MODE}" == "runtime" || "${SUITE_MODE}" == "diagnostic" ]]; then
  SUMMARY_ARGS+=(--maven-result "${DIAGNOSTIC_ROOT}/maven-result.tsv")
  if [[ "${FLINK_LINE}" == "2.2" ]]; then
    SUMMARY_ARGS+=(--xfail "org.apache.flink.table.planner.runtime.batch.sql.CalcITCase#testCurrentDate")
  fi
  if [[ -z "${FLINK_SUITE_TEST:-}" && -z "${FLINK_SUITE_SHARD:-}" ]]; then
    SUMMARY_ARGS+=(--require-contract-prefix org.apache.flink.)
  fi
  if [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then
    SUMMARY_ARGS+=(--require-contract-classes "${SUITE_ROOT}/runtime-shard-classes.txt")
    printf '%s\n' "${shard_selector}" | tr ',' '\n' > "${SUITE_ROOT}/runtime-shard-classes.txt"
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
if [[ "${SUITE_MODE}" == "paimon" && "${FLINK_LINE}" == "1.18" && -z "${FLINK_SUITE_TEST:-}" ]]; then
  while IFS= read -r shared_test; do
    if [[ "${shared_test}" == "!"* ]]; then
      continue
    elif [[ "${shared_test}" == *"#"* ]]; then
      SUMMARY_ARGS+=(--require-test-method "${shared_test}")
    else
      SUMMARY_ARGS+=(--require-test-class "${shared_test}")
    fi
  done < "${PAIMON_118_SHARED_TESTS}"
fi
python3 "${REPO_ROOT}/dev/flink-suite/summarize.py" "${SUMMARY_ARGS[@]}"
SUMMARY_STATUS=$?
if [[ ${SUMMARY_STATUS} -ne 0 ]]; then exit "${SUMMARY_STATUS}"; fi
if [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then
  python3 "${REPO_ROOT}/dev/flink-suite/runtime_shards.py" verify \
    --plan "${SUITE_ROOT}/runtime-shards.json" --shard "${FLINK_SUITE_SHARD}" \
    --audit "${DIAGNOSTIC_ROOT}/execution-audit.json" \
    --output "${DIAGNOSTIC_ROOT}/shard-coverage.json" || exit $?
fi
exit "${INVENTORY_STATUS}"
