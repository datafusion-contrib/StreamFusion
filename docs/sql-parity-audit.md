# Portable SQL parity audit

`FlinkPortableSqlAuditTest` is a self-contained matrix built from the published shapes in
[issue #106](https://github.com/datafusion-contrib/StreamFusion/issues/106) and
[issue #107](https://github.com/datafusion-contrib/StreamFusion/issues/107). The original
September audit was conducted independently; its SQL corpus, function implementations,
framework-created sources and exact parameter lists are unavailable. These fixtures do
**not** replay that private corpus or establish coverage of its reported 35 setup failures.
The checked-in matrix is the declared, reproducible coverage contract.

Run it against the project's released Flink dependencies and debug native build:

```bash
mvn -pl streamfusion-runtime -am test \
  -Dtest=FlinkPortableSqlAuditTest -Dsurefire.failIfNoSpecifiedTests=false
```

The suite needs no external service, credentials, source checkout or fixed filesystem path.
It also runs in the normal blocking Java CI job. CI retains the JSON result as the
`portable-sql-audit` artifact, including on test failure. This supplements the unchanged
[upstream Flink suite](upstream-flink-suite.md); it does not replace it.

## Fixtures and execution

Every case creates two fresh environments with identical mode, configuration, registered
functions and typed runtime sources. Standard fixtures default to parallelism 1, UTC and
one-phase grouped aggregation; each case records its table-setting overrides. Stock Flink executes first; native-enabled execution
still runs when the host fails. SQL expressions consume source rows rather than constant
`VALUES` expressions that the optimizer could fold away.

`PortableSqlFixtures` defines:

- `test_input`, a typed collection with numbers, strings, JSON documents, wall-time text,
  explicit ordering keys and NULLs; `right_input` supplies typed numeric join keys.
- `nested_input`, with a nullable ROW containing a BIGINT and nullable-element STRING ARRAY.
- `cdc_input`, with an explicit eight-record `+I/-U/+U/-D` sequence. Its final rows are
  `(1, A, 12)` and `(4, B, 8)`. A scalar query compares the complete ordered RowKind stream;
  grouped built-in and custom aggregates must produce final sums `A=12`, `B=8`.
- `test_scalar`, a declared BIGINT function returning `3*v+1`; `test_nested`, a declared
  nested ROW function; `test_split`, a declared table function preserving empty tokens and
  emitting no rows for NULL; and `test_aggregate`, a nullable BIGINT sum with accumulation,
  retraction and merge methods. Lifecycle checks require evaluation inside `open`/`close`
  and balanced opens/closes across both executions.
- Separate functions that fail in initialization and on a negative runtime value. Tests
  compare failure phase, root exception class and diagnostic text, and require native Calc
  substitution for the native-enabled failure attempts.

A scalar UDF in native Calc still uses the existing JVM expression bridge. Its native plan
status does not mean the Java function itself runs in Rust. Arbitrary table/aggregate
functions and the nested ROW-returning scalar currently retain explicit, asserted fallback
reasons. Successful host execution establishes a usable fixture, not native admission.

The recovery case uses a checkpointed source. It emits 32 of 96 rows, waits for a completed
checkpoint containing that prefix, and deliberately fails. One configured restart restores
source offset 32. Both engines must prove the failure and restoration, then produce grouped
sums `1488`, `1520`, `1552`. This checks restored aggregate state as well as source progress;
an identity function or a run that never fails cannot satisfy the test. It does not test
external connector transactions or rescaling.

## Parameters, modes and dialect semantics

The matrix expands the Cartesian product of all declared parameter dimensions and rejects
empty or duplicate variants. Five **new representative** JSON paths are declared:
`$[-1]`, `$[-2147483648]`, `$[--1]`, `$[1 2]`, and `$[`. Each executes in both `lax` and
`strict` mode with explicit default policies. These are not asserted to be the exact five
paths from the unavailable audit. All ten expanded cases must stay in the native pipeline and match Flink. The two valid
negative indexes retain the Rust evaluator; malformed selectors use the generated JVM Calc
and Flink's default policies. Native pipeline classification does not imply that the JSON
computation itself executes in Rust.

Preflight rejects unresolved `$JSON_PATH`, `${...}` and `{{...}}` placeholders, missing
declared tables/functions, and changed table settings before query planning. Temporal
variants run separately under UTC and America/Los_Angeles. Coverage counts expanded cases,
not SQL filenames, parameter templates or successful-only executions.

The dialect fixtures define their own semantics explicitly:

| Published shape | Released-Flink contract tested |
| --- | --- |
| Two-argument `FIRST_VALUE(value, order)` | The original signature remains a host planning failure. A separate declared contract selects the earliest **non-NULL** value by `ord`, with `id` as a deterministic tie-breaker, using `ROW_NUMBER`. Ascending and descending tie-break variants have different known answers. No ordering argument is silently discarded, and this is not claimed equivalent to an unspecified external dialect's NULL/tie rules. |
| Timezone argument to `TO_TIMESTAMP` | The unsupported signature remains a host failure. The supported two-argument call parses a wall-clock TIMESTAMP; explicitly casting it to TIMESTAMP_LTZ interprets it in the declared session zone. Winter and summer instants and NULLs are checked against known values. |
| Timezone argument to `DATE_FORMAT` | The unsupported signature remains a host failure. Runtime epoch values become TIMESTAMP_LTZ and are formatted under the declared session zone. Both zone variants have independently specified expected strings. |
| Implicit STRING/INT join comparison | The original join remains a host failure. The adaptation explicitly casts the string key to INT. Both `'1'` and `'01'` must match integer `1`; casting the integer to STRING would produce a different result. |
| Non-time `ORDER BY` | STREAMING records the host planning limitation; the identical BATCH query succeeds with ordered-result parity. BATCH is outside the native streaming runtime and is never counted as accelerated. |
| OVER consuming grouped updates | STREAMING records the released host's update/delete limitation. No new native semantics are invented to bypass it. |

## Results and accounting

The shared `NativeParity` result comparator recursively compares MAP keys and values,
ROW fields, and arrays by content, including nested binary arrays and NULLs. MAP entry
order is ignored; array order and duplicates, ROW field names and row kinds are retained.
Declared fixture results use the same recursive normalization as collected results, including
list representations of arrays, so nested expected values remain an independent content oracle.
Unordered comparisons count normalized rows rather than sorting their string representations;
map iteration order cannot reorder the comparison, and duplicate result rows remain significant.
Map normalization also retains every entry when distinct Java array keys have equal contents.
`NativeParityTest` covers equal and changed contents without executing an engine, so its
tests do not count as native SQL coverage. Value normalization does not replace declared
SQL type/schema checks or native/fallback route assertions in the audit harness.

The report is `streamfusion-runtime/target/sql-audit/portable-cases.json`. It records each
expanded case's SQL, mode, table settings, required setup, comparison mode, expected outcome,
observed execution, validation result, and both engines' failure phase, root cause, row count,
observed RowKinds, physical operators, substitution count and fallback reasons. The report also retains the standard fixture defaults and recovery configuration. Recovery
observations include checkpoint completion and restored source offsets for both runs.

| Validated outcome | Meaning |
| --- | --- |
| `NATIVE_PARITY` | Both executions succeeded, results matched, and at least one operator was actually substituted. |
| `EXPLICIT_FALLBACK` | Results matched with zero substitutions and the expected concrete fallback reason. |
| `SCAN_ONLY` | Results matched, with zero substitutions/reasons and only an explicitly recognized scan/values/sink plan. |
| `BATCH_HOST_ONLY` | BATCH succeeded and results matched, with no native streaming substitution. |
| `HOST_FAILURE` | Both attempts failed in the expected phase with matching root exception classes and expected diagnostics. This is not successful result parity. |
| `VALIDATION_FAILURE` | An expectation, result, lifecycle or recovery assertion failed. It is never included in validated native-parity counts. |

Unexpected native failures and unclassified plans fail their case. A report retains the
observed execution even when validation fails. The final accounting records declared,
executed, passed and unexecuted counts; an unexecuted declared case also fails the suite.

Comparisons preserve their declared output contract. CDC scalar results compare the ordered
raw changelog. Retracting aggregates compare the materialized multiset. The ordered-value
adaptations declare the first output column as an upsert key: `+U` replaces that key's prior
value, while `-U/-D` must remove the current value. This accommodates Flink's keyed upsert
and native delete/insert encodings without treating `+U` as an additional live row. Batch
sorting compares the ordered rows. Known-answer assertions additionally check fixture
semantics; host/native agreement alone is insufficient for those cases.

## Combined stateful recovery matrix

`StatefulRecoveryMatrixTest` and `StatefulSqlRescaleTest` implement the bounded matrix in
[#250](https://github.com/datafusion-contrib/StreamFusion/issues/250). Its 56 completed-result/failure cases
include INT/BIGINT group keys with DECIMAL(20,2) SUM and STRING COUNT DISTINCT, using memory
and the native RocksDB backend. Twelve changelog records include duplicate 12-KiB strings,
NULLs, removal of the final duplicate, group deletion and recreation. The reference executes
uninterrupted; the native run fails only after checkpoints containing source offsets 4 and 8
have completed, then restores twice. The checkpointed source has a stable UID. The final rows must be `(1, -4.50, 0)` and
`(2, -0.50, 1)`, with a DECIMAL(38,2) sum. Additional cases exercise an updating JOIN
on memory and native RocksDB state. One checkpointed source routes side-tagged changelogs
to the two inputs. Duplicate hot-key rows, 13/14-KiB STRING payloads, DECIMAL(20,2) values,
NULLs and an `l.amount < r.amount` residual cross checkpoints at offsets 6 and 12. A
retract/update pair turns a nonmatching value into a match; later removals and duplicate
insertions must leave exactly three joined rows, including one row with multiplicity two.

Twelve append-only Top-N cases combine TIMESTAMP(9)/TIMESTAMP_LTZ(9), both backends and
UTC/Asia/Shanghai/America/Los_Angeles. Negative-epoch nanoseconds, identical ordering keys,
and both Los Angeles DST transitions cross the two restores at offsets 4 and 8. Rank numbers
and casts between the two timestamp types have independently computed expected values,
so timezone settings affect the checked output. Equal ordering keys have identical payloads;
the fixture does not assume an ordering among distinguishable tied rows. These cases use
physical row limit 5 and logical mini-batch size 3.

Twelve one-second tumbling-window cases use TIMESTAMP(3)/TIMESTAMP_LTZ(3), both backends
and the same three zones. Explicit source watermarks `-1001`, `999` and `3999` follow offsets
4, 8 and 12. Windows close before each checkpoint while later windows retain state across
restore. The five expected sums are `3, 8, 10, 24, 21`, with counts `2, 2, 2, 3, 2`; a final
late row of value 99 arrives after the second restore, before the last watermark, and must
not recreate a closed window or change those results. Both executions must emit the exact explicit
watermark sequence and match the timezone-dependent window bounds.

Eight window combinations execute natively. TIMESTAMP_LTZ under Asia/Shanghai or
America/Los_Angeles retains the existing fixed-offset session-zone fallback, on both
backends. Those four cases still execute the full value/type and repeated-recovery checks,
require the specific fallback reason, and assert that the native window operator is absent.
The evidence records expected routing independently of the comparison result.

Ten Calc-before-GROUP-BY cases exercise INT-to-BIGINT boundary keys, STRING-to-DECIMAL(20,2)
rounding, whitespace, NULLs, Unicode distinct values and retractions across two restores.
The first two source rows are filtered out: the one-row Arrow variants therefore exercise
Calc batches with no surviving rows. Malformed values on filtered rows must never affect
the aggregate. Six cases run native Calc and aggregation with independent physical/logical
batch sizes. Two relational STRING-filter variants retain the representation-sensitive
ordering fallback and still verify the known recovered result.

The remaining two Calc cases deliberately overflow an ordinary STRING-to-DECIMAL cast after
the second restore. Both engines must raise `NumberFormatException` with the overflow
diagnostic during row evaluation; these are expected failures, not successful empty-result
comparisons. The report records the expected outcome separately from routing. Both restored
offsets and zero active sources are asserted for the failing native executions as well.

Four additional GROUP BY cases run in private local deployments with 4 MiB or 8 MiB of
TaskManager task off-heap memory, on both state backends. A per-job setting submitted to
the shared test cluster would not change that cluster's process budget, so the tests assert
the executed pool capacity as well as the requested configuration. These cases retain the
same long strings, two restores, five-row physical limit and three-row logical mini-batches.

The recovery matrix collects results through a test-only checkpointed sink. It copies incoming
rows into operator state, restores them with the job, and publishes the completed attempt's rows
through Flink job accumulators. Host and native executions use the same sink and retain full
changelog/result and exception assertions. Teardown requires no collector initialization, avoiding
Flink's collect-sink close-before-initialization crash
([FLINK-39330](https://github.com/apache/flink/pull/28474)), which can kill the private cluster's
only TaskManager and turn an expected overflow into a five-minute slot timeout. This workaround
uses released Flink APIs and does not modify the deployed engine or its Flink dependency.

Every matrix case reuses the suite's native cleanup check after execution. It waits briefly
for asynchronous cleanup and requires no live native handles, zero Arrow allocator bytes
and zero task off-heap reservations; all three observations are recorded alongside source
cleanup. The shared check now also enforces zero task reservations after other test cases.
Both normal completion and the two expected decimal failures pass these checks. The check
measures live ownership/reservations, not whether the native allocator returns pages to the OS.

Four separate cancellation cases execute grouped SUM and COUNT DISTINCT on both engines and
both state backends. The native jobs first restore the source at offset 32. All jobs then
hold the source open after 96 rows until a checkpoint containing the full input completes;
only then does the test cancel through the job client. It verifies terminal CANCELED status,
zero active sources, and the same native ownership cleanup checks. Native cases also require
a live native handle before cancellation. These are lifecycle checks, recorded with the
`CANCELLATION_CLEANUP` outcome; they do not claim complete-result parity for cancelled jobs.
Completed source offsets are included in the report.

Each case independently checks materialized results against known answers, resolved result
types, the required native operator plan, both restored offsets and failure injections, completed
checkpoint evidence, and zero active sources after collection. Route assertions cannot
prevent the result assertions from running. The shared recovery helper still supports the
original 32-of-96-row portable audit with one restart in both engines.

Run the cases and their helper regression suite with:

```bash
mvn -pl streamfusion-runtime -am test \
  -Dtest=StatefulRecoveryMatrixTest,FlinkPortableSqlAuditTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Add `-Pflink-1.18` for the other released dependency line. The matrix writes
`streamfusion-runtime/target/sql-audit/stateful-recovery.json`, including configuration,
result comparison, result types, native plan, fallback reasons and recovery observations,
even when a validation assertion fails. CI retains this file with the existing portable SQL
audit artifact. All 56 recovery cases (48 native successes, six explicit fallbacks and two expected native
failures), four cancellation cases, four transpose-configuration tests and 38 portable-audit regressions pass on each
of Flink 2.2.1 and 1.18.1 (102 tests per release).

The required matrix runs each row below with memory and native RocksDB state, at parallelism
1. Arrow row limits and Flink logical mini-batch sizes are independent:

| Operator / keys | Arrow row limit / logical mini-batch rows (0 means disabled) |
| --- | --- |
| GROUP BY / INT | 1024/0, 1/0, 5/3, 64/3; extra 5/3 cases with 4 MiB and 8 MiB budgets |
| GROUP BY / BIGINT | 1024/0 |
| Updating JOIN / INT | 1024/0, 1/3, 5/0, 64/3 |
| Top-N / INT, TIMESTAMP(9) or TIMESTAMP_LTZ(9) ordering | 5/3 in each of the three zones |
| TUMBLE / TIMESTAMP(3) or TIMESTAMP_LTZ(3) event time | 5/0 in each zone; LTZ outside UTC expects fallback |
| Calc → GROUP BY / INT-to-BIGINT keys | 1/0, 5/3, 64/3 native; 1/3 string fallback; 1/0 expected overflow failure |

The job-scoped `streamfusion.transpose.batchRows` option controls physical row-to-Arrow
batches. Post-exchange coalescing is disabled in these cases so it cannot recombine the
selected boundaries. Logical mini-batch latency is one hour: count triggers and checkpoint
flushes determine the tested bundles. A separate serialized-operator test checks actual
emission sizes for limits 1, 5 and 64, including a partial batch flushed by a watermark.
The default row limit remains 1024 for other jobs.

The opt-in `stateful-recovery-stress` Maven profile adds 12 seeded GROUP BY cases. Each
executes 8,704 changelog records: 4,096 insertions skewed toward a hot key, the same records
retracted in a seeded shuffled order, and four rows recreating each of 128 groups. Inputs
include long Unicode strings, duplicate/null DISTINCT values, null amounts and positive and
negative DECIMAL(20,2) limits. A separately constructed final answer requires SUM 1.00 and
two distinct strings for every recreated group.

Seed 20260927 uses INT keys and a verified 16 MiB budget; seed 20260928 uses BIGINT keys and
32 MiB. Both run on memory and native RocksDB with Arrow limits 1, 127 and 4096. Logical
mini-batches are respectively 0/257/257 for the first seed and 257/0/0 for the second.
Native execution restores three completed checkpoints at source offsets 4096, 6144 and
8192, spanning populated state, partial retraction and complete group removal. Every case
reuses the known-result, host/native type parity, route, restored-offset and ownership
cleanup assertions, and records its seed, row count and configuration in the audit JSON.
All 12 stress cases pass on each of released Flink 2.2.1 and 1.18.1. They are disabled in
the required CI matrix and impose no timing assertions.

Run just the stress cases with:

```bash
mvn -Pstateful-recovery-stress -pl streamfusion-runtime -am test \
  '-Dtest=StatefulRecoveryMatrixTest#seededGroupedStress' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Add `-Pflink-1.18,stateful-recovery-stress` instead to select the other released line.
`StatefulSqlRescaleTest` adds two GROUP BY rescale cases, one for each backend. An
uninterrupted host run processes 768 deterministic changelog rows. Native jobs retain
completed checkpoints at offsets 256 and 512 and restore them while changing parallelism
1→3→2 (maximum 128), then process the remaining suffix. Across 128 INT-keyed groups,
long duplicate strings and DECIMAL values cross both restores; suffix retractions remove
the last prefix DISTINCT value, while NULL amounts and duplicate replacement strings remain.
Known final sums/counts and resolved types must match the host.

These cases reuse the existing checkpointed file sink and released-line checkpoint
configuration helpers. Explicit transformation UIDs keep operator identities stable across
all three deployments, and the source restores its recorded offset. Checkpoint metadata
must contain nonempty native aggregate state for every subtask, with exactly that subtask's
assigned key groups. Committed file output materializes the complete changelog across
restores; it includes pre-checkpoint output and therefore detects loss or replay in both
operator and sink state. While a job is running, the sink may rename a hidden
`.part-*.inprogress.*` file after directory enumeration but before its attributes are read.
The output visitor ignores only `NoSuchFileException` for those temporary names; missing
committed files, permission errors and other I/O failures still fail the test. A regression
case checks this distinction and verifies that only committed changelog records contribute
to materialization. Jobs wait for checkpoint completion before cancellation, and
assert source/native ownership cleanup after every deployment. The separate
`stateful-rescale.json` artifact records configuration, source recovery, operator IDs,
key-group ranges, types, plans and cleanup. Failed cases retain configuration and the
completed stages' evidence.

Run these cases with `-Dtest=StatefulSqlRescaleTest` and either released Flink profile.
Both cases pass on Flink 2.2.1 and 1.18.1. Together with the matrix, transpose configuration
and portable SQL regressions, each release passes 104 tests; the opt-in stress method is skipped.
The rescale cases cover this grouped-state workload with Arrow limit 5 and logical
mini-batches of 3. JOIN, Top-N, window and Calc matrix cases still restore at fixed
parallelism; these tests do not claim their rescaling coverage or cross-version state
compatibility.

Each matrix scenario now owns a local test cluster with Flink's retained in-memory metrics
reporter, following the existing operator-metrics tests. The cluster lives through both
reference and native executions so counters from completed, failed and restored attempts
remain available until evidence is recorded. Requested task budgets configure that cluster;
no per-job setting is assumed to resize a shared TaskManager.

The reports record native operator names, job IDs, subtask/attempt identifiers and existing
`numRecordsIn`/`numRecordsOut` counters. Every required native compute operator must have
consumed rows; expected fallback cases must have no native operator metrics. Cancellation
and rescale cases also require consumed-row evidence before cancellation. These checks run
independently of value/type and route assertions. Counter observations belong to execution
evidence, not configuration. Metrics retain per-attempt observations rather than claiming
that their sum is a universally replay-free row total. Cleanup checks still run while the
cluster is alive, before its metrics and other resources are closed. This reuses current
Flink I/O accounting; broader accounting coverage remains tracked by #168.

With metrics enabled, all 104 required checks and 12 opt-in stress cases pass on each
released Flink line (116 total per release), including the portable SQL and transpose
configuration regressions. Both execution-counter artifacts were inspected.

Java CI failures retain Surefire reports, fork dump streams, and JVM fatal-error logs in the
`java-failure-diagnostics-<Flink line>` artifact for seven days. These accompany the console
log when a native abort prevents the crashing test from completing its XML report.
