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
