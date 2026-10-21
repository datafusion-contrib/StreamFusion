# Benchmark evidence index

Start with the [benchmark method and headline results](../benchmarks.md). This index
connects complete comparisons, component measurements and retained investigations.
Each report applies to its recorded revision, environment and measurement boundary;
a historical result is not a measurement of the current main branch.

## Choose the right document

| Question | Source |
| --- | --- |
| Does this query accelerate, and why does it fall back? | [Operator coverage](../operators/index.md), [connector coverage](../connectors/index.md) and [configuration](../configuration.md) |
| How does a current optimization work? | [Optimization ledger](../optimizations/index.md) |
| How do I reproduce a native kernel measurement? | [Criterion suites](native-criterion.md) |
| How do I compare complete Flink jobs? | [Benchmark methodology](../benchmarks.md) and [performance audit](performance-audit.md) |
| What did a particular SQL audit execute? | [SQL invocation inventory](../sql-inventory/index.md) and [upstream-suite guide](../upstream-flink-suite.md) |

Coverage pages describe the current supported contract. Recorded timings, profiles
and SQL inventories are evidence for their stated run, including its restrictions.

## Complete comparisons

These reports retain the corresponding workload, completion protocol, all trial
times and validation evidence. Compare within a report before comparing across
hosts, connectors, backends, exact SQL and explicitly labelled expression variants.

| Workload | Report | Scope |
| --- | --- | --- |
| Kafka Nexmark headline | [Benchmark table and reproduction](../benchmarks.md#nexmark-parallelism-4) | Historical headline and its sampling convention |
| Iceberg to Iceberg | [After boundary optimization](iceberg-optimized-20261017/README.md) | Complete repeated matrix, with [previous-matrix comparison](iceberg-optimized-20261017/previous-comparison.md) |
| Earlier Iceberg matrix | [Before boundary optimization](iceberg-final-20261017/README.md) | Earlier complete cohort; its [previous I/O comparison](iceberg-io-baseline-20261017/README.md) is separate from the stock-Flink matrix |
| Kafka to Iceberg | [Repeated comparison](kafka-iceberg-final-20261017/README.md) | Separate [q0/q19 CPU diagnostics](kafka-iceberg-final-20261017/profiles/README.md) are excluded from its aggregate |
| Kafka to Paimon | [Repeated comparison](kafka-paimon-final-20261017/README.md) | Includes a [matched Iceberg comparison](kafka-paimon-final-20261017/iceberg-comparison.md) with connector-setting differences retained |
| Scalar functions | [Recorded function workloads](scalar-functions.md) | Dated cohorts with full-job conversion costs and current coverage links |
| Basic ARRAY searches | [Native search and decimal-entry comparison](../optimizations/native-array-search.md) | Original/generated versus Rust kernels, large-decimal controls and remaining standalone losses; all 260 measured trials retained |
| Integer/DECIMAL ARRAY extrema | [Released reduction kernels](../optimizations/native-array-search.md#integer-and-decimal-array-extrema) | Generated versus released DataFusion MIN/MAX with both transposes; 120 measured trials, including tied cases and wide-decimal standalone losses |
| Basic collection edits | [Offset-based collection edits](../optimizations/native-collection-edits.md) | Prior generated evaluator and stock Flink versus Rust, with both transposes and complete measured trials |
| Decimal expressions | [Exact arithmetic comparison](decimal-expressions.md) | Recorded expression workloads and argument/type restrictions |
| Other focused workloads | [Append Top-1](append-top1.md), [basic types](basic-type-coverage.md), [floating GROUP BY](floating-group-by.md), [Flink 1.18](flink118-nexmark.md) | Separate fixtures and environments; not interchangeable headline cells |

## Component measurements and diagnostics

[Rust feature admission, 2026-10-19](rust-feature-admission-2026-10-19.md) starts the clock
inside Rust on prepared Arrow and compares matching stock Flink Java/RowData execution.
The [earlier Java-wrapper audit](feature-admission-2026-10-19.md) retains its five-switch
inventory, CPU profiles and trials as diagnostics; it includes JNI/C Data handoffs.

File-task loops and isolated routing measurements exclude parts of a streaming
job. Profiled durations and one-pair preflights do not replace repeated, unprofiled
complete-job observations. The linked reports explain which comparisons are valid.

| Investigation | Evidence and interpretation |
| --- | --- |
| Iceberg file-task development loop | [Method](iceberg-io.md), [first boundary investigation](iceberg-boundary-20261017/README.md) and its retained cohorts |
| Iceberg selection ownership and dynamic routing | [Second boundary investigation](iceberg-boundary-round2-20261017/README.md); includes an abandoned combined-size prototype and earlier generator references corrected in the checked comparison |
| Iceberg writer-only v2/v3 diagnostics | [Format v2](iceberg-dynamic-v2-20261017/README.md) and [format v3](iceberg-dynamic-v3-20261017/README.md); these are not Nexmark matrix cells |
| Iceberg state/checkpoint and I/O investigations | [Profile guide and cohort reports](iceberg-profile-20261017/README.md); profiled, compressed and previous-I/O runs stay separate |
| Iceberg correctness validation | [Recorded test inventory and reports](iceberg-validation-20261017/README.md); Criterion smoke checks fixtures, not throughput |
| Superseded Iceberg completion protocol | [Interrupted canonical-export sweep](iceberg-canonical-20261017/README.md); excluded from final aggregates |

## Experimental and rejected approaches

These materials preserve the reasoning and reproduction inputs for experiments.
Their presence in the documentation does not enable a production path. Current
admission remains on the operator pages; rejected decisions also live in the
repository's `.claude/wontdos/` directory.

| Experiment | Outcome and evidence |
| --- | --- |
| Hyperbolic compiler specialization | [Prototype comparison](hyperbolic-specialization.md): isolated kernel gains did not establish broad complete-job admission |
| Reserved primitive entry | [Rejected entry-only experiment](primitive-reserved-entry-experiment.md): mixed full-job changes did not resolve typed math regressions |
| Primitive and binary boundary experiments | [Criterion diagnostics](native-criterion.md#narrow-primitive-exit-rejection) and [scalar BINARY exit experiments](scalar-functions.md#rejected-manual-binary-exit-projection) retain their unfavorable results |

## Maintaining the evidence

- Link a new report from its topic page or this index, and link each cohort's
  human-readable result from its parent report. Global navigation contains the
  topic entry points rather than every repetition.
- Record the revision and library hashes, released dependencies, host, resources,
  input, measured boundary, warmups, repetitions and engine order. State any
  difference between the stock and preceding-StreamFusion controls.
- Keep raw observations, ranges, native-execution witnesses, validation results
  and reproduction inputs alongside the conclusion. Retain regressions, failed
  attempts and superseded cohorts with an explicit interpretation.
- Keep profile runs, changed configurations and incomplete sweeps out of a
  primary aggregate unless its stated method includes them. Preserve the
  original cohort when a method or implementation changes.
- Before moving or deleting evidence, update document links and any verifier
  that reads its paths. Large artifacts need a durable versioned location,
  checksums and retrieval instructions before their repository copy is removed;
  expiring CI artifacts are not a sole archive.
