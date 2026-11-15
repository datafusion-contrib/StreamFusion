# DATE predicate evaluation

Simple DATE comparisons between columns/literals reuse DataFusion's ordinary
comparison expressions over Arrow Date32. Previously the generic temporal
guard selected Flink-generated Java evaluation for these predicates. The change
follows Comet's comparison builders and Arroyo's direct DataFusion expression
planning; there is no new Rust operator or memory pool.

Admission is limited to two DATE column/literal operands and the six ordinary
comparison operators. Computed temporal expressions, mixed types, timestamps,
TIME and null-safe distinctness keep their existing evaluation paths. NULLs
preserve three-valued logic. DATE is compared as signed days, without a timezone
conversion or calendar computation.

This removes the two registered Java evaluators from each TPC-H Q6/Q14/Q15
date-range filter. The separate SF1 release comparison still lost to stock:
Q6 584.19/684.65 ms, Q14 685.04/868.95 ms and Q15 607.20/691.03 ms
(stock/native medians). All three use identical files/resources, three warmups,
seven alternating retained trials and complete job boundaries. These are local
shared-host observations, not proof of complete-job acceleration. See the
[TPC-H trials and compatibility controls](../benchmarks/tpch.md#native-date-predicate-follow-up).

Batch EXPLAIN now exposes evaluator registration counts and generated-expression
details, since an empty list of named JVM functions does not establish that a
predicate has no Java bridge. Registration is not a runtime callback counter.
