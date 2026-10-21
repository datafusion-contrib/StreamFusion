# Evaluate collection edits on Arrow batches

The existing collection extension can execute verified ARRAY_APPEND signatures
without rebuilding per-row JVM containers. The kernel uses the list offsets to
copy a row's visible child range and its corresponding scalar element through
Arrow's mutable array builder. It dispatches storage types in Arrow rather than
converting values to DataFusion scalars. A NULL container stays NULL; an empty
container becomes a one-element array, and a NULL appended element is retained.
Sliced offsets and DECIMAL precision/scale remain intact.

Flink's getter, string representation, NOT NULL-element and failing-expression
restrictions remain enforced by the planner. This replaces evaluation inside the
existing extension; its configuration is unchanged. Standalone conversion costs
and the accepted regressions remain tracked by
[issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

The `array_edits` Criterion suite invokes the production compiled Calc with
independently constructed expected arrays, sliced inputs, empty batches, short
and long arrays and nullable parents/elements/needles. Allocation requests and
shared output buffers are recorded separately from elapsed time. Whole-job
measurements retain both row/Arrow transposes, JNI, startup and the rowwise sink.

## Whole-job measurements

Baseline revision `8798b8e6`; Apple M4 Pro (14 cores), macOS arm64,
JDK 17.0.12 and Flink 2.2.1; release Rust with mimalloc and no RocksDB.
Parallelism one, 2 GiB heap, rowwise source and blackhole sink. Every
cohort uses a fresh JVM, two warmups per engine and five alternating measured
trials; both transposes, normal copies, JNI, planning and startup are included.
No task builds or other tests overlap timing; desktop activity is uncontrolled
and CPUs are not pinned. The baseline ran before these functions were changed.
Times are medians in seconds; negative deltas mean less elapsed time. Baseline
stock controls remain visible to expose drift. These are complete-job results,
including startup, rather than isolated sustained kernel throughput.

| Function | Rows | Prior stock | Prior SF | New stock | New SF | Versus prior SF | Versus paired stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ARRAY_APPEND | 2,000,000 | 0.558645 | 0.674472 | 0.522853 | 0.448795 | -33.46% | -14.16% |


[ARRAY_APPEND trials and ranges](../benchmarks/collection-array-append-2026-10-20.csv),
[fixture and library checksums](../benchmarks/collection-array-append-2026-10-20.json),
and [90 allocation probes](../benchmarks/collection-array-append-allocations-2026-10-20.csv)
retain all observations. This short-array BIGINT case improves on both controls;
it does not establish a gain for every type or width.

Validation: six Flink 2.2.1 runtime SQL tests cover BOOLEAN, all integer widths
and exact precision-38 DECIMAL, including NULL appended elements, empty arrays,
multiple batches, filters and all-filtered batches. The Criterion suite verifies
90 independent expected results; the scalar registry covers opcode 182 in all
nine null/Unicode/batch profiles. Full stateful recovery tests are outside this
stateless scalar change's local validation.

## Prepending elements

ARRAY_PREPEND uses the same checked offset builder, placing the scalar before
the visible child range. It retains the same basic nullable-type admission and
NULL-container/element rules. The SQL and Criterion fixtures independently check
the resulting element order, including sliced and empty inputs.

## Removing elements

ARRAY_REMOVE compares matching basic elements in Arrow and gathers retained indices
in original order. Unlike DataFusion/Spark removal, a NULL needle removes all NULL
elements rather than producing NULL. NULL containers stay NULL. Repeated matches,
sliced buffers, exact wide DECIMAL and all-filtered batches have parity fixtures.

## Concatenating arrays

ARRAY_CONCAT copies visible child ranges in argument order and intersects parent
validity. Every argument must be present, matching Flink rather than DataFusion's
NULL-skipping concatenation. The builder works on Arrow storage for matching
basic nullable element types; it does not reconstruct individual JVM containers.
Variadic and independently NULL operands are covered by runtime SQL tests.

## Union of arrays

ARRAY_UNION reuses DataFusion 54's released stable-union implementation for matching
nullable basic types. It preserves first occurrence across the left input then the
right input, including one NULL element, and returns NULL if either container is
NULL. The planner retains generated evaluation for other representations and
fallible operands. SQL fixtures exercise distinct left/right lengths and both
operand orders rather than assuming that a set operation may reorder its result.

## Stable array difference

ARRAY_EXCEPT dispatches once to typed integer, decimal or Boolean values. A reused
hash set records the right input and then each emitted left value, so NULL is a
first-class key, duplicates disappear and left occurrence order is preserved.
Only the selected left values are gathered; per-row JVM objects and serialized
row keys are unnecessary. Independent expected results and differing operand
lengths exercise NULLs, duplicate values and stable order.

## Left-ordered intersection

ARRAY_INTERSECT builds a reusable typed lookup set from the right input and probes
the left in occurrence order, removing each matched key after emission. This
preserves Flink's left order and NULL equality without another deduplication pass.
DataFusion 54 instead probes the longer input, which can reorder a shorter left
array; that implementation cannot be used unchanged. Tests cover both relative
input lengths, duplicates, NULL keys, empty arrays and exact DECIMAL values.

## Sorting verified arrays

ARRAY_SORT reuses DataFusion 54 for nullable integer/DECIMAL arrays whose Flink
comparator is verified safe: fewer than 32 elements, or a native ARRAY_DISTINCT
result. Literal Boolean options map explicitly to ASC/DESC and NULLS FIRST/LAST;
the two-argument Flink form puts NULL first exactly when ascending is true.

Unrestricted arrays retain generated Flink evaluation. Flink's comparator returns
-1 on equal non-NULL values; long duplicate-bearing arrays can therefore throw
`IllegalArgumentException: Comparison method violates its general contract!` in
Java TimSort. Substituting a total-order Arrow sort would change that observable
behavior. A 64-element runtime SQL reproducer protects this failure path. Runtime
and nullable options, strings, nested types and fallible operands also retain
Flink evaluation. This is deliberately partial native admission.

## Extracting a singleton

ELEMENT selects a nullable child index for each basic array and gathers it with
Arrow. Empty and NULL arrays produce NULL; multiple elements produce the released
exception class and message, carried through the existing typed-error
bridge. The planner counts ELEMENT as a failure site so other failing expressions
cannot be reordered across rows. The scalar registry covers empty, singleton,
NULL-element and NULL-parent arrays; SQL tests cover multi-element failures,
all-filtered inputs and competing arithmetic errors.

## Constructing scalar arrays

Projected runtime ARRAY expressions reuse the existing native scalar constructor
for matching nullable Boolean, integer, DECIMAL and temporal elements. The released
DataFusion builder interleaves columns into flat Arrow child storage. The existing
compact/expanded timestamp normalization retains Flink precision and pre-epoch
behavior. No new JNI protocol or container sidecar is introduced. Fallible,
STRING/nested and NOT NULL-element projections retain their previous evaluator.
SQL fixtures verify typed NULL literals, exact wide decimals, DATE/TIME and
TIMESTAMP/LTZ at millisecond and nanosecond precision.

## Constructing maps from arrays

MAP_FROM_ARRAYS shares the flattened input buffers when their offsets agree and
aligns visible ranges otherwise. Duplicate and NULL keys are retained, both
parents must be present, and length mismatch uses Flink's exception class/message.
The existing List-of-entry-Struct carrier permits nullable keys without unsafe
Arrow construction or a new ownership protocol. Its marker lives on the child
field so expression type inference preserves it. Only basic nullable key/value
types are admitted; representation-sensitive inputs retain generated evaluation.

## Shared map key views

MAP_KEYS reuses Arrow entry-key buffers, offsets and parent validity for ordinary
maps and verified map-list carriers. It follows the released DataFusion/Comet
projection pattern while accepting Flink's nullable-key representation. No
per-entry gathering or JVM container reconstruction is necessary.

MAP_VALUES uses the same verified basic-map admission and shared-buffer view, selecting the value child without changing NULLs or entry order.

## Released ELEMENT exception types

The Flink compatibility profile selects ELEMENT's released exception class without
an extra per-row argument, lookup or reflective class detection. Flink 1.18 uses
RuntimeException and Flink 2.2 uses TableRuntimeException; both use the exact same
array kernel and message. This changes only the error path and does not alter the
successful kernel or the complete-job measurements above.

## ARRAY_PREPEND complete-job evidence

Query: `ARRAY_PREPEND(arr,CAST(idx AS BIGINT))`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.579178 | 0.571242-0.585162 |
| baseline streamfusion | 1.030478 | 1.010875-1.038272 |
| candidate stock | 0.580153 | 0.567207-0.587232 |
| candidate streamfusion | 0.603231 | 0.594722-0.607353 |

Elapsed time changes by -41.46% versus the prior evaluator and +3.98% versus paired stock Flink. The remaining stock regression is retained under #392; native coverage is not a universal standalone speedup.

[All 20 trials](../benchmarks/collection-array-prepend-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-prepend-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-prepend-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_REMOVE complete-job evidence

Query: `ARRAY_REMOVE(arr,CAST(idx AS BIGINT))`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.752655 | 0.735844-0.760773 |
| baseline streamfusion | 1.215214 | 1.197503-1.223665 |
| candidate stock | 0.740103 | 0.719264-0.795632 |
| candidate streamfusion | 0.723312 | 0.711303-0.769443 |

Elapsed time changes by -40.48% versus the prior evaluator and -2.27% versus paired stock Flink. Ranges remain visible; small overlapping differences do not establish a sustained advantage.

[All 20 trials](../benchmarks/collection-array-remove-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-remove-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-remove-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_CONCAT complete-job evidence

Query: `ARRAY_CONCAT(arr,arr)`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.570231 | 0.567433-0.580449 |
| baseline streamfusion | 0.838364 | 0.834675-0.845219 |
| candidate stock | 0.558484 | 0.546826-0.568327 |
| candidate streamfusion | 0.469299 | 0.467287-0.473333 |

Elapsed time changes by -44.02% versus the prior evaluator and -15.97% versus paired stock Flink. Ranges remain visible; small overlapping differences do not establish a sustained advantage.

[All 20 trials](../benchmarks/collection-array-concat-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-concat-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-concat-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_UNION complete-job evidence

Query: `ARRAY_UNION(arr,arr)`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 8.542574 | 8.498861-8.561808 |
| baseline streamfusion | 9.010223 | 8.957234-9.215945 |
| candidate stock | 8.587815 | 8.560695-8.662645 |
| candidate streamfusion | 1.440899 | 1.397706-1.456708 |

Elapsed time changes by -84.01% versus the prior evaluator and -83.22% versus paired stock Flink. Ranges remain visible; small overlapping differences do not establish a sustained advantage.

[All 20 trials](../benchmarks/collection-array-union-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-union-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-union-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_EXCEPT complete-job evidence

Query: `ARRAY_EXCEPT(arr,arr)`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 2.194777 | 2.157157-2.219175 |
| baseline streamfusion | 2.488554 | 2.433773-2.537326 |
| candidate stock | 2.274940 | 2.226252-2.339422 |
| candidate streamfusion | 0.955151 | 0.933976-0.968978 |

Elapsed time changes by -61.62% versus the prior evaluator and -58.01% versus paired stock Flink. Ranges remain visible; small overlapping differences do not establish a sustained advantage.

[All 20 trials](../benchmarks/collection-array-except-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-except-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-except-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_INTERSECT complete-job evidence

Query: `ARRAY_INTERSECT(arr,arr)`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 3.324832 | 3.305647-3.434229 |
| baseline streamfusion | 3.793790 | 3.734219-3.998472 |
| candidate stock | 3.285400 | 3.278935-3.376071 |
| candidate streamfusion | 1.148712 | 1.141860-1.177721 |

Elapsed time changes by -69.72% versus the prior evaluator and -65.04% versus paired stock Flink. Ranges remain visible; small overlapping differences do not establish a sustained advantage.

[All 20 trials](../benchmarks/collection-array-intersect-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-intersect-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-intersect-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.

## ARRAY_SORT complete-job evidence

Query: `ARRAY_SORT(ARRAY_SLICE(arr,1,31))`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 1.128538 | 1.087371-1.134692 |
| baseline streamfusion | 1.450659 | 1.424786-1.502337 |
| candidate stock | 1.174192 | 1.113978-1.278579 |
| candidate streamfusion | 0.599582 | 0.590643-0.657086 |

Elapsed time changes by -58.67% versus the prior evaluator and -48.94% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-array-sort-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-sort-2026-10-20.json) and [480 allocation probes](../benchmarks/collection-array-sort-allocations-2026-10-20.csv) retain the evidence. Eleven released SQL tests and 480 independent compiled-Calc Criterion fixtures pass; unrestricted duplicate arrays retain the released comparator exception.

## ELEMENT complete-job evidence

Query: `ELEMENT(ARRAY_SLICE(arr,1,1))`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.322372 | 0.317993-0.327850 |
| baseline streamfusion | 0.491326 | 0.479114-0.510494 |
| candidate stock | 0.333849 | 0.332103-0.443378 |
| candidate streamfusion | 0.359720 | 0.351583-0.379171 |

Elapsed time changes by -26.79% versus the prior evaluator and +7.75% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-element-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-element-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-element-allocations-2026-10-20.csv) retain the evidence. Eight released SQL tests cover empty/singleton/NULL arrays, typed failure, all-filtered batches and competing row errors.

## ARRAY complete-job evidence

Query: `ARRAY[idx,idx+1]`, 2,000,000 rows, INT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.459126 | 0.450285-0.505861 |
| baseline streamfusion | 0.624988 | 0.617185-0.650706 |
| candidate stock | 0.497458 | 0.476757-0.525365 |
| candidate streamfusion | 0.588684 | 0.574635-0.593293 |

Elapsed time changes by -5.81% versus the prior evaluator and +18.34% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-array-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-2026-10-20.json) and [18 allocation probes](../benchmarks/collection-array-allocations-2026-10-20.csv) retain the evidence. Thirteen released SQL tests cover exact DECIMAL, typed NULLs and pre-epoch temporal values at precision 3 and 9.

## MAP_FROM_ARRAYS complete-job evidence

Query: `MAP_FROM_ARRAYS(arr,arr)`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.683158 | 0.674097-0.701060 |
| baseline streamfusion | 0.892107 | 0.882873-0.896468 |
| candidate stock | 0.622768 | 0.615380-0.648856 |
| candidate streamfusion | 0.563137 | 0.547667-0.563784 |

Elapsed time changes by -36.88% versus the prior evaluator and -9.58% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-map-from-arrays-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-from-arrays-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-from-arrays-allocations-2026-10-20.csv) retain the evidence. Seven released SQL tests cover NULL/duplicate keys, entry order, length errors and all-filtered batches; native tests verify sharing and sliced offsets.

## MAP_KEYS complete-job evidence

Query: `MAP_KEYS(MAP_FROM_ARRAYS(arr,arr))`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.373882 | 0.370583-0.389018 |
| baseline streamfusion | 0.677727 | 0.652690-0.703831 |
| candidate stock | 0.396228 | 0.389792-0.419411 |
| candidate streamfusion | 0.425222 | 0.422905-0.434925 |

Elapsed time changes by -37.26% versus the prior evaluator and +7.32% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-map-keys-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-keys-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-keys-allocations-2026-10-20.csv) retain the evidence. Six released SQL tests cover direct and composed Boolean, integer-width and exact DECIMAL maps; NULL keys, entry order and filtered batches are preserved.

## MAP_VALUES complete-job evidence

Query: `MAP_VALUES(MAP_FROM_ARRAYS(arr,arr))`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.402274 | 0.395600-0.407491 |
| baseline streamfusion | 0.671592 | 0.664695-0.689231 |
| candidate stock | 0.367639 | 0.355607-0.380343 |
| candidate streamfusion | 0.410347 | 0.407060-0.416887 |

Elapsed time changes by -38.90% versus the prior evaluator and +11.62% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-map-values-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-values-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-values-allocations-2026-10-20.csv) retain the evidence. Six released SQL tests cover direct and composed basic maps with independent NULL values and filtered batches.

## MAP_ENTRIES complete-job evidence

Query: `MAP_ENTRIES(MAP_FROM_ARRAYS(arr,arr))`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 1.053904 | 1.034219-1.084736 |
| baseline streamfusion | 2.711965 | 2.619169-2.780334 |
| candidate stock | 0.979474 | 0.972003-0.990304 |
| candidate streamfusion | 0.946615 | 0.937376-0.959690 |

Elapsed time changes by -65.09% versus the prior evaluator and -3.35% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-map-entries-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-entries-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-entries-allocations-2026-10-20.csv) retain the evidence. Six released SQL tests cover direct and composed basic maps, nullable fields and entry order.

## MAP_UNION complete-job evidence

Query: `MAP_UNION(MAP_FROM_ARRAYS(arr,arr),MAP_FROM_ARRAYS(arr,arr))`, 2,000,000 rows, BIGINT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 1.114927 | 1.089909-1.146467 |
| baseline streamfusion | 1.189789 | 1.184148-1.200119 |
| candidate stock | 1.115454 | 1.107556-1.178076 |
| candidate streamfusion | 0.818729 | 0.814623-0.826174 |

Elapsed time changes by -31.19% versus the prior evaluator and -26.60% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-map-union-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-union-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-union-allocations-2026-10-20.csv) retain the evidence. Six released SQL tests each cover three variadic/order forms and explicit MAP_KEYS/MAP_VALUES consumers; native fixtures retain duplicates, NULL keys and empty-right behavior.

## MAP complete-job evidence

Query: `MAP[idx,idx+1]`, 2,000,000 rows, INT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.469222 | 0.466385-0.473432 |
| baseline streamfusion | 0.699248 | 0.682948-0.711830 |
| candidate stock | 0.477376 | 0.468256-0.485551 |
| candidate streamfusion | 0.593470 | 0.582599-0.596819 |

Elapsed time changes by -15.13% versus the prior evaluator and +24.32% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-map-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-map-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-map-allocations-2026-10-20.csv) retain the evidence. Nine released SQL tests cover basic type widths and repeated-key expression fallback, including discarded fallible values.

Flink 2.2 uses ordered grouping of generated key expressions. Its verified native
constructors can therefore feed ordinary Arrow UNNEST while preserving entry order.
Flink 1.18 groups through an unordered map; multi-entry constructors retain the
released generator instead of imposing the native constructor's order on later
consumers. Single-entry constructors retain native execution. These admission
limits leave the single-entry Flink 2.2 benchmark above unchanged; no separate
UNNEST speedup is claimed by this compatibility correction.

## ROW complete-job evidence

Query: `ROW(idx,idx+1)`, 2,000,000 rows, INT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.460757 | 0.456146-0.469860 |
| baseline streamfusion | 0.767587 | 0.760155-0.778142 |
| candidate stock | 0.478091 | 0.467846-0.482988 |
| candidate streamfusion | 0.584552 | 0.577802-0.608428 |

Elapsed time changes by -23.85% versus the prior evaluator and +22.27% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-row-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-row-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-row-allocations-2026-10-20.csv) retain the evidence. Six released SQL tests cover basic types, NULL fields, field access and filtered batches.

## OBJECT_OF complete-job evidence

Query: `OBJECT_OF('tech.streamfusion.FlinkNativeObjectSqlHarnessTest$NumericItem','a',idx,'b',idx)`, 2,000,000 rows, INT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.533071 | 0.487540-0.556143 |
| baseline streamfusion | 0.646736 | 0.636877-0.653464 |
| candidate stock | 0.522066 | 0.511138-0.550588 |
| candidate streamfusion | 0.380340 | 0.374885-0.385542 |

Elapsed time changes by -41.19% versus the prior evaluator and -27.15% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-object-of-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-object-of-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-object-of-allocations-2026-10-20.csv) retain the evidence. Released SQL tests cover basic structured values and field access. Class/field validation remains with Flink.

## OBJECT_UPDATE complete-job evidence

Query: `OBJECT_UPDATE(OBJECT_OF('tech.streamfusion.FlinkNativeObjectSqlHarnessTest$NumericItem','a',idx,'b',idx),'a',idx)`, 2,000,000 rows, INT, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.524227 | 0.511699-0.562427 |
| baseline streamfusion | 0.656629 | 0.646492-0.679796 |
| candidate stock | 0.461467 | 0.454539-0.465250 |
| candidate streamfusion | 0.379016 | 0.376367-0.381759 |

Elapsed time changes by -42.28% versus the prior evaluator and -17.87% versus paired stock Flink. Ranges remain visible; these results do not establish a universal advantage.

[All 20 trials](../benchmarks/collection-object-update-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-object-update-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-object-update-allocations-2026-10-20.csv) retain the evidence. Released SQL tests cover inline atomic updates and alias-sensitive projection orders; shared or non-atomic inputs remain generated.

## ARRAY_JOIN complete-job evidence

Query: `ARRAY_JOIN(arr,'|','NULL')`, 1,000,000 rows, STRING, width 8. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 1.690393 | 1.657495-1.698081 |
| baseline streamfusion | 2.805160 | 2.785818-3.040277 |
| candidate stock | 1.733543 | 1.726776-1.795149 |
| candidate streamfusion | 1.883071 | 1.868195-1.892549 |

Elapsed time changes by -32.87% versus the prior evaluator and +8.63% versus paired stock Flink. The remaining stock regression is retained under #392 and the accepted optional extension; default admission is unchanged.

[All 20 trials](../benchmarks/collection-array-join-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-join-2026-10-20.json) and [9 allocation probes](../benchmarks/collection-array-join-allocations-2026-10-20.csv) retain the evidence. Four released SQL tests cover runtime separators/replacements, isolated UTF-16 source surrogates, NULLs, filters and sensitive co-projections.

Released 1.18 probes also expose Janino's int-to-byte/short assignment failure in
ELEMENT. Those two result types stay on stock fallback in 1.18; BOOLEAN, INT, BIGINT and
DECIMAL use the native variant. Flink 2.2 admits the verified narrow types.

[Nine release allocation fixtures](../benchmarks/collection-element-legacy-allocations-2026-10-20.csv) also cover the 1.18 ELEMENT variant.

## Unary map union

A one-argument MAP_UNION preserves all entries. Its Arrow map input must still be
normalized to the return carrier declared by the kernel; reusing the original
MapArray object would return a different type from a declared List result. The
normalization shares both entry children, offsets and validity and copies no
entry values. Six released SQL checks cover ordinary nullable maps across basic
types, and sliced duplicate-entry native fixtures assert schema and buffer sharing.
The registry includes [six nullable/sliced unary allocation profiles](../benchmarks/collection-map-union-unary-allocations-2026-10-20.csv).
Each requests 17 allocations totaling 1,295 bytes for invocation metadata while
sharing all output buffers; these requests do not measure copied entry bytes.
Existing whole-job MAP_UNION measurements describe the two-argument query, not
this unary form.

The [final validation inventory](../benchmarks/collection-validation-2026-10-20.json)
records the expanded suite and focused rechecks: 429 successful JDK 17/Flink 2.2
checks, 330 successful JDK 11/Flink 1.18 checks with 99 released-capability skips,
690 release Rust core tests, and 630 array-edit, 480 sort and 240 collection-registry
Criterion smoke fixtures. The full JVM, external connector integration and
stateful recovery suites were not run locally.
