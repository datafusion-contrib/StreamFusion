# Deferred filtering in native Calc

**Status:** WONTDO (2026-09-27).

[#249](https://github.com/datafusion-contrib/StreamFusion/issues/249) investigated copies of
columns that survive projection pruning. A tested, rejected integration gathered top-level
SUBSTRING results directly for STRING columns with literal INT start/length bounds. It reused
the existing substring slicing logic, visited only retained rows and preserved output order
and NULLs. Shared payloads, other expressions, empty selections and all-true selections kept
the ordinary evaluator.

Only this infallible operation bypassed input materialization. Other projections retained their
evaluation order after filtering. Changelog tags used the ordinary filter; the Arrow batch kept
its key group, and gathered strings owned their buffers. No selection metadata or borrowed
string view crossed an operator boundary.

The `calc_selection` benchmark contains the initial local prototype for
`SUBSTRING(payload, 1, 32)` that visits selected bitmap indices and copies only the resulting
prefix. It does not introduce selection metadata into batches or change execution admission.

DataFusion 54.1's default `PhysicalExpr::evaluate_selection` is not a deferred-copy solution:
it filters the input batch, evaluates the expression, and scatters the result back to input
positions. Comet's projection builder uses DataFusion's ordinary `ProjectionExec`; neither
interface removes the need to handle selected string inputs locally here.

The diagnostic compares the production substring UDF after pruning/filtering, a control that
filters but bounds substring output allocation, and the selected-prefix prototype. The last
two use identical substring logic, including the ASCII check, so their difference isolates
filtering/copying and selection traversal. A naive gather that scanned every predicate bit
was slower than the current path for sparse long payloads; iterating Arrow's set-bit indices
removes that cost. All three results are compared for every workload.

Release+mimalloc diagnostic against main `022f735a`, on Intel Core i7-12650H, Linux/WSL,
2026-09-27: 4,096 rows,
20 samples, 200 ms warmup and one second measurement per case, with 1,000 bootstrap resamples.
The grid uses 32/256/4,096-byte ASCII payloads and comparable Unicode payloads, NULL payloads
every seventh row, and nominal 0/1/10/50/100% selections. Partial selections include NULL
predicates every thirteenth row; the 100% case is all true. Median microseconds:

| ASCII payload | Selection | Current pruned filter | Bounded-output filtered control | Selected-prefix prototype |
|---|---:|---:|---:|---:|
| 32 bytes | 1% | 1.618 | 1.595 | 0.611 |
| 32 bytes | 50% | 23.765 | 22.323 | 11.058 |
| 4,096 bytes | 0% | 0.296 | 0.416 | 0.293 |
| 4,096 bytes | 1% | 6.363 | 5.724 | 2.549 |
| 4,096 bytes | 10% | 75.762 | 72.558 | 23.724 |
| 4,096 bytes | 50% | 448.708 | 432.197 | 141.413 |
| 4,096 bytes | 100% | 433.588 | 392.244 | 339.065 |

For 4 KiB ASCII payloads at 1%, current filtering copies 131,072 payload bytes before producing
1,024 output bytes. The prototype copies only those 1,024 bytes. Requested allocation traffic
falls from 264,816 bytes in 28 allocation/reallocation calls to 1,816 bytes in nine calls;
the bounded-output filtered control still requests 134,210 bytes in 23 calls. These are
requested bytes, not peak live memory, and payload-copy counts exclude offsets and validity
buffers. Allocation counters surround one warmed invocation, outside the timing loop. UDF registration,
argument/result fields and configuration are cached before measurement.

The prototype is 2.50–3.19x faster for the measured partial long-ASCII selections and
2.91–4.80x for their Unicode counterparts. All-true results are mixed: the final ASCII run is faster, but
Unicode is about 6% slower. The rejected integration retained the existing all-true path.
These are isolated string-expression measurements, not whole-Calc or end-to-end speedups.
Before removal, the integration passed 25 native Calc tests and 119 SQL/operator/schema regression cases on each of
Flink 2.2.1 and 1.18.1 across focused runs. Coverage includes sliced Arrow arrays and masks,
NULL predicate bits, Unicode and extreme substring bounds, nested ordinary projections,
zero remaining input columns, bounded reservation for sliced backing buffers, empty/all-true
batches, CASE guards, retained failures,
changelog/key-group alignment, a downstream native aggregate, and retained outputs after
input release and operator shutdown.

The COALESCE probe exposed an existing failure-parity gap: released Flink can evaluate a
strict cast in a later operand even when an earlier operand is non-NULL. Such known fallible
COALESCE expressions now use the existing generated evaluator, like volatile/UDF operands.
Both released lines reproduce the host error. Prototype timings alone do not establish
whole-job gains; the integration release comparison is reported below.

Reproduce the diagnostic and the whole-job baseline harness with:

```sh
cargo bench --manifest-path native/Cargo.toml -p streamfusion \
  --bench calc_selection --features mimalloc
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=CalcSelectionBenchmark \
  -Dselection.rows=1000000 -Dselection.bytes=32,4096 \
  -Dselection.percent=0,1,10,50,100 -Dselection.warmup=2 -Dselection.runs=5
```

The whole-job harness adds runtime INT ids and DECIMAL(20,2) amounts, evaluates
`amount + CAST(1 AS DECIMAL(20,2))`, and compares full-payload and short-substring projections.
It asserts native Calc and both Arrow transposes, uses a rowwise source and blackhole sink,
and alternates stock Flink/native runs. The comparisons below retain those boundaries.

### Whole-job comparison

The first production comparison used one million rows per job, the default 8 GiB maximum
Java heap, two warmups and five measured runs, alternating Flink/native engines. Both versions
used the same runtime source, DECIMAL arithmetic, both transposes and rowwise sink. Tables
show median seconds; the identity projection is an unchanged full-payload control. Stock
Flink substring times are from the second run of the grid.

**32-byte payloads**

| Selection | Native identity before | After | Native substring before | After | Flink substring |
|---|---:|---:|---:|---:|---:|
| 0% | 0.372 | 0.373 | 0.364 | 0.351 | 0.219 |
| 1% | 0.369 | 0.370 | 0.369 | 0.378 | 0.223 |
| 10% | 0.383 | 0.388 | 0.389 | 0.385 | 0.235 |
| 50% | 0.455 | 0.460 | 0.459 | 0.465 | 0.307 |
| 100% | 0.543 | 0.549 | 0.553 | 0.559 | 0.387 |

**4096-byte payloads**

| Selection | Native identity before | After | Native substring before | After | Flink substring |
|---|---:|---:|---:|---:|---:|
| 0% | 4.744 | 4.843 | 4.729 | 4.751 | 3.666 |
| 1% | 4.770 | 4.886 | 4.759 | 4.792 | 3.690 |
| 10% | 4.873 | 4.898 | 4.818 | 4.776 | 3.839 |
| 50% | 5.371 | 5.474 | 4.987 | 5.048 | 4.457 |
| 100% | 5.846 | 5.898 | 5.014 | 5.034 | 5.316 |

This grid does not establish a reliable whole-job speedup. Small-payload results are broadly
flat relative to their controls. The long-payload runs contain large spikes: at 50% selection,
two native substring trials took about 6.4 seconds and the unchanged identity control also
had a roughly 7.0-second trial. The maximum heap exceeded this machine's 7.6 GiB RAM.
A focused repeat used identical 2 GiB heaps, four warmups and nine samples, one million rows,
4096-byte payloads and 50% selection. Median seconds (minimum–maximum):

| Projection | Before | Gather integration |
|---|---:|---:|
| Unchanged identity control | 6.792 (6.693–7.082) | 6.700 (6.638–6.825) |
| Substring | 6.389 (6.216–6.596) | 6.216 (6.132–6.475) |

Substring elapsed time fell 2.7%, while the unchanged control fell 1.4%. The sample ranges
overlap and the earlier full grid is flat. These measurements do not establish a repeatable
whole-job advantage sufficient to maintain a separate expression recognizer and evaluator.
The integration was removed; the diagnostic benchmark and SQL/operator regressions remain.
The COALESCE failure-parity fix is independent and remains. After removal, all 11 new
SQL/operator regressions passed again on each of Flink 2.2.1 and 1.18.1.

Reproduce the focused workload by adding `-Dselection.bytes=4096 -Dselection.percent=50
-Dselection.engine=native -Dselection.warmup=4 -Dselection.runs=9` and
`-Dsf.extraJvmArgs="-Xms2g -Xmx2g"` to the Maven command above. The current code is the
pruned baseline, not the rejected integration. The retained Criterion prototype reproduces
the allocation/copy comparison.

Reconsider only when an end-to-end profile shows surviving string materialization dominates
and repeated measurements against the pruned baseline show a gain beyond control variation.
A wider selection-carrying batch contract is not justified by this evidence.
