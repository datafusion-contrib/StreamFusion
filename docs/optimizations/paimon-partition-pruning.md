# Preserve Paimon partition pruning

**Applies to:** native streaming Paimon scans with retained string partition equalities

The native source previously rebuilt Paimon's read builder with projection but omitted pushed
filters. Flink's residual filter kept results correct, but irrelevant partitions could still be
planned and decoded. The planner now passes a narrow partition-only hint to the released
`ReadBuilder.withFilter` API, keeping the original residual filter in the plan.

Supported hints compare a `VARCHAR` partition column to a non-null string literal. Conjunctions
may contribute supported equalities; disjunctions, casts, other types, and non-partition columns
contribute no hint. Consumed stock source filters require stock fallback. Ordered source-ability
schemas map expression indices back to full-table ordinals, independently of projected output.
Physical plan copies and shared source nodes retain the hint. Paimon's own snapshot/live split
planning and existing checkpoint offsets keep their contracts.

## Measurement

`PaimonPartitionPruningBenchmark` consumes every planned split and measures released planning,
file decoding, and residual checksum work. On the validated 131,072-row, 32-partition,
256-byte-payload fixture, pruning reduced planned files **32→1**, planned file bytes
**169,821→5,308**, and decoded rows **131,072→4,096**. The isolated median was **82.96 ms**
without the hint versus **9.54 ms** with it; released stock pruning also measured **9.54 ms**.
Planned bytes are metadata totals, not measured I/O. This fixture demonstrates the available
pruning boundary, not an SQL job throughput result.

The [raw results JSON](../benchmarks/results/paimon-partition-pruning.json) archives all 18
full-drain observations, file witnesses, checksums, and inclusive quartile summaries.

| Payload bytes | Reader mode | Median ms | Range ms | IQR ms |
| ---: | --- | ---: | --- | ---: |
| 64 | NATIVE_FULL | 55.868 | 48.410–60.347 | 5.968 |
| 64 | NATIVE_PRUNED | 9.681 | 8.352–11.176 | 1.412 |
| 64 | STOCK_PRUNED | 11.075 | 9.548–13.310 | 1.881 |
| 256 | NATIVE_FULL | 82.961 | 78.538–97.197 | 9.329 |
| 256 | NATIVE_PRUNED | 9.541 | 8.379–11.229 | 1.425 |
| 256 | STOCK_PRUNED | 9.542 | 8.418–13.627 | 2.604 |

Both payload widths decode 131,072 rows without pruning and 4,096 rows with it, with the same
4,096 selected rows and checksum 268,369,920. Native opens are 32 versus 1; the stock mode's
native counter is zero because it uses the released Java reader. For width 64 the planned bytes
are 138,973→4,344; width 256 uses the totals above.

`bin/collect-paimon-pruning-results.py` regenerates the archive from fixture logs. Supply
`--micro-log`, `--output`, and optional `--sql-before-log`/`--sql-after-log` once paired SQL runs
finish. It accepts both the original p00-only log format and the first/last-partition format,
retaining source log names, planning-order witnesses, every observation and per-shape summaries.
The direct pruned fixture and SQL candidate both call the released read-builder filter API and
production native split reader; only the SQL run proves the planner actually supplies the hint.

`PaimonPartitionSqlBenchmark` separately preserves startup, source decoding, Arrow-to-row output,
collection, and cancellation on one immutable table. It measures `p00`, `p31`, and all partitions,
logs the released planning order and native plan witness, and checks every collected row.
Selective streaming jobs stop after their expected result prefix; early cancellation can hide
unrelated decoding, and partition names do not guarantee scheduling order. The initial p00-only
262,144-row comparison showed approximately **245 ms before and 244 ms after**, providing
no demonstrated end-to-end gain. The final paired run retained all three query cases, one warmup
and five observations per stock/native shape, on identical 262,144-row tables. All 29 source SQL
cases and two predicate tests passed. Every candidate selective native plan reported
`partition_hint=true`; baseline native plans reported no hint.

| Query | Source | Before median ms | After median ms | Before range / IQR ms | After range / IQR ms |
| --- | --- | ---: | ---: | --- | --- |
| p00 | stock | 345.882 | 239.865 | 249.158–398.990 / 44.643 | 238.866–341.311 / 100.223 |
| p00 | native | 247.428 | 243.529 | 244.064–249.555 / 2.175 | 238.631–286.049 / 38.566 |
| p31 | stock | 345.145 | 281.067 | 241.844–351.758 / 59.090 | 239.449–344.653 / 99.931 |
| p31 | native | 243.779 | 239.002 | 242.189–244.810 / 0.991 | 238.415–242.751 / 0.611 |
| all | stock | 1127.174 | 898.806 | 880.195–6286.358 / 4984.580 | 886.313–1076.695 / 66.777 |
| all | native | 897.027 | 901.372 | 887.817–1178.576 / 88.455 | 877.795–1032.875 / 101.557 |

Native selective medians changed 247.428→243.529 ms for p00 (1.016×) and
243.779→239.002 ms for p31 (1.020×); these short live-prefix jobs establish no meaningful
SQL speedup. Unfiltered native changed 897.027→901.372 ms. Stock times drift substantially
between JVMs, and the 6,286.358 ms baseline unfiltered stock outlier remains in the archive and
range. The after p00 native median is slightly slower than stock, while p31 is faster; neither
should be generalized into sustained source throughput.

Both released full-scan plan witnesses list p00 at position 18 and p31 at position 29 of 32,
with p30 first and p25 last. Thus p00/p31 are name-end cases, not actual first/last scheduled
partitions. Identical observed planning orders do not guarantee runtime ordering or exclude
prefetch before cancellation. Only the full-drain microbenchmark above demonstrates physical
file/row reduction; its verified 32→1 native opens are not aggregate SQL metrics.
Only the unfiltered SQL case collects the complete initial snapshot. See the
[Paimon connector](../connectors/paimon.md) for fixture controls and supported source semantics.
