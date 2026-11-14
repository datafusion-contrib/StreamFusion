# TPC-DS with Paimon input and output

The opt-in TPC-DS harness imports all 103 SQL variants for the 99 templates from
Apache Flink release 2.2.1. It runs the SQL unchanged against 24 Paimon input tables
and commits each query's result to a typed Paimon output table. The source manifest
records the immutable upstream commit and SHA-256 of every query and schema.

The released `io.trino.tpcds:tpcds:1.4` generator prepares the inputs through Paimon's
Java SDK outside timing. Its `p_response_targe` column spelling is matched by ordinal
to Flink's `p_response_target`; all 425 column names and types are checked. SF1 is the
comparison scale. Smaller data is useful for auditing but can produce division by
zero in otherwise valid upstream queries; the harness does not rewrite those queries.

Both engines receive identical data, schemas, parallelism, compression and memory
settings. Like Flink's own TPC-DS runner, both enable join reordering and a 10 MiB
broadcast threshold. Catalog statistics use observed row/null counts, integral/date
bounds and string lengths, plus released Flink NDV estimates capped by non-NULL rows.
Decimal bounds are omitted to avoid rounding a decimal interval into an unsafe double
bound. Prepared fixtures are owned, checksummed and reused only when their manifest matches.

The `tpcds` profile uses production JVM assertion settings. In released Flink, an
`IN` predicate over a string column can estimate selectivity above one when the list
has more values than the column's NDV. The planner's Java assertion rejects that
estimate in Q73 under `-ea`; a normal production JVM proceeds. This profile affects
the benchmark module and retains every JUnit schema, result and native-work assertion.
The unchanged upstream qualification suites run with their existing settings.

## Run the suite

Use a release native build and an owned scratch directory:

```sh
SF_TPCDS=true mvn test -Ppaimon,bench,tpcds -pl streamfusion-paimon -am \
  -Dtest=PaimonTpcdsBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.tpcds.directory=/path/to/owned/tpcds-run \
  -Dsf.tpcds.scale=1 -Dsf.tpcds.mode=compare
```

Modes are `prepare`, `plan`, `audit`, `verify` and `compare`. `audit` records fallbacks;
`verify` and `compare` require native source and compute work in addition to exact
results. `compare` defaults to three warmups and seven measured trials per engine,
alternating engine order. `sf.tpcds.queries` accepts a comma-separated diagnostic
selection; only an empty selection denotes the complete suite. `sf.tpcds.parallelism`
defaults to two and `sf.tpcds.nativeBytes` to 536870912. The legacy adapter shares the
harness through `-Ppaimon1,bench,tpcds` with its matching released Flink line.

For queries that previously fell back for computation, `sf.tpcds.compute=stock`
keeps native Paimon input/output and uses stock computation. Its report labels the
engine `native_io_stock_compute` and asserts that no native compute ran. This is
a controlled ablation on the current engine, including its corrected shared-buffer
ownership, rather than a rebuilt historical release. It includes the row/Arrow
conversions needed between stock computation and native IO. Fully native runs
require zero entry transposes and zero source fallback rows.

`sf.tpcds.shuffleCompression=false` is a matched diagnostic that disables Flink's
blocking-shuffle compression for both engines. It defaults to true, as in the
primary cohort. Changed-configuration diagnostics remain separate from primary results.

Timing starts before SQL submission and ends after the final Paimon commit. Input
preparation, output DDL and stock readback are outside timing. The verifier compares
exact typed result multisets, preserving duplicates, NULLs, decimal scales and floating
bit patterns. It checks committed row counts, native Parquet footers, source counters,
compute counters and stock's absence of native work. Each report records physical
input/output files, splits, partitions and buckets, along with plans and callback lists.
Inputs and outputs are unpartitioned tables; query parallelism and hash exchanges are
recorded separately from table partitioning. Diagnostic `sf.tpcds.retainGauges=true`
retains deployed/planned share counts; regular repeated comparisons retain counters.

The report is rewritten after each query. Output tables are removed after validation;
remove the owned run directory after retaining concise benchmark evidence. This is
an engine comparison using TPC-DS-derived queries, not an audited TPC benchmark result.

## Qualification

CI plans every unchanged variant against generated SF0.01 Paimon fixtures on both
supported Flink lines and the legacy Paimon adapter. These checks require native
source and compute planning; they do not execute the full suite. SQL operator tests
cover runtime edge cases separately. SF1 verification and repeated comparisons
add exact committed results, actual compute counters and output encoder checks.

## SF1 comparison, 2026-11-14

All 103 variants completed all 2,060 trials, including warmups, with exact committed
result types/multisets and positive native source, compute and output evidence.
Every native trial had zero source fallback and zero entry-transpose rows. Nonempty
native outputs had parquet-rs footers; stock runs had no native work or encoders.

The recorded source is `bdfa739c59699682cbd561c4140183afeb5464ee`, based on main's
compatibility-based batch admission in `918716ab8`. The frozen runtime records Java
artifact and native-library hashes. Flink 2.2.1/Paimon 2.1.0 ran on an Apple M4 with
16 GiB RAM, JDK 17.0.17, an 8 GiB heap, parallelism two, 512 MiB native memory,
1 GiB Flink managed memory, 8,192-row batches and production Arrow IPC exchanges.

Native medians were faster for **85 variants** and slower for **18**. The geometric
mean stock/native ratio was **1.6697×**. Summing the per-query medians gives
**195.919 s stock versus 100.825 s StreamFusion**, a **1.9432×** ratio. These sums
exclude fixture preparation, output DDL and stock readback, as do the individual
timers; they are not the wall-clock duration of the benchmark harness.

This is a warm-cache comparison on a shared local host. A recorded host observation
showed active swap and other JVM activity. Trial ranges retain that variability;
these measurements do not isolate the effect of the admission-policy change or
identify the cause of individual regressions. Profiling remains separate from this
unprofiled primary cohort.

Median milliseconds (minimum–maximum), including submission through final commit.
A stock/native ratio below one indicates a slower native median.

| Query variant | Stock Flink | StreamFusion | Stock / native |
| --- | ---: | ---: | ---: |
| 1 | 472.2 (441.2–560.5) | 340.2 (301.7–426.9) | 1.39× |
| 2 | 1,515.9 (1,379.9–3,629.1) | 525.1 (489.6–1,106.0) | 2.89× |
| 3 | 612.6 (570.9–808.7) | 630.3 (577.8–813.5) | 0.97× |
| 4 | 4,536.1 (3,937.5–5,307.8) | 1,698.9 (1,367.0–1,765.3) | 2.67× |
| 5 | 1,851.2 (1,360.5–2,145.0) | 1,165.4 (1,063.9–1,509.1) | 1.59× |
| 6 | 859.5 (603.5–1,017.0) | 470.0 (432.0–608.8) | 1.83× |
| 7 | 1,393.8 (924.7–2,066.2) | 1,252.9 (1,204.8–1,564.7) | 1.11× |
| 8 | 3,062.7 (2,599.2–4,004.5) | 1,053.8 (904.8–1,276.7) | 2.91× |
| 9 | 3,765.0 (2,561.8–5,113.6) | 2,666.9 (2,351.2–3,461.2) | 1.41× |
| 10 | 721.5 (580.6–893.4) | 796.4 (707.2–868.3) | 0.91× |
| 11 | 3,075.6 (2,533.0–3,465.3) | 1,129.2 (1,007.0–1,591.7) | 2.72× |
| 12 | 458.1 (395.7–704.4) | 335.3 (289.8–426.5) | 1.37× |
| 13 | 1,605.3 (1,090.3–2,205.7) | 455.5 (364.3–526.2) | 3.52× |
| 14a | 3,561.3 (3,062.6–3,797.8) | 2,423.6 (1,808.7–2,965.9) | 1.47× |
| 14b | 5,222.9 (4,333.1–6,048.1) | 4,148.9 (3,679.0–6,050.8) | 1.26× |
| 15 | 463.8 (300.7–793.5) | 432.8 (349.4–481.9) | 1.07× |
| 16 | 1,891.2 (1,736.3–2,422.0) | 674.6 (621.4–779.1) | 2.80× |
| 17 | 768.6 (739.5–938.7) | 793.2 (699.0–947.7) | 0.97× |
| 18 | 901.7 (783.8–1,178.3) | 1,220.6 (1,043.1–1,823.0) | 0.74× |
| 19 | 502.3 (470.7–579.7) | 800.1 (649.7–899.1) | 0.63× |
| 20 | 446.8 (271.1–488.3) | 436.2 (386.6–494.3) | 1.02× |
| 21 | 737.2 (653.1–1,745.8) | 2,243.9 (1,682.8–3,762.4) | 0.33× |
| 22 | 4,123.3 (3,495.8–6,722.6) | 3,185.1 (2,857.8–3,605.8) | 1.29× |
| 23a | 7,958.8 (5,776.0–9,514.4) | 2,772.7 (2,005.8–3,591.9) | 2.87× |
| 23b | 6,634.7 (4,542.3–8,900.1) | 2,497.6 (1,710.7–3,326.6) | 2.66× |
| 24a | 732.6 (646.7–855.4) | 980.1 (634.8–1,065.7) | 0.75× |
| 24b | 754.0 (637.8–890.4) | 1,050.2 (906.9–1,227.3) | 0.72× |
| 25 | 1,383.9 (965.7–1,842.6) | 1,301.4 (978.4–1,504.4) | 1.06× |
| 26 | 725.3 (621.9–899.7) | 834.7 (720.5–1,681.4) | 0.87× |
| 27 | 927.5 (838.7–1,085.7) | 1,580.8 (1,506.9–2,063.5) | 0.59× |
| 28 | 1,242.1 (1,031.0–1,345.8) | 432.5 (404.1–607.0) | 2.87× |
| 29 | 857.2 (756.8–1,319.5) | 880.5 (634.6–948.0) | 0.97× |
| 30 | 425.1 (383.8–542.7) | 300.6 (281.2–593.9) | 1.41× |
| 31 | 1,998.4 (1,873.9–2,297.9) | 1,040.5 (950.6–1,112.8) | 1.92× |
| 32 | 469.7 (367.1–618.5) | 507.8 (446.6–667.7) | 0.93× |
| 33 | 455.3 (440.0–697.1) | 904.4 (777.3–1,086.7) | 0.50× |
| 34 | 481.9 (399.2–570.7) | 313.0 (307.5–380.5) | 1.54× |
| 35 | 600.7 (577.0–673.9) | 317.3 (313.4–331.5) | 1.89× |
| 36 | 1,006.3 (884.5–1,830.2) | 654.1 (599.8–687.7) | 1.54× |
| 37 | 804.9 (701.2–888.1) | 292.1 (284.0–306.0) | 2.76× |
| 38 | 717.8 (635.4–950.3) | 307.1 (301.4–334.5) | 2.34× |
| 39a | 3,300.2 (3,235.5–3,423.3) | 1,144.0 (1,126.7–1,197.3) | 2.88× |
| 39b | 3,228.0 (3,193.1–3,363.2) | 1,204.2 (1,195.4–1,293.6) | 2.68× |
| 40 | 341.9 (300.1–522.7) | 251.5 (242.1–265.9) | 1.36× |
| 41 | 188.1 (174.4–272.4) | 116.8 (110.7–130.3) | 1.61× |
| 42 | 362.4 (337.7–544.5) | 324.4 (308.8–342.2) | 1.12× |
| 43 | 865.7 (794.9–1,034.0) | 383.3 (349.9–590.1) | 2.26× |
| 44 | 560.6 (488.7–736.6) | 229.4 (219.3–260.3) | 2.44× |
| 45 | 403.0 (325.6–535.5) | 228.3 (210.9–345.5) | 1.77× |
| 46 | 774.9 (678.5–884.5) | 654.4 (631.5–987.6) | 1.18× |
| 47 | 1,662.3 (1,591.1–1,908.1) | 1,144.4 (1,000.3–1,244.5) | 1.45× |
| 48 | 1,012.5 (944.5–1,122.4) | 330.8 (294.6–352.7) | 3.06× |
| 49 | 890.4 (852.7–985.2) | 348.6 (284.5–358.8) | 2.55× |
| 50 | 667.9 (605.2–782.8) | 325.4 (314.8–345.9) | 2.05× |
| 51 | 1,256.0 (1,205.5–1,358.8) | 778.9 (755.6–910.5) | 1.61× |
| 52 | 416.7 (378.7–487.3) | 313.4 (296.5–350.9) | 1.33× |
| 53 | 18,812.1 (18,242.0–19,892.1) | 1,492.4 (1,435.1–1,705.5) | 12.61× |
| 54 | 616.9 (567.8–700.5) | 362.8 (345.5–411.1) | 1.70× |
| 55 | 424.9 (370.0–663.8) | 321.4 (311.2–461.6) | 1.32× |
| 56 | 653.7 (491.3–803.7) | 543.7 (468.5–686.5) | 1.20× |
| 57 | 1,045.2 (911.3–1,433.4) | 992.8 (754.4–1,056.2) | 1.05× |
| 58 | 1,122.2 (954.0–1,405.4) | 523.0 (390.5–792.6) | 2.15× |
| 59 | 2,563.2 (2,305.8–3,775.6) | 647.2 (584.4–717.1) | 3.96× |
| 60 | 787.8 (518.4–974.8) | 580.5 (456.6–1,069.2) | 1.36× |
| 61 | 1,971.8 (1,594.8–2,316.2) | 665.5 (499.5–890.2) | 2.96× |
| 62 | 514.5 (406.4–549.6) | 349.4 (272.0–432.5) | 1.47× |
| 63 | 20,748.3 (18,747.1–39,418.0) | 1,568.7 (1,414.1–1,925.0) | 13.23× |
| 64 | 3,189.5 (2,085.7–3,744.3) | 2,400.1 (2,115.7–2,772.2) | 1.33× |
| 65 | 811.5 (722.0–1,330.3) | 559.6 (483.5–696.4) | 1.45× |
| 66 | 1,614.5 (1,212.0–2,137.6) | 547.5 (495.9–871.3) | 2.95× |
| 67 | 7,674.1 (5,850.1–10,471.0) | 2,301.0 (2,143.6–2,761.9) | 3.34× |
| 68 | 945.4 (860.9–1,156.8) | 1,039.0 (809.6–1,263.1) | 0.91× |
| 69 | 774.4 (568.9–920.3) | 600.4 (443.4–712.6) | 1.29× |
| 70 | 1,521.3 (1,192.0–1,783.5) | 574.1 (468.6–712.1) | 2.65× |
| 71 | 724.3 (569.4–933.2) | 535.3 (432.5–765.4) | 1.35× |
| 72 | 4,771.5 (4,643.5–5,462.4) | 1,728.2 (1,398.6–1,869.6) | 2.76× |
| 73 | 500.7 (376.4–941.3) | 421.4 (346.3–540.2) | 1.19× |
| 74 | 2,632.0 (2,040.0–3,603.0) | 738.9 (596.5–1,193.9) | 3.56× |
| 75 | 2,236.9 (1,728.8–2,392.8) | 1,411.1 (975.0–1,856.6) | 1.59× |
| 76 | 843.9 (556.9–1,034.9) | 378.4 (282.9–563.0) | 2.23× |
| 77 | 1,224.1 (1,104.4–1,746.9) | 720.1 (489.6–798.5) | 1.70× |
| 78 | 2,236.1 (2,072.2–2,402.7) | 1,690.4 (1,493.4–2,061.8) | 1.32× |
| 79 | 951.6 (675.0–1,242.9) | 1,127.2 (884.3–1,225.2) | 0.84× |
| 80 | 2,142.5 (1,181.3–2,524.8) | 1,223.8 (1,103.1–1,452.6) | 1.75× |
| 81 | 496.6 (491.3–757.5) | 285.1 (199.8–298.3) | 1.74× |
| 82 | 1,112.1 (917.5–1,245.1) | 431.6 (365.5–592.3) | 2.58× |
| 83 | 596.7 (467.8–852.5) | 283.8 (233.3–509.4) | 2.10× |
| 84 | 446.8 (367.5–561.6) | 215.7 (181.2–282.2) | 2.07× |
| 85 | 4,636.3 (3,299.3–8,522.8) | 636.0 (562.9–700.0) | 7.29× |
| 86 | 845.1 (585.5–954.8) | 408.7 (339.0–488.2) | 2.07× |
| 87 | 998.8 (880.4–1,617.9) | 454.1 (356.2–501.6) | 2.20× |
| 88 | 4,988.6 (4,576.2–7,926.4) | 10,242.7 (7,213.5–14,717.4) | 0.49× |
| 89 | 537.3 (519.3–928.3) | 460.8 (448.0–499.3) | 1.17× |
| 90 | 591.1 (473.9–1,063.8) | 1,197.1 (1,016.9–1,884.4) | 0.49× |
| 91 | 868.2 (570.7–2,080.1) | 258.6 (184.4–364.1) | 3.36× |
| 92 | 582.4 (475.7–868.3) | 391.8 (257.1–502.8) | 1.49× |
| 93 | 1,498.9 (1,282.1–2,220.2) | 747.2 (605.7–828.4) | 2.01× |
| 94 | 1,134.3 (938.7–1,428.9) | 351.7 (304.6–621.6) | 3.23× |
| 95 | 2,025.4 (1,886.9–2,452.8) | 449.4 (360.9–991.9) | 4.51× |
| 96 | 1,339.6 (1,209.4–1,669.4) | 2,982.5 (2,784.9–3,371.8) | 0.45× |
| 97 | 931.7 (723.5–1,231.1) | 493.0 (355.2–528.1) | 1.89× |
| 98 | 695.1 (572.8–776.2) | 388.9 (364.5–559.0) | 1.79× |
| 99 | 883.5 (728.2–924.5) | 477.9 (427.6–574.2) | 1.85× |

[Every warmup/measured trial, ranges, typed-result hashes and native-work witnesses](results/tpcds-sf1-20261114.json)
are retained with the input/schema/query and runtime identities.

The [pre-rebase partial cohort](results/tpcds-sf1-before-all-or-nothing-20261114.json)
was stopped at 65 completed variants when main adopted the new batch policy. Its
partial Q14b stock run failed a released Flink initialization-metrics timestamp
invariant. That cohort, including its losses and partial trials, is diagnostic and
is excluded from the complete comparison above.

## Previous fallback control

Nine queries that previously needed stock computation were repeated with native Paimon
IO and stock computation: 5, 6, 8, 14b, 22, 24a, 36, 44 and 53. All 180 trials passed
exact results, native IO/sink evidence and the absence of native compute. Input,
runtime hashes, resources, three warmups and seven measured repetitions match the
primary configuration. The source ownership correction is held constant.

This control loses to its paired stock engine in eight of nine cases (geometric
stock/control ratio 0.7625×). It includes the row/Arrow boundaries around stock
computation; it does not isolate decoder or encoder kernel speed. The comparison
below also shows native-compute medians from the separate complete cohort. Their
direct ratio is descriptive across cohorts, not a simultaneous three-engine trial
or an estimate that removes shared-host variation.

Median milliseconds; ranges and every observation remain in the structured report.

| Query | Stock in control cohort | Native IO / stock compute | Native compute in full cohort | Previous / native compute |
| --- | ---: | ---: | ---: | ---: |
| 5 | 1,120.1 | 1,450.5 | 1,165.4 | 1.24× |
| 6 | 481.2 | 692.6 | 470.0 | 1.47× |
| 8 | 2,290.5 | 2,012.5 | 1,053.8 | 1.91× |
| 14b | 3,277.9 | 5,837.0 | 4,148.9 | 1.41× |
| 22 | 3,480.8 | 5,030.2 | 3,185.1 | 1.58× |
| 24a | 672.6 | 1,076.8 | 980.1 | 1.10× |
| 36 | 973.7 | 1,551.8 | 654.1 | 2.37× |
| 44 | 554.3 | 571.7 | 229.4 | 2.49× |
| 53 | 19,084.2 | 19,748.8 | 1,492.4 | 13.23× |

[The complete previous-fallback control](results/tpcds-sf1-previous-control-20261114.json)
retains its paired stock timings, variability, exact-result hashes and runtime proof.

## Regression diagnostics

Q21, Q33 and Q88 were repeated under JFR after the unprofiled comparisons finished.
All 60 trials retained exact committed results and native execution checks. The
large primary Q21/Q33 slowdowns did not reproduce; Q88 remained slightly slower.
These diagnostic medians do not replace any primary observation.

| Query | Profiled stock, ms | Profiled StreamFusion, ms |
| --- | ---: | ---: |
| 21 | 888.3 | 854.8 |
| 33 | 648.4 | 561.0 |
| 88 | 5,628.6 | 6,055.7 |

The combined profile contains substantial Java shuffle-decompression work:
`LZ4JavaSafeFastDecompressor` occurs inclusively in 5,911 of 6,712 Java execution
samples on native-named workers (88.1%), and 4,278 of 6,399 on stock-named workers
(66.9%). Thread names classify workers; these are Java-stack sample fractions,
not percentages of whole-job CPU. Native-method samples are recorded separately
because they can include blocking. A separate OS sample resolves Rust Parquet push
decoding, dictionary/RLE decoding and Arrow take code, without establishing a complete
Rust CPU distribution. The profiles cover these three queries, not all 18 losses.

A follow-up unprofiled comparison disabled shuffle compression for both engines,
then repeated the default setting with the same frozen runtime, three warmups and
seven measured trials. Every trial passed the same result and execution checks.

| Query | Compression off: stock / native, ms | Compression on: stock / native, ms |
| --- | ---: | ---: |
| 21 | 843.2 / 863.7 | 995.9 / 878.0 |
| 33 | 628.2 / 460.8 | 603.4 / 497.5 |
| 88 | 4,998.8 / 8,600.3 | 5,177.0 / 5,951.3 |

Disabling compression did not eliminate Q88's loss. The cohort order was fixed
and the host shared, so these comparisons do not isolate compression's causal
effect or identify the cause of the primary regressions. Defaults and admission
remain unchanged. [Structured diagnostics](results/paimon-tpcds-diagnostics-20261114.json)
retain every trial, ranges, runtime hashes, sampling counts and interpretation limits.

## Archived evidence

Full plans, raw reports/logs, interrupted and unfavorable cohorts, source/runtime
provenance, JFR recordings and the native sample are retained in the
[versioned evidence release](https://github.com/jordepic/StreamFusion-contrib/releases/tag/paimon-batch-tpcds-evidence-2026-11-14-bdfa739c5).
The [SHA-256 manifest](results/paimon-tpcds-evidence-20261114.json) records the archive
and every member. Generated datasets and build artifacts are excluded. This archive
was downloaded independently and verified before local raw evidence was removed.

To restore verified evidence beneath an owned directory, from the repository root:

```sh
python3 - <<'PY'
from pathlib import Path
import sys
sys.path.insert(0, "dev")
from benchmark_artifacts import fetch
fetch(["docs/benchmarks/paimon-tpcds-20261114-evidence"],
      Path("/path/to/owned/evidence"), Path("/path/to/owned/cache"),
      Path("docs/benchmarks/results/paimon-tpcds-evidence-20261114.json"))
PY
```
