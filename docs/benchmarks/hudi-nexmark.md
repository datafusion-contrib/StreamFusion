# Four-partition Hudi Nexmark

`NexmarkHudiBenchmark` uses the existing Nexmark matrix queries and Kafka event
corpus with Hudi tables at both ends. This is a separate storage workload from
the README Kafka benchmark. It requires the released Hudi 1.2.1 Flink 1.18 bundle
and release StreamFusion engine/Parquet libraries.

For the Java batching/allocation investigation, the manual **CI startup profile**
workflow with suite `hudi-benchmark` runs Q8/Q12 on one isolated Ubuntu runner. Supply a full baseline commit
SHA with identical native sources, build inputs and benchmark harness. The runner
admits changes only to the allocation authority, Calc transport and Hudi terminal-batch proof,
builds the two release libraries once, verifies their hashes throughout, and
alternates baseline/candidate then candidate/baseline. Each invocation retains
one warmup and three rotating stock/previous/native trials, four source/sink tasks,
heap state and mini-batching off. It restores candidate sources and deletes runtime
tables even after failure. The workflow artifact retains every observation and log.
This focused comparison is separate from the complete four-configuration matrix.
The Java-only runner follows the module-owned core sources and rejects unlisted
core, Hudi or Parquet changes before replacing any source files.
Q10 is also available for a Java-only comparison with matching native inputs.

The same comparison can run on an otherwise idle developer machine using
`python3 bin/bench-hudi-java.py --baseline FULL_BASELINE_SHA --output NEW_DIRECTORY`
with production engine/Parquet libraries in `native/target/release`. Set `JAVA_HOME`
to a supported JDK and prevent system sleep for local timings. The
[processing-time investigation](hudi-processing-time-batches-20261114.json)
retains the local diagnostic and excluded contended attempts.

For complete qualification, select suite `hudi-benchmark` with
`hudi_full_matrix=true` and set `native_run` to a successful Hudi release
producer. Suite `hudi-native` builds and publishes only the two release libraries
using the same build/cache steps; it needs no Java comparison baseline and makes
no benchmark or correctness qualification claim. Use it when native sources have
changed. A successful focused `hudi-benchmark` run can also supply those libraries.
The full mode uses two million events and every unchanged query in four
isolated jobs: heap/RocksDB × mini-batching off/on. Native source/toolchain inputs
must match the producer, its release-build step must have succeeded, and its
library artifact must still be available. Each job downloads those same libraries
and records their hashes. No native rebuild is required for Java-only follow-ups.
`hudi_configurations=all` retains the default four configurations. For retries,
`off` selects both mini-batching-off configurations; `memory-false`, `memory-true`,
`rocksdb-false` or `rocksdb-true` selects one complete configuration. These selections
retain every query, engine, warmup and trial within that configuration. Preserve
successful configurations and rerun only unfinished ones; partial query reports
do not satisfy the complete-configuration and final native-cleanup gates.

`bin/bench-hudi-matrix.py` checks all 198 timing identities plus the exact Q7 stock
planner exclusion per configuration, full-value/count validation, fixed resources,
and actual RocksDB engagement. Partial or duplicate reports fail validation. Source
commit, producer, platform, library hashes and every trial remain in the artifacts.
Generated runtime tables are removed on success and failure. Focused and full
modes have separate workflow concurrency groups; run the required CI, upstream
Flink and docs gates for the final candidate after recording the results.
Each full configuration has a six-hour limit on slower hosted CPUs. The
launcher forwards the existing per-query/engine progress lines to the job console
while retaining the complete byte-for-byte Maven log, including failure output.
The selected queries, warmups, trials, resources and measured boundaries remain
the fixed matrix above; the timeout does not exclude slow controls or partial runs.

For a focused correctness reproduction, select suite `hudi-diagnostic`, one
explicit `hudi_configurations` value such as `rocksdb-false`, `hudi_queries=q10`
and a source-compatible `native_run`. This reuses the same two-million-event
runner, resources, warmup and three-engine rotating trials for the selected
unchanged queries. It publishes `hudi-diagnostic-*` artifacts and records
`diagnostic-passed`; it cannot qualify a complete configuration. The default full
matrix still requires all 198 timing identities and Q7's exact exclusion.

## Full Linux matrix (2026-11-15)

The [complete hosted campaign](https://github.com/jordepic/StreamFusion-contrib/actions/runs/43680106018)
passes all four configurations at source `67d03a0f775e7ea8d5339c9bfd8bdc316778d23d`,
native tree `54cfc7608463ba3a18aefac0d98764fcbfa88c4c`. All configurations reuse
the source-verified release libraries from producer `43663389459`. They include
the bounded window output and resource-ownership integration from upstream
`2a304d5`. Subsequent upstream canonical-state and Paimon integrations are outside
these timings; final merge qualification is separate from this measured source.

Geometric means of median speedups across the same 22 timed queries are:

| State | Mini-batching | Stock/native | Previous SF/native |
| --- | --- | ---: | ---: |
| Heap | off | **1.891×** | **11.232×** |
| Heap | on | **2.208×** | **13.724×** |
| RocksDB | off | **1.801×** | **9.284×** |
| RocksDB | on | **2.094×** | **8.974×** |

Native medians beat stock on **17/22 queries in every configuration**. All
**792 timing identities and four exact Q7 stock planner exclusions** validate
without combining source versions or partial attempts. Every deterministic
result passes full-value multiset comparison; Q12 uses processing-time bounded
count invariants. Both native Hudi boundaries, absence of payload transposes,
Rust user-payload Parquet footers and final native handle/memory cleanup pass.
Both RocksDB configurations also pass all three actual backend engagement checks.
The jobs remove their runtime tables and logs after retaining their reports.

The fixed configuration retains two million events, four physical Hudi
source/sink partitions, eight slots, 8 GiB heap, a 48 GiB logical native off-heap
limit, object reuse, one-second checkpoints, one warmup and three rotating
stock/previous/native trials. Each runner has four processors and JDK 17.0.21+9.
Heap/off uses an Intel Xeon Platinum 8573C host. Heap/on and RocksDB/off use
Intel Xeon 6975P-C hosts; RocksDB/on uses AMD EPYC 9V45. Physical memory is about
15.3–15.6 GiB. Compare engines **within each
configuration**: these hosts do not establish causal backend, mini-batch or
before/after effects. These are complete-job timings, not sustained throughput.

[Every observation, range, source/library hash and configuration](hudi-nexmark-linux-20261115.json)
is retained. The [raw evidence archive](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-matrix-20261115-67d03a0f7)
contains all four original reports/logs, the consolidation script, capacity
snapshot and verification record. Its 136,335-byte bundle has SHA-256
`ac00363b0aed7c9ab92614d4b4f1c5daac78162736e6d06c771ca6c52bfdf91f`;
the bundle and all 16 members were independently downloaded and checked before
local raw-log cleanup.

Q5/Q8/Q11/Q13/Q14 retain substantial stock losses in all four configurations.
For example, heap/on Q11 is 55,432 ms native versus 2,860 ms stock; RocksDB/off Q5
is 176,694 ms versus 19,677 ms. RocksDB Q5 also loses to previous SF in both modes.
Q8 on RocksDB/on has nearly identical previous/native medians with overlapping
trial ranges. [Issue #663](https://github.com/datafusion-contrib/StreamFusion/issues/663)
tracks these remaining costs. The tables retain every loss.

### Per-query results

Values are median milliseconds (minimum–maximum).

### memory, minibatching false

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 19,576.6 (19,259.1–19,922.6) | 39,112.6 (38,871.4–39,873.3) | 1,356.6 (1,261.1–1,622.4) | 14.43× |
| q1 | 19,597.7 (19,360.1–19,927.4) | 39,737.8 (39,481.2–40,038.1) | 1,377.0 (1,342.8–1,377.7) | 14.23× |
| q2 | 2,076.9 (1,944.4–2,132.3) | 15,544.2 (15,538.0–15,727.8) | 825.8 (743.5–838.4) | 2.52× |
| q3 | 2,225.2 (2,073.1–2,306.1) | 32,000.5 (31,883.2–32,021.3) | 1,205.1 (1,196.0–1,224.6) | 1.85× |
| q4 | 7,603.1 (6,810.2–8,505.7) | 178,320.8 (176,097.5–182,653.8) | 3,725.5 (3,364.5–4,794.6) | 2.04× |
| q5 | 15,875.2 (15,821.5–16,070.1) | 87,544.1 (87,534.5–88,309.4) | 78,360.7 (78,084.6–78,383.1) | 0.20× |
| q8 | 5,702.6 (5,622.5–5,937.3) | 53,993.9 (53,923.3–55,195.1) | 51,322.6 (50,504.9–51,912.2) | 0.11× |
| q9 | 16,902.6 (16,828.0–16,984.8) | 397,675.2 (393,362.6–400,368.1) | 8,627.0 (8,497.6–8,774.2) | 1.96× |
| q10 | 25,357.9 (25,210.5–25,737.2) | 59,424.5 (59,404.3–59,796.7) | 1,808.5 (1,804.3–1,851.8) | 14.02× |
| q11 | 4,031.1 (3,995.3–4,222.2) | 65,600.8 (65,518.4–65,610.9) | 63,681.1 (63,592.6–63,724.6) | 0.06× |
| q12 | 2,446.3 (2,429.6–2,469.5) | 63,380.0 (63,372.2–63,628.4) | 742.0 (723.9–763.4) | 3.30× |
| q13 | 15,130.6 (14,961.5–15,362.6) | 40,557.4 (40,484.7–40,558.0) | 34,724.3 (33,771.8–35,421.9) | 0.44× |
| q14 | 26,522.6 (26,130.7–26,926.6) | 57,183.5 (57,104.1–57,316.9) | 49,519.5 (49,345.0–49,671.6) | 0.54× |
| q15 | 18,394.1 (17,980.3–18,428.6) | 68,953.4 (68,950.9–69,576.3) | 16,636.4 (15,910.1–16,765.6) | 1.11× |
| q16 | 16,642.9 (16,491.8–16,774.7) | 91,619.5 (90,897.3–91,709.9) | 14,444.0 (14,224.4–14,495.0) | 1.15× |
| q17 | 17,379.9 (16,809.9–18,189.2) | 107,411.0 (107,352.8–107,748.2) | 10,620.0 (10,284.4–10,648.8) | 1.64× |
| q18 | 51,102.7 (50,924.7–51,907.2) | 204,097.9 (203,797.8–204,223.1) | 10,847.2 (10,575.3–10,900.8) | 4.71× |
| q19 | 114,347.6 (114,227.4–122,803.4) | 280,507.1 (276,681.4–282,996.0) | 34,514.3 (33,500.7–34,685.7) | 3.31× |
| q20 | 5,688.8 (5,688.7–7,210.4) | 136,020.7 (135,518.6–136,511.1) | 2,444.4 (2,314.2–2,570.1) | 2.33× |
| q21 | 2,834.4 (2,756.6–2,887.5) | 26,972.3 (26,589.8–27,301.6) | 2,146.0 (2,056.9–2,162.1) | 1.32× |
| q22 | 21,465.4 (21,146.8–21,716.0) | 41,208.6 (40,818.4–41,298.5) | 1,515.8 (1,473.4–1,523.0) | 14.16× |
| q23 | 77,165.7 (76,301.8–78,012.5) | 207,366.6 (206,921.6–208,644.8) | 5,230.0 (5,015.0–5,299.4) | 14.75× |

Geometric mean: **1.891× stock / 11.232× previous SF**.

### memory, minibatching true

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 15,466.5 (15,380.9–15,738.2) | 29,554.9 (29,361.7–29,985.3) | 1,090.5 (1,042.4–1,097.9) | 14.18× |
| q1 | 15,770.3 (15,198.9–16,086.6) | 30,111.4 (30,089.2–30,421.9) | 1,041.0 (1,032.5–1,151.1) | 15.15× |
| q2 | 1,496.5 (1,482.5–1,516.4) | 12,048.8 (11,939.1–12,157.8) | 607.1 (569.2–654.1) | 2.46× |
| q3 | 1,664.7 (1,633.0–1,779.0) | 25,270.6 (25,220.8–26,256.6) | 904.3 (903.1–980.5) | 1.84× |
| q4 | 4,571.2 (4,313.8–5,562.9) | 90,965.3 (90,653.7–91,809.8) | 2,133.4 (1,989.3–2,249.1) | 2.14× |
| q5 | 11,239.0 (11,157.1–11,254.4) | 64,327.7 (63,857.8–64,894.5) | 59,454.0 (59,352.7–60,303.4) | 0.19× |
| q8 | 4,019.0 (3,820.2–4,206.7) | 40,520.4 (40,498.6–40,854.9) | 38,461.3 (38,213.1–38,845.2) | 0.10× |
| q9 | 11,649.0 (11,566.1–12,623.5) | 160,221.0 (159,071.4–164,762.4) | 3,841.5 (3,643.6–3,919.3) | 3.03× |
| q10 | 19,945.2 (19,779.9–20,841.6) | 44,506.6 (44,435.1–45,587.5) | 1,322.7 (1,309.1–1,347.7) | 15.08× |
| q11 | 2,859.7 (2,814.6–3,002.3) | 56,128.2 (55,682.2–56,487.2) | 55,431.5 (54,518.4–55,459.0) | 0.05× |
| q12 | 1,716.0 (1,700.0–1,734.9) | 54,251.6 (54,000.0–54,413.2) | 649.3 (641.7–650.8) | 2.64× |
| q13 | 11,789.6 (11,692.4–12,092.1) | 30,288.4 (30,196.3–30,537.9) | 26,213.5 (26,178.2–26,643.7) | 0.45× |
| q14 | 21,551.9 (20,940.2–21,651.0) | 42,518.9 (42,358.2–42,591.2) | 38,086.1 (37,962.0–38,286.9) | 0.57× |
| q15 | 2,215.6 (2,085.0–2,329.2) | 25,465.4 (25,309.6–25,555.5) | 949.7 (928.4–994.8) | 2.33× |
| q16 | 2,653.6 (2,620.8–2,714.9) | 35,469.4 (35,451.6–35,585.9) | 1,529.8 (1,442.7–1,547.5) | 1.73× |
| q17 | 5,288.7 (5,050.6–5,686.9) | 34,950.7 (33,098.5–35,850.3) | 1,845.2 (1,827.9–1,897.5) | 2.87× |
| q18 | 40,757.6 (39,200.1–41,836.7) | 157,625.5 (157,487.8–161,612.4) | 8,855.7 (8,849.7–8,874.7) | 4.60× |
| q19 | 86,188.8 (84,472.3–88,952.9) | 123,509.0 (122,859.7–125,967.6) | 5,997.6 (4,861.0–6,301.6) | 14.37× |
| q20 | 3,841.5 (3,759.9–4,241.3) | 104,497.4 (104,226.6–105,108.7) | 1,711.2 (1,707.7–1,781.6) | 2.24× |
| q21 | 1,968.4 (1,950.2–1,992.5) | 21,095.0 (20,994.5–21,136.1) | 1,355.7 (1,327.8–1,382.3) | 1.45× |
| q22 | 16,236.6 (15,625.5–16,444.4) | 30,234.1 (29,822.7–30,234.7) | 1,177.6 (1,068.6–1,204.7) | 13.79× |
| q23 | 60,511.9 (59,508.3–64,178.5) | 114,741.9 (114,016.3–116,121.4) | 3,694.9 (3,324.4–3,857.2) | 16.38× |

Geometric mean: **2.208× stock / 13.724× previous SF**.

### rocksdb, minibatching false

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 15,919.8 (15,840.1–16,280.3) | 30,101.6 (29,215.9–30,560.4) | 1,054.8 (989.3–1,085.5) | 15.09× |
| q1 | 15,691.8 (15,377.3–15,777.1) | 30,218.9 (30,083.9–30,821.0) | 976.3 (969.3–1,110.0) | 16.07× |
| q2 | 1,414.4 (1,410.9–1,447.6) | 12,033.3 (11,977.6–12,058.6) | 654.5 (628.5–655.2) | 2.16× |
| q3 | 1,600.5 (1,448.2–1,702.3) | 25,496.8 (25,361.6–25,717.2) | 1,023.0 (956.7–1,117.8) | 1.56× |
| q4 | 15,432.1 (14,807.3–16,277.4) | 158,454.6 (156,960.6–170,166.3) | 8,354.8 (7,831.5–9,426.8) | 1.85× |
| q5 | 19,676.8 (19,663.0–20,319.2) | 141,110.6 (140,744.4–142,547.8) | 176,693.6 (173,066.4–178,622.7) | 0.11× |
| q8 | 5,651.2 (5,649.1–5,739.0) | 45,630.7 (45,017.0–45,698.5) | 44,287.4 (43,888.6–44,350.9) | 0.13× |
| q9 | 23,907.0 (23,688.2–24,041.1) | 329,589.3 (326,921.9–336,902.1) | 13,540.0 (12,629.7–14,291.6) | 1.77× |
| q10 | 19,776.0 (19,768.8–20,151.7) | 44,619.4 (44,466.7–44,678.0) | 1,363.5 (1,347.5–1,364.0) | 14.50× |
| q11 | 8,652.8 (8,637.6–8,693.4) | 85,781.4 (85,559.5–86,499.5) | 83,172.1 (83,022.1–83,994.6) | 0.10× |
| q12 | 1,832.5 (1,819.2–1,966.1) | 53,328.4 (53,123.9–53,546.4) | 702.8 (641.7–759.6) | 2.61× |
| q13 | 11,592.6 (11,265.6–11,597.7) | 29,691.7 (29,678.6–30,090.6) | 26,216.2 (26,116.9–26,319.8) | 0.44× |
| q14 | 20,137.6 (19,815.1–21,715.7) | 42,488.4 (41,760.7–42,767.2) | 37,965.2 (37,745.5–38,516.7) | 0.53× |
| q15 | 21,616.8 (21,423.0–21,910.3) | 69,906.8 (69,125.0–70,060.5) | 17,695.7 (17,655.2–18,202.4) | 1.22× |
| q16 | 16,810.7 (16,501.9–16,899.8) | 76,548.9 (76,491.3–76,663.5) | 14,268.0 (13,993.9–14,676.9) | 1.18× |
| q17 | 14,146.2 (14,060.0–14,435.9) | 82,954.0 (82,102.1–83,911.2) | 8,208.8 (8,183.2–8,290.3) | 1.72× |
| q18 | 45,155.8 (43,873.4–46,618.2) | 157,551.8 (157,176.0–158,959.5) | 10,327.5 (10,170.6–10,426.2) | 4.37× |
| q19 | 92,197.7 (91,778.4–95,345.5) | 186,116.4 (182,398.4–190,668.9) | 24,469.3 (23,718.6–24,762.3) | 3.77× |
| q20 | 10,915.6 (10,638.7–11,015.3) | 111,258.4 (110,103.9–114,076.1) | 6,250.9 (5,737.4–6,653.1) | 1.75× |
| q21 | 1,934.0 (1,917.6–2,006.6) | 21,268.0 (21,224.0–21,388.3) | 1,489.4 (1,432.8–1,586.2) | 1.30× |
| q22 | 16,472.6 (16,335.9–17,309.3) | 30,672.4 (30,541.5–30,830.7) | 1,087.3 (1,086.4–1,096.3) | 15.15× |
| q23 | 71,849.4 (71,449.9–78,736.8) | 186,150.4 (176,763.6–189,766.1) | 8,479.2 (8,220.7–8,761.9) | 8.47× |

Geometric mean: **1.801× stock / 9.284× previous SF**.

### rocksdb, minibatching true

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 10,431.4 (10,368.6–10,810.6) | 20,673.3 (20,666.0–21,455.6) | 1,020.7 (963.7–1,159.6) | 10.22× |
| q1 | 10,488.1 (10,321.1–10,695.2) | 21,598.7 (20,624.9–21,795.6) | 1,069.8 (1,064.2–1,203.8) | 9.80× |
| q2 | 1,648.3 (1,569.7–1,726.1) | 8,430.7 (8,278.6–8,761.7) | 652.3 (541.8–737.3) | 2.53× |
| q3 | 1,581.3 (1,554.6–1,618.4) | 17,130.5 (17,004.5–17,607.5) | 828.2 (766.4–867.5) | 1.91× |
| q4 | 10,188.2 (10,047.9–19,946.9) | 64,296.9 (63,916.5–65,702.9) | 3,454.1 (3,126.7–4,388.0) | 2.95× |
| q5 | 19,045.1 (17,667.0–21,114.1) | 124,406.6 (124,090.6–126,377.8) | 154,184.0 (152,802.1–155,165.3) | 0.12× |
| q8 | 4,845.1 (4,368.1–5,284.2) | 30,121.9 (29,809.4–31,065.5) | 30,146.5 (29,192.0–31,023.6) | 0.16× |
| q9 | 21,974.6 (20,404.7–24,310.7) | 102,314.0 (101,772.0–103,376.6) | 4,704.2 (4,236.6–5,218.9) | 4.67× |
| q10 | 13,593.3 (13,461.7–13,603.2) | 30,472.3 (30,465.0–31,414.4) | 1,320.0 (1,284.9–1,458.9) | 10.30× |
| q11 | 8,425.2 (8,237.6–8,525.5) | 66,550.1 (66,525.1–67,501.7) | 65,238.5 (63,574.9–65,926.0) | 0.13× |
| q12 | 1,767.7 (1,761.9–1,887.7) | 36,365.5 (36,187.9–36,582.0) | 729.9 (521.9–777.2) | 2.42× |
| q13 | 7,982.2 (7,940.1–9,460.8) | 20,991.2 (20,283.5–22,851.1) | 17,385.4 (16,712.3–17,700.9) | 0.46× |
| q14 | 14,695.6 (14,598.1–14,924.5) | 29,556.5 (29,224.2–31,279.9) | 25,700.0 (24,766.9–26,590.2) | 0.57× |
| q15 | 2,453.3 (2,427.6–2,499.1) | 16,670.9 (16,532.5–16,701.9) | 1,496.3 (1,273.4–1,681.9) | 1.64× |
| q16 | 3,503.7 (3,266.5–3,518.2) | 23,423.5 (22,593.5–24,389.0) | 2,970.4 (2,799.3–3,037.6) | 1.18× |
| q17 | 5,165.1 (5,063.4–5,324.8) | 24,418.4 (22,757.9–24,440.7) | 2,253.2 (2,032.0–2,499.4) | 2.29× |
| q18 | 38,674.1 (35,004.5–42,205.5) | 120,275.4 (118,563.1–130,247.9) | 10,323.5 (9,785.1–10,329.6) | 3.75× |
| q19 | 71,592.9 (71,592.7–73,108.0) | 84,071.9 (79,664.3–85,507.1) | 6,584.6 (5,965.1–7,189.4) | 10.87× |
| q20 | 10,177.0 (9,932.0–10,525.2) | 75,149.0 (74,592.8–78,296.3) | 2,350.2 (2,027.8–2,373.2) | 4.33× |
| q21 | 1,954.9 (1,846.0–2,153.3) | 14,556.3 (14,414.0–14,844.5) | 1,338.6 (1,296.0–1,360.0) | 1.46× |
| q22 | 11,654.1 (11,590.2–11,809.4) | 21,758.0 (21,248.3–21,789.1) | 1,126.0 (1,119.5–1,199.2) | 10.35× |
| q23 | 53,035.0 (52,475.7–53,999.4) | 101,342.2 (99,327.8–107,577.6) | 6,060.5 (5,874.7–7,216.4) | 8.75× |

Geometric mean: **2.094× stock / 8.974× previous SF**.


## Historical cohorts and Q10 failure

The preceding [partial Linux matrix](hudi-nexmark-linux-20261114.json), measured
at source `866620764` and native tree `16d4c0052bb61c9a6c783bd03bdafc6c02fe9896`,
qualified only its two mini-batching-on configurations: heap 1.271× stock /
10.277× previous SF, and RocksDB 1.321× / 8.095×. Its off jobs timed out at the
original three-hour limit. [All original reports and timeout evidence](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-matrix-attempt1-20261114-866620764)
remain archived; those partial attempts do not qualify the completed campaign above.

The [old RocksDB/off retry](https://github.com/jordepic/StreamFusion-contrib/actions/runs/43650417787)
failed Q10 previous-SF control trial 1: 1,696,199 rows instead of 1,840,000.
That control uses stock Hudi endpoints. Its 66 timings and Q7 exclusion remain
diagnostic only. Heap/off was cancelled when new native inputs superseded that
attempt; its 153 timings and Q7 exclusion also remain unqualified.
The [original failure archive](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-q10-failure-20261115-5cf84474a)
and [separate superseded-cohort/integration archive](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-integration-20261115-bf877576f)
were independently downloaded and verified before local raw-log cleanup.

The completed campaign validates all **36 timed Q10 identities** across four
configurations, including that previous-control trial, with 1,840,000 rows and
full values each. Earlier focused [local](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-integration-20261115-bf877576f)
and [Linux](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-q10-linux-20261115-480eefc2a)
repeats also pass. These later results do not establish the cause or claim a fix
for the archived failure on the older source. The [failure/repeat record](hudi-q10-validation-20261115.json)
retains the original identities and limits. On any new failure the harness
captures per-file physical row counts, timeline and commit totals before cleanup;
a metadata inspection error retains partial counts and the original assertion.

The measured source also shares production/corpus inputs with the full default
upstream Hudi replay at `bf877576f`: 349 enabled cases and one unchanged upstream
skip in each stock/native mode, with all native-work/fallback contracts intact.
Its [source-pinned compatibility report and provenance](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-upstream-20261115-bf877576f)
remain independently archived. This local compatibility evidence does not replace
final exact-head hosted CI and the full upstream Flink suite.

## Focused Java transport comparisons

The first [isolated comparison](hudi-java-comparison-20261114.json) passed on Linux
x86-64 with four available processors, JDK 17.0.21, 200k events, heap state and
mini-batching off. Two alternating baseline/candidate passes give six timed trials
per engine/query/variant. Medians in milliseconds:

| Query | Variant | Stock | Hudi-disabled SF | Native Hudi |
| --- | --- | ---: | ---: | ---: |
| Q8 | Baseline | 1,189.7 | 6,109.3 | 5,832.2 |
| Q8 | Batch proof + owner candidate | 1,312.0 | 6,140.9 | 5,894.3 |
| Q12 | Baseline | 597.1 | 6,251.3 | 6,120.8 |
| Q12 | Batch proof + owner candidate | 633.8 | 6,355.8 | 399.6 |

Q12 improved 15.3× over its native baseline and 1.59× over stock in this candidate.
The Q8 ranges overlap and do not establish an owner-lookup gain; that allocation
candidate was removed. All 72 trials, ranges and source/library hashes are retained.
The subsequent report measures the retained batch proof and Calc transport changes.
These focused results do not qualify the full two-million-event matrix.

The [retained-configuration repeat](hudi-calc-transport-20261114.json) removes the
owner trial and adds Calc input pruning/empty-result suppression. On that runner,
Q8 native medians are 7,824 -> 6,910 ms (1.13×), versus 1,890 ms stock and
7,182 ms Hudi-disabled SF after the change. Q12 is 8,138 -> 529 ms (15.39×),
versus 809 ms stock and 8,157 ms Hudi-disabled SF. It retains another 72 observations
with the same focused workload and alternating order. Compare variants within each
run; the two hosted runners have different absolute timings. Q8's remaining
3.66× stock loss and the full-matrix recheck remain open.

A subsequent local CPU diagnostic of that retained configuration passed six Q8
native iterations against the stock output. Its 11,013 samples place 33.8% of
captured CPU inside Arrow C Data export, including allocation/accounting work.
These inclusive shares overlap, and Calc chains include downstream operators;
they are not isolated component timings or a matched speedup comparison. The
[compact observations](hudi-q8-profile-20261114.json) record source/library hashes
and link the independently verified, versioned archive of folded stacks and logs.
Repeated handoffs remain a target for further profiling.

Historical development logs and diagnostics are preserved in a separate
[versioned evidence archive](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-development-20261114-866620764).
It contains 327 files across multiple source revisions, including interrupted or
withdrawn timings and rejected candidates. Their presence does not qualify those
runs or the current full matrix. The release manifest records every member's size
and SHA-256; the archive and all members were independently downloaded and verified
before removing local raw copies. Retrieve it without running the archived inputs:

```bash
gh release download benchmark-evidence-hudi-development-20261114-866620764 \
  --repo jordepic/StreamFusion-contrib --dir target/hudi-development-evidence
python3 - <<'PY'
from pathlib import Path
import hashlib, json
root = Path('target/hudi-development-evidence')
bundle = json.loads((root / 'manifest.json').read_text())['bundle']
assert hashlib.sha256((root / bundle['path']).read_bytes()).hexdigest() == bundle['sha256']
PY
tar -xzf target/hudi-development-evidence/hudi-development-evidence.tar.gz \
  -C target/hudi-development-evidence
```

The fixture is a COW table partitioned by `sf_partition`, populated once using
stock Hudi with a single ordered input and checkpoints disabled during seeding.
Measured jobs retain one-second checkpoints. Each event retains its original schema and timestamp; the storage
partition is an additional column outside the query projection. Queries retain
their original SQL, four-second watermark and output columns. A wrapper adds a
deterministic storage partition to the output. Source and sink parallelism are
four. The source has four physical partition files. Output keys hash to a
four-way storage partition; low-cardinality query results can occupy fewer than
four directories. The no-op bid query partitions by auction and must
populate all four sink partitions at smoke and representative sizes. Other
append queries use their first output column; updating queries use their first
primary-key column. A hexadecimal MD5 digit selects one of four equally sized
hash buckets. Append results use metadata-free COW files; updating results use version-9
SIMPLE BUCKET MOR with Parquet logs and commit-time ordering. Hudi retains Java
buffering, precombine, index, storage and commits; StreamFusion retains Arrow payloads and
encodes Parquet in Rust.

The default matrix uses heap and RocksDB state, with mini-batching off and on,
two million events, one warmup and three rotating measured trials. The three
engines are stock Flink, StreamFusion with its Hudi boundaries disabled, and
StreamFusion with native Hudi source/sink boundaries. Every native plan must
contain both boundaries and no payload transpose. Every nonempty output must
contain Parquet data with the expected Java or Rust encoder footer.
Encoder checks cover user data and log payloads. Hudi's own timeline/archive and
partition metadata retain their original Java implementation and are excluded.
Object reuse follows the shared Kafka harness: enabled unless `SF_OBJECT_REUSE=false`.
The first q4 warmup in each RocksDB configuration checks that each engine opens
an actual RocksDB database. Watcher setup, checks and teardown are outside
measured trials.

Timing includes planning, admission, reads, shuffles, processing, Java buffering,
Rust encoding and Hudi commits. After the bounded Flink job finishes, the timer
continues until Hudi's original timeline reports no pending commits. This avoids
planning a read before the final coordinator event becomes visible. Empty commits
are enabled for both engines so an empty final checkpoint also reaches a terminal
timeline state. Fixture creation, EXPLAIN and stock-Hudi result
readback are outside timing. The shared test MiniCluster supplies eight slots
and its standard task memory configuration. Resources are identical between
engines. Output comparison sorts full rows, including their storage partition,
with bounded heap and retains
duplicates. Processing-time q12 uses count invariants because its windows depend
on execution time.

```bash
SF_HUDI_BENCH=true SF_ROWS=2000000 SF_WARMUP=1 SF_RUNS=3 \
mvn -B -ntp -Phudi,flink-1.18,bench,hudi-nexmark \
  -pl streamfusion-hudi -am test \
  -Dtest=NexmarkHudiBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1
```

`SF_MATRIX_QUERIES=q0,q1,q4` selects queries. `SF_HUDI_BACKENDS=memory` and
`SF_HUDI_MINIBATCHES=false` select matrix dimensions. `SF_ROWS=2000`, no warmup
and one trial are useful correctness smoke settings, but are insufficient for
performance claims. Structured observations are written to
`streamfusion-hudi/target/hudi-nexmark.json`; override with
`-Dsf.hudi.benchmark.output=...`. Temporary tables and sorted comparison files
are removed after each run.

The completed heap configurations below are the baseline before persistent-window
deadline caching. The first RocksDB campaign stopped during Q5 to prioritize its
performance regression. The complete Linux matrix above follows that tuning.
macOS power logs confirmed repeated host
suspensions during the earlier campaign, and a later previous-path Q10 run failed
with an expired Hudi commit heartbeat. The earlier full-value comparisons remain
correctness evidence; their timings are retained as diagnostics and do not qualify
speedups: [Q0](hudi-nexmark-q0-20261113.json),
[Q1](hudi-nexmark-q1-20261113.json), and [Q4](hudi-nexmark-q4-20261113.json).

Prevent system sleep throughout measurement. On macOS, prefix Maven with
`caffeinate -is`; this assertion lasts only for that command and leaves the display
policy unchanged. Each measured observation records its UTC start, monotonic
elapsed time and wall-clock elapsed time. A clock gap of one second or more fails
the run. The power guard is still necessary because host suspension also affects
Hudi heartbeats and processing-time timers.

For focused CPU profiling, use `SF_HUDI_BENCH=true SF_HUDI_PROFILE=true` with
`SF_MATRIX_QUERIES` selecting one deterministic query and one state/mini-batch
configuration. Select `-Dtest=NexmarkHudiBenchmark#profileNativePipeline` with the
same release profile. This mode computes a stock reference once, then repeats
the native pipeline for `SF_WARMUP + SF_RUNS` iterations, checking full values,
encoders and final native cleanup. It reports `profiled`, so diagnostic captures
cannot be mistaken for a passed comparison matrix. It avoids repeating the slow
previous engine while investigating an admitted native path; normal qualification
continues to compare all three engines.

Stock Flink 1.18.1 rejects the unchanged q7 query over the Kafka corpus's
TIMESTAMP_LTZ rowtime when it compares that time with a TIMESTAMP window boundary.
The matrix records the exact planner rejection for this query and configuration;
it supplies no timing or parity claim for q7. The original schema and SQL remain
unchanged. Other planner errors fail the run.

## Baseline heap configurations

Both heap-state configurations passed on an Apple M4 (10 available processors,
16 GiB physical memory), JDK 17.0.17, with 8 GiB maximum heap and the shared test
cluster's 48 GiB logical native off-heap limit. The latter is a configured limit,
not reserved physical RAM. These short complete-job times include startup and
commits and do not establish sustained streaming throughput. Compare engines
within this report, not with the README's Kafka results from another machine.

[Structured observations, CPU profile summary and memory context](hudi-nexmark-20261114.json)
retain all 396 timed observations, both stock q7 rejections and the full configuration.
The overall report is marked `partial`: the two RocksDB configurations are incomplete.
All 21 deterministic queries passed full-value multiset comparison
in each completed configuration; q12 passed processing-time count invariants.
Payload encoders and final native resource cleanup passed. The largest
wall/monotonic clock discrepancy was 14.0 ms.

Geometric means of median speedups across all 22 timed queries, including
processing-time q12, are **0.55× stock / 7.51× previous StreamFusion** with
mini-batching off and **0.64× / 8.78×** with it on. Twelve and fifteen native
medians respectively beat stock; several differences are small relative to their
ranges. Q5/Q8/Q11/Q12 and the lookup/UDF paths remain substantially slower.
This matrix does not qualify every admitted pipeline as accelerated. The draft's
performance disposition remains open in issue #663.

The first configuration ran at `0037e0bfa`; the second ran with the `197d45bfd`
MOR reader fix, which preserves physical field layouts. The measured COW source,
append/MOR sinks, engine, codecs and harness are unchanged between these revisions.
The subsequent persistent-window deadline optimization has separate measurements
below and is not included in these baseline tables.

Values below are median milliseconds with minimum–maximum across three measured
trials per engine, following one warmup. A ratio below one means native is slower.

### Mini-batching off

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 986.1 (957.0–1,053.7) | 7,143.7 (7,140.2–7,368.9) | 529.9 (528.8–591.4) | 1.86× |
| q1 | 1,039.4 (974.4–1,069.8) | 7,317.8 (7,240.8–7,323.0) | 584.0 (575.7–587.9) | 1.78× |
| q2 | 597.5 (548.8–640.7) | 5,486.2 (5,417.6–5,490.3) | 364.9 (346.8–390.0) | 1.64× |
| q3 | 554.3 (553.4–557.7) | 15,393.6 (15,342.6–15,447.2) | 437.5 (430.1–452.3) | 1.27× |
| q4 | 2,039.6 (1,976.5–2,504.2) | 85,742.2 (83,596.4–88,342.1) | 1,323.7 (1,145.9–1,527.8) | 1.54× |
| q5 | 3,483.4 (3,419.2–3,588.3) | 32,159.3 (31,799.2–32,327.2) | 35,211.0 (34,940.9–35,375.9) | 0.10× |
| q8 | 1,296.4 (1,288.7–1,322.6) | 23,829.9 (23,444.2–24,234.6) | 29,411.0 (29,283.4–29,496.0) | 0.04× |
| q9 | 2,741.7 (2,676.7–3,021.2) | 166,255.0 (165,392.0–177,349.0) | 2,804.8 (2,769.9–2,835.9) | 0.98× |
| q10 | 1,217.2 (1,168.0–1,218.9) | 10,938.9 (10,845.9–11,013.2) | 671.5 (632.3–673.0) | 1.81× |
| q11 | 831.2 (809.1–842.4) | 30,535.2 (30,214.2–30,575.7) | 29,805.1 (29,661.2–30,026.8) | 0.03× |
| q12 | 563.2 (561.9–649.6) | 31,973.8 (31,885.9–32,042.1) | 31,553.6 (31,368.1–31,590.5) | 0.02× |
| q13 | 1,148.5 (1,088.8–1,285.0) | 9,573.9 (9,459.2–9,646.7) | 11,289.3 (10,970.3–11,460.1) | 0.10× |
| q14 | 1,205.5 (1,179.8–1,206.7) | 9,204.0 (9,158.5–9,421.2) | 13,565.1 (13,283.7–13,674.3) | 0.09× |
| q15 | 7,668.2 (7,604.7–7,719.7) | 27,506.3 (27,276.4–27,623.0) | 7,636.2 (7,566.7–7,707.1) | 1.00× |
| q16 | 4,705.6 (4,675.2–5,052.5) | 34,121.6 (34,109.0–34,804.2) | 5,310.6 (4,815.8–5,443.8) | 0.89× |
| q17 | 3,233.4 (2,921.8–3,364.2) | 35,431.1 (35,420.2–35,825.6) | 3,082.6 (2,788.6–3,210.8) | 1.05× |
| q18 | 4,052.6 (3,668.2–4,104.2) | 69,200.0 (67,687.0–69,340.2) | 5,231.2 (3,662.4–6,703.8) | 0.77× |
| q19 | 11,287.3 (10,884.4–14,690.8) | 59,540.8 (59,081.0–60,001.1) | 10,531.0 (9,826.4–13,355.9) | 1.07× |
| q20 | 1,527.6 (1,165.6–2,292.3) | 59,850.4 (59,053.6–59,988.7) | 1,013.6 (911.0–1,229.4) | 1.51× |
| q21 | 650.8 (650.5–853.5) | 8,749.3 (8,337.6–8,797.6) | 655.7 (645.6–878.6) | 0.99× |
| q22 | 1,308.4 (1,307.2–1,469.6) | 7,449.0 (7,411.2–7,678.1) | 623.0 (603.6–672.8) | 2.10× |
| q23 | 3,943.4 (3,744.8–4,016.1) | 62,203.8 (62,017.2–62,734.7) | 1,556.3 (1,544.9–1,621.5) | 2.53× |

Geometric mean: 0.554× stock, 7.513× previous SF.

### Mini-batching on

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 1,039.5 (997.8–1,073.7) | 7,398.4 (7,370.4–7,570.4) | 547.2 (538.0–548.6) | 1.90× |
| q1 | 1,086.1 (961.0–1,102.1) | 7,428.2 (7,426.5–7,638.0) | 566.3 (533.1–626.0) | 1.92× |
| q2 | 620.3 (568.8–636.4) | 5,634.0 (5,604.0–5,658.4) | 372.3 (346.3–404.9) | 1.67× |
| q3 | 565.0 (547.1–646.7) | 15,770.6 (15,739.5–15,977.6) | 436.2 (433.7–452.0) | 1.30× |
| q4 | 1,398.6 (1,269.7–1,578.2) | 50,070.6 (49,961.0–50,207.1) | 963.9 (950.4–1,175.0) | 1.45× |
| q5 | 3,829.0 (3,725.8–3,830.6) | 31,797.2 (31,794.1–32,089.4) | 35,934.2 (35,902.1–35,950.7) | 0.11× |
| q8 | 1,255.3 (1,241.9–1,380.5) | 21,951.2 (21,866.9–22,018.9) | 26,896.0 (26,870.4–26,949.7) | 0.05× |
| q9 | 2,852.9 (2,696.6–3,045.9) | 76,686.1 (76,227.9–80,805.1) | 1,596.6 (1,429.4–1,752.8) | 1.79× |
| q10 | 1,243.5 (1,225.4–1,324.6) | 11,471.6 (11,459.6–11,555.2) | 638.1 (628.2–645.7) | 1.95× |
| q11 | 848.7 (844.8–856.5) | 31,966.4 (31,958.6–32,190.8) | 31,290.2 (31,177.1–31,353.3) | 0.03× |
| q12 | 603.2 (578.9–610.8) | 33,661.3 (33,352.7–33,930.0) | 32,895.9 (32,874.9–33,038.3) | 0.02× |
| q13 | 1,099.7 (1,027.7–1,721.7) | 9,793.7 (9,792.4–9,872.7) | 11,475.1 (11,468.4–11,531.8) | 0.10× |
| q14 | 1,199.2 (1,182.4–1,217.5) | 9,510.1 (9,453.5–9,529.7) | 13,681.0 (13,645.7–13,765.6) | 0.09× |
| q15 | 850.5 (828.5–925.8) | 11,004.8 (10,975.9–11,042.8) | 448.8 (433.2–449.8) | 1.89× |
| q16 | 1,083.7 (1,008.2–1,118.3) | 14,990.1 (14,884.4–15,321.6) | 711.9 (677.0–730.5) | 1.52× |
| q17 | 1,302.5 (1,220.3–1,464.9) | 12,394.1 (12,031.0–12,602.9) | 1,146.6 (983.2–1,164.6) | 1.14× |
| q18 | 4,122.0 (3,933.7–5,051.2) | 65,727.5 (65,463.8–78,558.7) | 4,483.1 (3,817.4–6,261.9) | 0.92× |
| q19 | 11,508.4 (11,187.7–21,950.5) | 45,982.9 (45,940.6–46,601.5) | 2,867.4 (2,020.2–2,937.9) | 4.01× |
| q20 | 1,463.0 (1,371.6–1,756.4) | 64,212.6 (62,649.6–65,996.5) | 1,033.8 (1,005.8–1,085.5) | 1.42× |
| q21 | 704.4 (689.8–707.2) | 8,677.6 (8,667.4–8,689.9) | 676.2 (665.4–684.5) | 1.04× |
| q22 | 1,341.1 (1,306.6–1,385.4) | 7,742.0 (7,630.0–7,749.3) | 677.9 (571.5–687.2) | 1.98× |
| q23 | 3,616.1 (3,581.9–3,939.3) | 47,349.2 (47,209.3–47,472.6) | 1,931.0 (1,849.8–2,076.9) | 1.87× |

Geometric mean: 0.640× stock, 8.780× previous SF.

## Persistent-window deadline optimization

A focused Q5 RocksDB CPU capture attributed 71.4% of 19,743 samples to aggregate
firing and 14.8% to window-join firing. The shared state implementation now uses
the earliest pending end to avoid firing scans before any window is due. Inserts
lower that bound; restore and timer import rebuild it. Watermark positions,
late-row decisions, payload representations and checkpoint formats are unchanged.
Regional calendar firing retains its existing path.

The unchanged Q5 query at 200k events, four source/sink tasks, RocksDB and
mini-batching off compares the preserved old library with the updated release
library. Each uses one warmup and three rotating trials per engine, full-value
readback, encoder checks and final native cleanup.

| Engine | Before median ms (range) | After median ms (range) |
| --- | ---: | ---: |
| Stock | 1,390.3 (1,345.6–1,565.7) | 1,396.5 (1,246.9–1,643.3) |
| Previous SF boundaries | 25,769.0 (24,502.9–30,935.4) | 7,648.0 (7,604.6–8,504.8) |
| Native Hudi boundaries | 35,801.2 (35,071.8–41,816.8) | 10,118.9 (8,845.8–10,900.4) |

The native median improves **3.54×**, but remains **7.25× slower than stock**.
The previous-boundary path also benefits because the optimization is shared engine
state code. [All 18 timings, source trees, library hashes and CPU diagnostic](hudi-window-firing-20261114.json)
are retained. A separate smoke checks actual RocksDB databases for all three engines.
All 99 affected native window/store tests pass, including canonical state transfer.
These smaller complete jobs do not establish the two-million-event result or
sustained throughput. The later full Linux matrix above completes all four
configurations at its recorded source/native trees, including the subsequent
bounded-output integration. Q5 remains slower than stock in every configuration;
the combined results do not isolate this deadline optimization or establish
sustained throughput.

The subsequent cursor revision visits populated key groups through one iterator,
seeks from the earliest live end, and skips accumulator namespaces when loading
timers. A repeated deadline-cache baseline has medians of 1,289 ms stock,
7,635 ms Hudi-disabled SF and 10,039 ms native. The cursor revision records
1,267 / 7,593 / 8,004 ms respectively. Native ranges are 8,528–10,479 ms before
and 7,895–8,026 ms after: a 1.25× median improvement. The combined improvement
from the original 35,801 ms median is 4.47×, while stock is still 6.32× faster.
All 100 affected native tests pass, including negative window ends, keyless
timers and frozen read views. The same structured report now retains all 54
timings across retained, intermediate and rejected variants. Per-timer dirty
tracking was rejected after overlapping native ranges and a regression in the
Hudi-disabled path; the first per-group-only seek variant was also inconclusive.

## Remaining bottleneck

[Issue #663](https://github.com/datafusion-contrib/StreamFusion/issues/663)
tracks the remaining eager-watermark, handoff and allocation work.

An independent Q8 release CPU profile collected 4,163 samples. Inclusive shares
were 60.2% under watermark assignment, 62.7% under Calc, 16.2% under local window
aggregation and 13.7% under Arrow memory reservation; the shared Arrow owner
lookup accounted for 8.3%. These shares overlap and are not independent costs or
elapsed-time improvements. The profile identifies eager fragmentation, repeated
handoffs and allocation as follow-up work; it does not establish all causes of
the regression. The 29.4-second native median also loses to the previous path's
23.8 seconds, so the remaining cost cannot all be attributed to pre-existing core
operators. Windows retain their observable watermark boundaries.

### Host memory context

Two read-only OS/process snapshots during heap/mini-batching-on Q23's previous
engine trial recorded benchmark JVM RSS of about 6.6–6.4 GiB and about 1.8 GiB
of global swap usage. Global compression/decompression counters advanced, while
swap-in/out counters did not change during the five-second sample. These global
counters are not process-specific, and the JVM runs all three engines; this
sample does not establish the cause of any regression. It records the host's
memory conditions without excluding unfavorable observations. Future matched
profiles should measure memory pressure as well as CPU and allocation.
