# Separate q0/q19 CPU diagnostics

These observations are excluded from the primary matrix. Released async-profiler
4.5 records `itimer` CPU stacks for one warmup and one measured pair per query,
using the same two-million-event corpus, source/sink partitioning, RocksDB budget,
mini-batch-off configuration and exact release-library hashes as the main run.
All four profiled pairs pass their normal committed-output and native-boundary checks.

| Query | Profiled stock, seconds | Profiled native, seconds | Primary median stock/native |
|---|---:|---:|---:|
| q0 | 2.255 | 1.361 | 0.41× |
| q19 | 30.723 | 9.487 | 0.56× |

Neither primary regression reproduces in this smaller, fresh profiled cohort.
These one-pair timings cannot replace the repeated unprofiled medians. They show
why elapsed-time reversals need a longer-cohort wall/GC reproduction before assigning
the primary regressions to the encoder. [The existing state/checkpoint investigation](https://github.com/datafusion-contrib/StreamFusion/issues/391)
retains earlier CPU/wall/kernel evidence; this diagnostic does not resolve that issue.

[Raw CPU counts and matching frame patterns](itimer/inclusive-cpu.csv) use inclusive
stack matches, excluding stacks that contain a C1/C2 compilation entry. Compilation
can annotate methods from the other engine, so it must be excluded before attributing
runtime source/writer work. The q0 pair spends 40.1% stock / 38.2% native of all sampled
CPU in compilation. Percentages below use the remaining execution samples, overlap,
and are not elapsed-time percentages.

| q19 CPU boundary | Stock samples / 5,106 | Native samples / 2,350 |
|---|---:|---:|
| Source chain/fetch | 246 (4.8%) | 149 (6.3%) |
| Sink row/batch processing | 1,286 (25.2%) | 785 (33.4%) |
| Sink pre-checkpoint flush | 529 (10.4%) | 504 (21.4%) |
| Data encoding: Java DataWriter / Rust write+finish | 526 (10.3%) | 189 (8.0%) |
| Java position-delete finalization | 477 (9.3%) | 494 (21.0%) |
| Java delete-key bookkeeping | 468 (9.2%) | 394 (16.8%) |
| Stock rank processing / native rank push | 1,363 (26.7%) | 291 (12.4%) |

Source-chain matching may include chained calculation/routing work. The two data
encoding boundaries use different implementations and are not an isolated encoder
speed test. In this profile, Java position-delete finalization has similar absolute
sample counts on both engines while its share grows as other native work falls.
That retained Java work is part of the agreed design; data files remain Rust encoded.
[Main q19 committed-file summaries](itimer/q19-main-file-volumes.json) retain comparable
physical row/delete volumes, so the primary slowdown is not explained by a large
increase in output rows alone.

The [profile environment](itimer/environment.json), [method](itimer/method.json),
[all observations](itimer/observations.jsonl), [sample summary](itimer/summary.json),
compressed log and per-query archives retain every warmup/measured profile, graph
and final table metadata. They use docs-only commit `80d63ba7`; production library
hashes match the primary matrix exactly.
