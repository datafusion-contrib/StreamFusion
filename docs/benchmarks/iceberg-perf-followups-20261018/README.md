# Iceberg performance follow-ups

This cohort starts from merged main `0a02ecb8`. Release + mimalloc libraries are
unchanged; their hashes are in `environment.json`. Data-file payloads remain Arrow and
Rust Parquet. Java retains routing, delete encoding, rolling, checkpoints and commits.
Acceleration remains enabled by default. Streaming only.

## State installation component

`state-installation.jsonl` retains every warmup and measured iteration from two independent
previous/current JVM pairs. The real production canonical-state writer installs synthetic
native partitions in the real Flink heap backend. Both sides use 4 GiB heaps, two warmups
and five measurements per size. Setup, restore/readback, native encoding and filesystem
writes are excluded. Thread allocation requests are measured with the JDK ThreadMXBean;
they are not copied-byte measurements. Compressed process logs retain all assertions.

| Payload / groups | Previous medians, ms | Current medians, ms | Previous / current allocation, MiB |
|---|---:|---:|---:|
| 4 KiB / 128 | 0.652 / 0.777 | 0.593 / 0.518 | 1.098 / 0.596 |
| 2 MiB / 128 | 76.964 / 74.670 | 48.877 / 50.975 | 512.098 / 256.096 |
| 4 MiB + 1 / 16 | 23.060 / 26.072 | 18.316 / 18.225 | 128.018 / 64.017 |

This validates the removal of a full temporary payload array. It does not establish an
end-to-end checkpoint speedup. The saved format and actual checkpoint bytes do not shrink.

## Fresh state-heavy baseline

[state-before/results.md](state-before/results.md) retains the unchanged q7/q18/q20
memory/off comparisons: 2M events, four input/output buckets, p4, equal 4 GiB heaps,
one warmup and three alternating measured pairs, standard snapshot compression disabled.
All 12 pairs pass committed-result parity and native source/writer/footer witnesses.
All timings retain configured checkpoints and final drain/commit. The fresh baseline
ratios are 0.33× / 1.21× / 0.33× stock, with large ranges on both engines.

The seven-second kernel probe retains 134 blocked-thread observations: 133 on async
checkpoint workers, including 112 `folio_wait_bit_common` and 21 `jbd2_log_wait_commit`
waits; one is an Iceberg worker cloning a thread. Concurrent thread observations cannot
be summed into wall time. Stock q18 ranges 4.024–43.104 s and native 4.222–32.976 s.
This reproduces storage variability on both engines and does not establish a Parquet
read/write bottleneck. Temporary-copy removal reduces allocation and CPU work while
leaving the physical checkpoint format and standard compression setting unchanged.

## Dynamic allocation diagnostic

`dynamic-profile-before/` retains a separate whole-JVM CPU/allocation diagnostic with one
warmup and one measured pair for append and keyed format-v2 writing. It includes setup,
stock trials and untimed readback. Profiled durations are excluded from primary claims.
Inclusive sampled allocations under metadata extraction total 2.76 GB and control-record
construction 0.89 GB; these categories overlap other inclusive adapter counts. Extraction
includes reflection argument arrays and repeated equality-set copies. CPU sample counts
are overlapping observations, not elapsed-time percentages.

The experimental direct-main and incomplete-classpath probes were stopped and excluded:
they did not reproduce the normal shared-cluster lifecycle. Complete comparisons use
JUnit discovery and the repository's normal shared-cluster extension, 48 GiB task-budget
bookkeeping, 4 GiB heaps and `streamfusion.exchange.zeroCopyLocal=false`. These bookkeeping
budgets do not allocate 48 GiB of physical memory. Only matched complete cohorts are used
for performance claims.

## Metadata-only intermediate probe

The complete intermediate `metadata-probe/` cohorts retain 64 validated v2/v3 writer
trials (one warmup and three measured pairs per mode/format). They use typed handles
and the initial metadata cache, before integer-partition reuse and identity hashing. For the complete previous/native
comparison, source and release library baselines are frozen from `0a02ecb8`.

| Format / mode | Previous stock / native median, s | Intermediate stock / native median, s |
|---|---:|---:|
| v2 append | 2.349 / 1.817 | 2.378 / 1.877 |
| v2 keyed | 2.983 / 2.752 | 3.591 / 3.911 |
| v3 append | 2.464 / 1.870 | 2.332 / 1.734 |
| v3 keyed | 2.784 / 2.131 | 3.002 / 2.117 |

Metadata reuse alone does not establish a consistent end-to-end gain; the unfavorable
v2 keyed observations remain. The final selector optimization requires new complete
comparisons. All committed payloads, native footers, v3 keyed Puffin deletes and resource
release assertions pass. Per-trial observations retain ranges, file counts and resources.

`file-task-before/` retains the fresh 262,144-row main baseline for reads, writes and
normal/dynamic generator routing. It uses the existing fast-loop method, two warmups
and five alternating repetitions. File read/write medians are 1.94× / 1.41× stock.
This is a component baseline, separate from streaming lifecycle costs.

## Bounded partition-key reuse

The first broad integer-tuple prototype wins with four buckets but loses with 16,384
buckets: the cache clears frequently and misses repeat partition transforms. Both complete
high-cardinality cohorts remain in `dynamic-key-cardinality*` (0.60× / 0.62× stock).
The final optimization caches only bucket combinations whose maximum tuple cardinality,
including nulls, fits 1,024 entries. Identity/time and larger bucket spaces retain the
original dynamic selector. The data path continues to accelerate by default.

| Case | Stock median [range], ms | Arrow median [range], ms | Ratio |
|---|---:|---:|---:|
| Four buckets, final | 30.19 [28.83–31.26] | 12.55 [11.76–13.16] | 2.41× |
| 16,384 buckets, bounded admission | 29.45 [27.84–33.23] | 29.49 [28.59–38.08] | 1.00× |

Two earlier four-bucket repeats give 2.16× / 2.30×. The fixture takes about two seconds
per process, verifies every original Java-selected writer key, and excludes the full
catalog/processor, shuffle, writing and lifecycle. `component-final/` verifies reads,
writes and existing route/generator paths; their native encoders are unchanged and
cohort drift must not be attributed to the new dynamic routing changes.

## Retained component cohorts

These tables belong to the component methods described above. They are not complete streaming
job measurements, and repeated cohorts remain separate observations rather than extra samples
in a combined performance claim.

| Cohort | Recorded result |
| --- | --- |
| File-task baseline | [Read, write and route tables](file-task-before/results.md) |
| Final file-task components | [Read, write and route tables](component-final/results.md) |
| Initial four-bucket routing | [Measured table](dynamic-key-final/results.md) |
| Repeated four-bucket routing | [Measured table](dynamic-key-repeat/results.md) |
| Initial high-cardinality routing | [Measured table](dynamic-key-cardinality/results.md) |
| Repeated high-cardinality routing | [Measured table](dynamic-key-cardinality-repeat/results.md) |
| Bounded four-bucket routing | [Measured table](dynamic-key-bounded-final/results.md) |
| Bounded high-cardinality routing | [Measured table](dynamic-key-bounded-cardinality/results.md) |

## After-allocation diagnostic

`dynamic-profile-after/` retains another separate whole-JVM diagnostic with the same
one-warmup/one-measured-pair boundary as before. It overlaps unrelated local validation
work and is excluded from unprofiled performance claims. Sampled allocation estimates
under metadata extraction fall 2.761→0.315 GB (8.78× reduction); control-record
construction falls 0.893→0.372 GB. Inclusive dynamic-adapter allocations fall
12.815→8.495 GB. Categories overlap and include setup/readback reached from those
frames; they cannot be summed or treated as copied bytes. `dynamic-inclusive-counts.csv`
records numerators and all-JVM denominators. Runtime samples in the partition memoizer
confirm that the decorated released generator actually uses it. The reduction preserves
borrowed Arrow views and original Java controls.

## Correctness checks

The complete released streaming Iceberg selection passes 309 tests, with 13 explicit
batch exclusions. This includes unchanged upstream SQL/control tests, branch/upsert
writers, native reads/writes, source recovery and real captured Arrow-channel recovery
with rescaling. Ten focused shared-state tests also pass: chunk boundary ownership and
restore, CRC/version checks, incremental RocksDB checkpoints and canonical backend
migration. The shared-state fixture compiles against the runtime's Java 11 API target.
Compressed logs and exact Maven commands are retained in `validation/`.

The final bounded-bucket guard and factored handle signature additionally pass 18
focused dynamic/partition tests, including all eight live dynamic-sink cases. This
adds compound-bucket/null cardinality and overflow checks to the original cache,
metadata-mutation, branch/upsert and recovery selection.

## Complete dynamic writer cohorts

All complete cohorts below use 2M Arrow rows, p4, four buckets per target, Zstd 3,
4 GiB heaps, the normal shared-cluster lifecycle, Arrow IPC shuffle, one warmup
and three alternating measured pairs per mode. Every trial validates all committed
payloads, native data footers, v3 keyed Java Puffin deletes and resource release.
Timing includes routing, shuffling, encoding, checkpoints and final commit.

| Cohort | Format / mode | Stock median [range], s | Native median [range], s | Stock/native |
|---|---|---:|---:|---:|
| baseline-dynamic-v2 | v2 / forward-hash | 2.688 [2.588–2.709] | 1.783 [1.735–2.011] | 1.51× |
| baseline-dynamic-v2 | v2 / keyed | 3.980 [3.813–4.933] | 3.423 [3.016–3.607] | 1.16× |
| baseline-dynamic-v3 | v3 / forward-hash | 2.554 [2.398–2.924] | 2.145 [1.849–2.148] | 1.19× |
| baseline-dynamic-v3 | v3 / keyed | 2.668 [2.613–3.596] | 2.202 [2.170–2.498] | 1.21× |
| bounded-current-v2 | v2 / forward-hash | 2.394 [2.335–2.477] | 1.662 [1.607–1.803] | 1.44× |
| bounded-current-v2 | v2 / keyed | 3.587 [3.423–3.736] | 2.766 [2.500–3.034] | 1.30× |
| bounded-current-v3 | v3 / forward-hash | 3.389 [3.018–3.441] | 1.780 [1.664–3.086] | 1.90× |
| bounded-current-v3 | v3 / keyed | 3.508 [2.935–3.832] | 3.225 [2.940–9.048] | 1.09× |
| idle-previous-v2 | v2 / forward-hash | 2.348 [2.285–2.527] | 1.769 [1.719–1.872] | 1.33× |
| idle-previous-v2 | v2 / keyed | 3.573 [3.330–3.774] | 2.704 [2.495–2.915] | 1.32× |
| idle-current-v2 | v2 / forward-hash | 2.687 [2.656–3.404] | 2.001 [1.651–2.607] | 1.34× |
| idle-current-v2 | v2 / keyed | 3.725 [3.100–3.737] | 3.301 [2.341–4.151] | 1.13× |
| idle-current-v3 | v3 / forward-hash | 2.390 [2.341–2.778] | 1.603 [1.559–2.597] | 1.49× |
| idle-current-v3 | v3 / keyed | 3.853 [3.258–4.365] | 2.346 [2.180–2.382] | 1.64× |
| idle-previous-v3 | v3 / forward-hash | 2.385 [2.325–2.410] | 2.050 [1.807–2.544] | 1.16× |
| idle-previous-v3 | v3 / keyed | 2.617 [2.587–2.837] | 2.092 [2.058–2.189] | 1.25× |

The `idle-*` cohorts were launched after the machine became idle; another session
started short Java workloads during collection. Their host-load log is retained.
The earlier `bounded-current-v3` cohort overlaps a larger foreign test run and keeps
its 9.048 s native keyed observation. No samples are dropped or replaced.

These complete-job cohorts do not establish a consistent gain over previous native
behavior: append gains and keyed regressions coexist with changing stock baselines
and checkpoint/file-count variation. The measured benefit is lower routing/allocation
cost at the component boundary; whole-job limits remain explicit. The full normal-SQL
Nexmark matrix is evaluated separately.
