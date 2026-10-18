# Iceberg streaming diagnostics

These profiles investigate the q3/q4 regressions observed during the full benchmark.
They are diagnostics, excluded from the final matrix and its geometric means.
Both engines use 2M deterministic events, parallelism four, four-bucket Iceberg
tables, a 4 GB job heap, one-second checkpoints and identical Parquet settings.
The production libraries are the same isolated release+mimalloc files used in the
matrix; hashes and environment are retained with each cohort.

`cpu/` contains one paired trial for q3/q4 in memory/off, memory/on and disk/off,
without a warmup. `wall/` contains one warmup and one measured pair for q4 in
memory/off and memory/on. Each archive retains the thread-labelled collapsed
profiles, plan, graph, final table metadata and checkpoint file sizes. All ten
pairs passed committed-result parity and native/stock file witnesses. The released
async-profiler 2.9 starts before each trial timer and stops before stock readback.
Profiled durations must not be substituted for unprofiled measurements.

The initial unprofiled q4 memory/off medians were 11.204 s stock and 15.590 s native;
individual samples ranged from 2.949–18.503 s and 2.355–16.506 s respectively.
The native median corpus/window phase was faster (1.504 s versus 2.409 s), while
checkpoint/drain waits varied substantially. The CPU diagnostic reproduced a
native memory/on drain wait (3.370 s), and the wall diagnostic reproduced long
waits in both engines. This is not evidence that Rust Parquet encoding is slower.

In the CPU-profiled q4 disk/off trial, the native updating join appears in 49.1%
of all JVM CPU samples and the native backend savepoint in 13.4%; the retained
Java sink flush appears in 2.8%. These are inclusive, overlapping sample shares,
not wall-time percentages. Stock RocksDB decompression appears in 61.0% of its
CPU samples. The next comparison holds native query execution constant and
switches only Iceberg IO, to distinguish connector costs from state/checkpoint work.

`io-baseline/` retains that six-cell comparison (one warmup and three measured
pairs per cell), with the original canonical-export protocol. Both legs run
native query operators; the baseline retains stock Iceberg IO and the row/Arrow
transposes. All 24 pairs pass. GC logging records 417 pauses, a maximum of
239.344 ms and 3.435 s total. Kernel snapshots at a nominal 200 ms interval
instead repeatedly find Java checkpoint workers in `jbd2_log_wait_commit` and
`folio_wait_bit_common`. Sample counts overlap across threads and are not summed
into elapsed time. The compressed raw GC/kernel logs are retained.

`normal-checkpoint/` is a separate storage/protocol probe, with one warmup and one
measured pair for each of the same six cells. All 12 pairs pass. It requested
JobManager checkpoint storage, but Flink's test MiniCluster supplies a filesystem
checkpoint directory; the inherited metadata location still persists the state.
It is excluded from final results. Flink's `MiniClusterResource` explicitly chooses
filesystem checkpoints for tests, including the existing Kafka headline harness.

The final protocol keeps that shared filesystem storage, waits for an ordinary
configured checkpoint, then drains through a backend-native savepoint. It keeps
checkpoint/commit time and the final disk export measured, while avoiding the
extra canonical migration exports. This changes benchmark completion, not the
production reader, writer, query or state backend. The first filesystem preflight
q4 memory/off pair is 6.741 s stock and 3.624 s native; this single diagnostic pair
is not a repeated performance claim. The final matrix reruns every cell under
the same revised protocol rather than mixing old and new observations.
`filesystem-checkpoint/` retains both validated preflight pairs, including q4
disk/on (46.452 s stock, 2.079 s native), checkpoint IDs/bytes/durations, native
task and file witnesses, and the final savepoint sizes. These remain preflight
observations, separate from the repeated final matrix.

## q9 memory/on checkpoint sensitivity

The uncompressed primary cell has medians of 3.611 s stock and 22.942 s native,
with ranges 3.127–35.944 s and 10.768–46.235 s. Native corpus progress is faster
(median 1.073 versus 2.882 s); checkpoint/drain waits dominate the regression.
`q9-cpu/` retains a separate warmed CPU-profile pair that reverses the elapsed
ranking (37.809 s stock, 2.474 s native), reproducing the storage variability.
In the native profile, the data-write JNI entry appears in 0.1% of CPU samples,
sink processing in 0.7%, sink flush in 1.2%, source fetch in 1.9%, and join-state
snapshot encoding in 10.5%. These inclusive frame shares overlap and omit time
blocked in uninterruptible kernel IO; they do not support blaming Parquet encoding.

`q9-compressed/` separately enables Flink's standard snapshot compression for
both engines, with one warmup and three measured pairs. All four pairs pass.
Checkpoint bytes fall from roughly 254/266 MiB stock/native to 49.63/52.86 MiB.
Measured medians are 3.312 s stock [3.261–3.440] and 2.026 s native
[1.772–2.079], a 1.63× ratio. This is an operational tuning comparison, not
a production connector change or a replacement for the uncompressed primary cell.
Its compression setting is explicitly recorded and cannot be mixed on resume.

## q18/q20 and the previous I/O path

`q18-cpu/` and `q20-cpu/` retain one warmup and one measured stock/native pair
for memory/off. Both pairs pass for each query. Their measured native elapsed
times are 40.465 s and 40.554 s; stock is 4.329 s and 18.128 s. These profiled
observations are excluded from the primary medians.

[Inclusive CPU counts](inclusive-cpu-counts.csv) retain the exact frame substrings,
sample numerators and all-JVM denominators. In q18, source fetch is 3.0% stock /
1.6% native, sink processing 16.8% / 15.0%, and the native data-write JNI entry
4.0%. Native dedup snapshot encoding appears in 26.9%. In q20, source fetch is
7.3% / 2.6%, sink processing 0.3% / 0.9%, native data writes 0.4%, and native join
snapshot encoding 10.1%. Native Java Parquet samples are expected from the retained
Java delete writers. Percentages are inclusive, overlap, and describe CPU rather
than blocked wall time.

`q18-compressed/` and `q20-compressed/` enable standard Flink snapshot compression
for both engines, with one warmup and three measured pairs per cell. All twelve
pairs pass. Compressed checkpoint state sizes and repeated medians are:

| Query / mode | Stock state, MiB | Native state, MiB | Stock median, s | Native median, s | Ratio |
|---|---:|---:|---:|---:|---:|
| q18 / memory-off | 64.69 | 94.32 | 4.149 | 3.601 | 1.15× |
| q20 / memory-off | 36.56 | 36.02 | 1.906 | 1.406 | 1.36× |
| q20 / memory-on | 36.56 | 36.03 | 11.155 | 5.253 | 2.12× |

The uncompressed q18 checkpoints are approximately 254/261 MiB stock/native;
q20 is approximately 215/220 MiB. Storage variability remains: q18's compressed
native samples range from 3.600–30.323 s. Every range is retained in the individual
reports. These comparisons establish a useful tuning option, not a guarantee that
compression removes all waits or a reason to replace the primary defaults.

`q9-disk-io-cpu/` separately holds native query execution fixed and switches only
Iceberg I/O, with one warmup and one measured pair. Both pass. The measured stock-I/O
leg takes 28.736 s and native I/O 125.693 s. Source fetch appears in 1.2% / 0.8%
of CPU samples, sink processing 4.8% / 4.3%, sink flush 2.5% / 3.2%, and RocksDB
frames 42.4% / 27.6%; the native data-write entry appears in 1.6%. The slower
native observation is retained. Small CPU shares do not rule out I/O waiting in a
component. Alongside the earlier wall/kernel traces and phase metrics, this points
to state/checkpoint I/O and input scheduling as investigation targets rather than
establishing a Parquet-encoding bottleneck. The disk regression against previous
StreamFusion I/O remains a measured limitation; supported contracts now accelerate by default.
[Issue #391](https://github.com/datafusion-contrib/StreamFusion/issues/391) tracks the
state/checkpoint and input-scheduling investigation.
