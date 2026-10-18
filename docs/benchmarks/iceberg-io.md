# Fast Iceberg reader and writer comparisons

Use this loop while changing the connector. It compares the production file-task
boundaries with the released Flink Iceberg reader/writer, without starting a Flink
cluster. The default process takes about 8–9 seconds on the recorded benchmark host,
including validation; an unchanged build check takes about 0.04 seconds. Initial
release compilation and dependency preparation are separate, one-time costs.
[Recorded before/after comparisons](iceberg-boundary-20261017/README.md) retain
full ranges, profiles and focused streaming confirmation.

```sh
# JDK 17, Rust and Maven required; run from a checkout.
bin/bench-iceberg-io.py --output target/io-before
# Edit connector Java or Rust, then compare with the previous native result.
bin/bench-iceberg-io.py --baseline target/io-before/observations.jsonl
# Focus on one side or change the workload.
bin/bench-iceberg-io.py --cases read --rows 65536 --stripes 1
bin/bench-iceberg-io.py --cases write --batch-rows 1024 --width 264
```

The runner builds release + mimalloc libraries and prepares the Java dependency
classpath once. Subsequent connector Java edits compile directly with `javac`;
connector Rust edits use incremental Cargo. Core Java or dependency changes refresh
the Maven preparation. Libraries are staged under immutable content-derived paths,
so rebuilding does not overwrite a library mapped by a JVM. `--prepare` refreshes
the full cached preparation explicitly. Ordinary runs verify source fingerprints
before executing; they do not silently benchmark stale connector classes.

By default each implementation receives 262,144 deterministic rows with six scalar
columns: long ID, nullable integer/double/Unicode string/boolean and a timestamp.
The table has four bucket partitions. Both sides use Zstd level 3, 128 MiB row groups,
1 MiB pages, 2 MiB dictionaries and a 512 MiB rolling target. Source fetch batches
match the requested 8,192 rows on both sides. Two warmups and five measured runs
alternate implementation order. Controls include `--rows`, `--batch-rows`, `--width`,
`--partitions`, `--stripes`, `--target-bytes`, `--warmup`, `--runs` and `--cases`.

Reading reuses immutable Java-written input files across invocations: four files per
partition by default. Both readers consume identical Java-planned file tasks and
perform real local FileIO. Stock uses `RowDataReaderFunction`; native uses the
production Arrow reader function, including JNI, schema/metadata translation,
decode, C Data import, file close and batch recycling. Native/fallback task counters
must show all tasks decoded natively. The timed consumer counts decoded records.
Separate untimed scans verify every field and detect duplicate/missing IDs.

Writing starts with equivalent prepared RowData and Arrow inputs, each in its
normal engine representation. It runs the real Java task writer factory, and the
native leg runs the production Arrow batch writer and appender. Only the Flink
operator's metric initialization is supplied directly. Writer creation, Java row-view
traversal/partitioning, rolling checks, JNI, encoding, close and file metrics remain
timed. Preparing retained fixture views is outside timing. Every produced file is
then decoded by stock Java Iceberg, every value/ID checked, and the footer required
to identify the expected Java or Rust encoder. Native handles and additional
Arrow/task reservations must be released after each trial. Output payload files
are deleted after validation; raw observations, ranges, build fingerprints, library
hashes and logs remain under `target/iceberg-io/` or `--output`.

The optional `--cases dynamic-route` compares the production dynamic generator
with its original uncached implementation, including borrowed Arrow row views. It
checks every table/branch destination and excludes the host processor, catalog,
shuffle and file writing. [Candidate results](iceberg-boundary-round2-20261017/README.md)
retain gains, inconclusive writer timings and rejected prototypes.

For CPU, allocation or wall profiles, use a released async-profiler installation:

```sh
bin/bench-iceberg-io.py --cases write --profile itimer --profiler /path/to/profiler.sh
bin/bench-iceberg-io.py --cases read --profile alloc --profiler /path/to/profiler.sh
```

For hash-routing edits, `--cases route` isolates Java partition transforms and
key-group assignment, comparing the stock row selector with the production Arrow
routing helper. It verifies every destination against stock. This case takes about
two seconds including validation on the recorded host. It excludes Rust batch
splitting, exchange transport and key-group-to-writer range mapping.

Each profiled trial retains a thread-labelled collapsed profile. Profiling surrounds the
timed production boundary and excludes validation. Profiled durations are diagnostic;
use unprofiled runs for performance comparisons. More rows or repetitions provide
more samples when a boundary is too short for useful sampling.

This is a warm local file-I/O benchmark, not a full Flink job or a Nexmark result.
It excludes discovery, source fetcher queues, network/Arrow IPC shuffle, query state,
checkpoint barriers, table commits and recovery. The default writer is append-only;
keyed delete behavior, dynamic routing, range distribution and recovery stay in the
streaming SQL/connector suites. Once a change wins here, run a focused release
Nexmark confirmation and the relevant SQL/recovery tests. Rerun the complete matrix
when preparing the final end-to-end comparison, not after every edit.
