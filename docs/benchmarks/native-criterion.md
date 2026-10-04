# Native Criterion suites

Criterion benchmarks call production Rust implementations through thin adapters, with Arrow
fixtures built before measurement. They isolate where allocation and materialization costs arise
inside a native island. They complement release-mode Flink comparisons; they cannot establish an
end-to-end speedup or replace both transposes, JNI, scheduling, or host-backed I/O in those comparisons.

## Running and comparing

Run from a checkout with Rust, Maven, and a JDK available. The runner discovers workspace benchmark targets
using Cargo metadata, so a new suite is picked up without another hard-coded runner list.

```sh
python3 bin/bench-native.py --list
python3 bin/bench-native.py --smoke
python3 bin/bench-native.py --bench handoffs --save-baseline before
# Change the implementation, using the same host, toolchain, configuration, and fixtures.
python3 bin/bench-native.py --bench handoffs --baseline before
python3 bin/bench-native.py --bench scalar_registry --filter decimal
```

`--package` and `--bench` are repeatable. Cargo's benchmark profile is optimized; do not change it
to a debug profile when collecting timings. Criterion retains its samples and baseline data under
`native/target/criterion`. The runner writes each suite's combined output, `allocations.csv`, and
`metadata.json` under a timestamped `native/target/native-benchmarks/` directory, or `--output`.
Metadata includes commit, dirty status, toolchain, platform, commands, and relevant build settings.
Allocation CSVs use LF line endings so retained probes produce clean repository diffs.
For `scalar_registry` and `jvm_truncate`, the runner compiles the production Java classes and resolves
their released Maven dependencies. The latter also compiles test fixtures: its reference wrapper
supplies Flink's generated NULL guard around released `struncate`, with the same boxed argument types
as the shortcut. An embedded JVM keeps SQL/JSON buffer-recycler calls in the measured path; VM
startup is outside measurement. The JVM uses a 128 MiB initial and 256 MiB maximum heap, UTF-8,
and UTC. Maven setup logs and resolved classpath are retained. Set `SF_NATIVE_BENCH_CLASSPATH`
to reuse a classpath when invoking the Criterion executable directly.

`jvm_truncate` additionally registers the production generated Flink expression evaluator and
the borrowed-row shortcut evaluator.
Generation and opening happen before timing; per-row evaluation and Arrow/C Data conversion stay
inside the same Calc boundary, with exact output comparisons against the released reference.
Checks compare DOUBLE bits, NULL positions and schemas, preserving signed-zero distinctions.
All 72 upcall fixtures pass. The [four-way measurements](recovered-historical-diagnostics-2026-10-01.txt)
retain the partial borrowing improvement and slower fallback cases. The earlier [three-way measurements](recovered-historical-diagnostics-2026-10-01.txt)
include slower fallback profiles; see the [optimization ledger](../optimizations/host-exact-builtins-upcall.md#bounded-double-truncate-shortcut).
Retain the actual diff when comparing an uncommitted implementation. Baselines must have matching
case names and representative data. Existing comparison suites include experimental alternatives;
only cases explicitly calling the production implementation describe shipped behavior.

For repeated persistent GROUP BY comparisons, use `--bench persistent_group`.
For an engine run with mimalloc, add `--package streamfusion --features mimalloc`;
features are passed to each selected package and retained in metadata and Cargo
commands. Select packages that provide the requested features. For example:

```sh
python3 bin/bench-native.py --package streamfusion --bench persistent_group --features mimalloc --smoke --filter group_retract
```

This focused target registers the same production adapters, fixtures and oracles
as `persistent_state`, including COUNT/SUM retractions, without constructing
unrelated join, window or session fixtures. The comprehensive target retains
those GROUP BY fixtures. Filters still restrict timing rather than fixture probes
within the selected target.

Full timing runs are intentionally long. Start with a case filter for a suspected copy, then cover
the operation's other profiles before accepting an optimization. Run timing suites sequentially on
an otherwise quiet host. Inspect Criterion's confidence intervals and repeat noisy comparisons;
report input rows per second only with the stated output cardinality. Smoke mode uses Criterion
`--test`, executes each fixture once, and produces no timing baseline. CI runs smoke checks and
retains diagnostics without enforcing performance thresholds on shared runners.

## Allocation and buffer diagnostics

New suites perform an untimed allocation probe outside Criterion's warmup and sampling. Stateful
fixtures use a fresh operator with the same initial state as their timed case. Cache-reusing scalar
and decoder fixtures warm up before probing. Fixture setup and allocation-report traversal are outside
the probe. Stateless `iter` cases include output disposal; operator cases using `iter_batched_ref`
exclude operator construction and destruction. Cases that consume the setup operator include its
routine-side destruction. Check the individual suite when selecting a baseline.

The benchmark-only allocator delegates to Rust's system allocator and records allocation,
zeroed-allocation, and reallocation requests on the measured thread. Reallocation counts the entire
requested size, not its growth delta. This is allocation traffic, **not peak memory or copied bytes**.
C/C++ allocations, RocksDB background workers, JVM allocations, and other threads are outside the
counter. The allocator has a disabled counting check during timed execution; compare runs with the
same instrumentation. The default runner does not enable mimalloc. When a retained
run explicitly enables the engine `mimalloc` feature, record that configuration and
verify the executable allocator aliases; the counting wrapper still excludes
RocksDB C++ and background-thread requests.

Buffer diagnostics recursively inspect Arrow value, offset, validity, and child buffers. An output
range wholly contained in an input buffer is classified as shared; other ranges are new. Identical
ranges are counted once. Slices can retain larger backing allocations, and overlapping nonidentical
ranges are not merged, so these lengths are neither live heap usage nor logical payload size. New
buffers may hold computed results, rather than copies. For encoded byte vectors and checkpoint
files, the Arrow output columns are empty: zero output-buffer bytes means the metric is inapplicable,
not that encoding avoided allocation. Scalar registry probes time UDF invocation returning a
`ColumnarValue`; expansion of a scalar result to an Arrow array is outside that timing and allocation
probe. The Calc suite includes production result materialization. Case filters restrict Criterion
timings; allocation CSVs also include the fixtures constructed before the filter is applied.
Correlate these probes with timing and a CPU/allocation profile
before proposing a copy-removal change.

## Coverage inventory

Most new suites vary 16, 1,024, and 16,384 input rows. Codec and handoff suites include 8-, 264-, and
4,096-byte payloads; state suites vary one, 64, and batch-sized key cardinalities. Profiles deliberately
include sliced arrays, nulls, and Unicode where the operation handles them. Updating-join probes use
one right-side row per key; window joins use unique join keys. This bounds output cardinality rather
than accidentally constructing a quadratic result while investigating per-row materialization.
Some historical suites use
4,096 or 8,192 rows and remain available for their original comparisons.

| Production boundary | Suite | Current witnesses |
| --- | --- | --- |
| Arrow C Data ownership | `handoffs` | Full-schema and cached-schema export/import; nullable sliced string and integer batches |
| Shared bridge transforms | `handoffs` | Timestamp unit conversion, float canonical ordering, partition splits |
| Calc evaluation and compilation | `calc_expressions` | Arithmetic, booleans, CASE, casts, hashes, regex, date formatting/extraction, string and floating builtins; warm execution and first-batch compilation |
| Fixed binary expressions | `binary_expressions` | Production Calc casts and ELT, direct UDF scalar/array adaptation and all-scalar contracts; sliced inputs, NULLs, invalid indices, fixed/variable literals/results, widths 1/16/256, one/three scalar value arguments |
| Calc and column movement | `data_movement` | Compiled projection, grouping-set EXPAND, inner/left array UNNEST, Arrow IPC encode/decode |
| Stateful processing | `operator_allocations` | Filter, local/global SUM, tumble/session aggregate, running/bounded OVER, append/retract Top-N, first/last dedup, normalize, updating/interval/window joins, Paimon upsert merge |
| Further stateful processing | `data_movement`, `keys_and_checkpoints` | First-N, event-time sort, temporal join, window rank |
| Key materialization | `keys_and_checkpoints` | Arrow-row encode/decode, Flink BinaryRow hash; primitive and wide nullable string composite keys |
| Memory checkpoints | `keys_and_checkpoints`, `data_movement` | Group aggregate and append Top-N snapshot/restore, temporal-join snapshot |
| Persistent state | `persistent_state` | Production RocksDB temporal sort, keep-first deduplication, interval join, window rank, temporal join and rowtime running-SUM OVER; ingestion/update, probe/eviction/firing, checkpoint and aligned/rebuild recovery; nullable sliced wide rows, ordering and continued-arrival assertions; fixed options in `engine/benches/fixtures/rocks-options.json` |
| Persistent window aggregation | `persistent_state` | Tumbling and session Int64 SUM; partial firing, checkpoint and aligned/rebuilt single-source recovery; session bridge merging, independent event-list oracle, saved watermark and continued processing |
| Persistent window join | `persistent_state` | INNER join with unique UTF-8 keys; right arrival, partial firing, checkpoint and aligned/rebuilt single-source recovery; independent multiset/schema oracle, pre-replay arrivals, post-replay late rejection and continued processing |
| Persistent group aggregation | `persistent_state`, `persistent_group` | Mini-batch COUNT(*), nullable Int64 SUM and Int64 MIN/MAX with UTF-8 keys; ingestion/flush, checkpoint and aligned/rebuilt single-source restore; independent continued-changelog oracle and per-key row-kind order checks; half/all-row retractions with deletion and reinsertion oracles |
| Matched memory window ranking | `persistent_state` | Ingestion, populated-state updates and firing over the same sliced, nullable, wide-payload shapes as the disk-backed ranker; independent row-index output oracle |
| Collection expression kernels | `collection_expressions` | ARRAY_DISTINCT over Boolean, integer and string arrays; sliced/null input, short/large lists, unique/repeated values and wide UTF-8; expected outputs asserted |
| Exact scalar JNI upcalls | `jvm_truncate` | Production Rust Calc/C Data export, Java reflection/generated row evaluation and Arrow result import; bounded DOUBLE TRUNCATE shortcut versus released Flink; nullable sliced input and fallback profiles |
| Registered Flink scalar functions | `scalar_registry` | Every registered numeric opcode; completeness assertion requires a fixture for new registrations; ASCII/null/Unicode profiles |
| Parameterized scalar kernels | `scalar_registry` | Decimal cast/round/truncate/arithmetic/float conversion, integer parse/format/divide, FROM_UNIXTIME, array item, literal/dynamic map lookup, random, clock, float comparison |
| JSON decode | `json_decode`, `json_codecs` | Direct production decode, projection, wide messages, historical nested Nexmark corpus |
| Raw decode | `raw_decode` | All admitted primitive/string/binary types, endianness, null bodies, slices |
| CSV decode | `csv_decode` | Quoted wide strings, nullable schema, strict/ignore-errors configuration |
| Avro decode | `avro_decode` | Bare and Confluent framing, wide strings |
| Protobuf decode | `protobuf_decode` | Direct descriptor-based decode, primitive/string fields, wide strings |
| Sink format encoding | `format_encode`, `kafka_sink` | Production JSON, CSV, raw, Avro, Confluent Avro, Protobuf; historical JSON/timestamp comparisons |
| Parquet file operations | `parquet_io` | Production encode, selected-row encode, ordinary decode, paired INT96 decode, nullable wide strings and full-range timestamps |
| ORC post-decode normalization | `normalization` | CHAR trimming, string pass-through, full-range timestamp conversion |
| Historical operator experiments | `operators`, `calc_selection`, `scalar_functions` | Typed distinct, mini-batch sizes, aggregate layouts, selection strategies, DATE_FORMAT and string comparisons |

Persistent tumbling/session SUM and unique-key INNER window-join lifecycle
fixtures are now validated. Window/session aggregation with other shapes or
aggregate kinds, updating joins and other window-join kinds, duplicate keys,
residual predicates and TTL still need dedicated persistent witnesses. GROUP BY fixtures cover COUNT(*),
SUM and Int64 MIN/MAX; other aggregate kinds, extrema value types, DISTINCT views and TTL still
need dedicated persistent benchmarks. The operation inventory is not a claim of
exhaustive type, option or state-backend coverage.

This is operation-level coverage, not exhaustive coverage of every SQL type, expression opcode,
codec option, state backend, and recovery mode. Expand profiles alongside implementation changes.
Disk recovery fixtures prepare the source checkpoint and an empty destination directory outside
measurement. Aligned recovery adopts checkpoint files; clipped recovery rebuilds the verified
singleton key group through the production store. Timed recovery includes copy/rebuild, database
opening and closing, while directory cleanup and output verification stay outside it. Allocation
probes cover construction on the current Rust thread, not RocksDB's C++/worker allocations. Each
fixture appends after recovery and checks the complete ordered batch, including NULLs, to validate
the restored arrival-sequence counter. Both recovery modes use one source checkpoint; these do not
measure multi-source rescaling or compaction. All 48 persistent fixtures pass release smoke checks.
The largest nullable wide-row fixture requests 3,711 Rust bytes across 68 allocation calls for
aligned recovery, compared with 4,707,561 bytes across 32,808 calls for key-group rebuild.
These are different recovery modes, not before/after timings or proof of an avoidable copy.
[All 48 regenerated persistent allocation probes](native-followups-probes-2026-10-01.csv) are retained with the expression probes; disk/native-worker
costs remain outside these counters.

Focused release Criterion means for that 16,384-row fixture are 35.861 ms for clipped rebuild
and 11.711 ms for aligned adoption, including opening and closing the restored database.
The same source checkpoint is reused with warm filesystem caches; three seconds of warmup,
100 samples and at least five seconds of measurement run sequentially, clipped first. Rust 1.94.0,
Arrow 58.3.0, Linux/Core i7-12650H. The fixed store options are linked in the inventory above.
[Mean confidence intervals](recovered-historical-diagnostics-2026-10-01.txt) retain variability.
These characterize existing paths and do not measure a new optimization or whole-job speedup.
Reproduce with `--bench persistent_state --filter 'temporal_sort_restore/16384/bytes=264/nulls=true'`.

The main remaining boundaries are:

- JVM upcalls and host-owned reader callbacks: `jvm_truncate` exercises reflective scalar
  upcalls and generated row evaluation, while the scalar registry retains SQL/JSON recycler calls.
  The `parquet_host_reader` suite exercises synchronous memory and local-file
  reader callbacks. Arbitrary UDF types and remote reader callbacks still need
  dedicated workloads. Existing release
  integration harnesses remain the whole-job timing authority for those boundaries.
- Persistent stores beyond temporal sort, keep-first deduplication, interval join, window rank,
  temporal join, rowtime running-SUM OVER, COUNT(*)/SUM and COUNT(*)/MIN/MAX
  GROUP BY (including half/full retractions), tumbling SUM and session SUM
  or unique-key INNER window joins
  need dedicated workloads. Partial-firing recovery is covered for the running-SUM
  OVER and tumbling SUM fixtures described below; other aggregate/window shapes,
  multi-source RocksDB rescale/compaction, TTL migration/compaction and unlisted
  checkpoint variants remain unmeasured. Memory checkpoint probes cannot stand in
  for disk I/O and native worker allocations.
- ORC file reading remains JVM-backed; normalization is not an ORC decoder throughput benchmark.
  Parquet INT96 fixtures cover paired decoding from an in-memory file. The
  `parquet_host_reader` suite covers memory and local-file callback decoding of
  BIGINT and nullable STRING columns; remote callbacks and nested timestamp
  profiles remain unmeasured.
- Codec option/type combinations, CDC envelopes, nested encoder profiles, temporal arithmetic,
  decimal division, and parameterized scalar variants beyond the listed witnesses need additional cases. Registry completeness covers the factory, not all Calc opcodes.

`native-build`, `raw` (the deployment shim over shared raw decoding), and `integration-tests` do not
introduce an independent data-plane hot loop. Their operational implementation is measured in its
owning crate; build tooling and correctness tests are not timed as operators.

The environment reset removed temporary benchmark artifacts. Historical timing links above point to surviving task-log excerpts; complete timing samples must be regenerated. Fresh allocation probes are retained below.

A fresh release smoke run on 2026-10-01 passes all 276 fixtures in `binary_expressions`,
`collection_expressions`, `jvm_truncate`, and `persistent_state`.
[Regenerated allocation and Arrow-buffer probes](native-followups-probes-2026-10-01.csv)
cover those fixtures; smoke mode produces no timing evidence.

The remaining 20 discovered suites also pass release smoke checks on the integrated revision.
Together, the 24 suites produce 2,574 allocation-probe records; historical timing-only suites
also pass their fixtures but do not emit allocation records.
[Remaining operation probes](native-operation-probes-2026-10-01.csv) retain codec, handoff,
Calc, stateful-operator, key/checkpoint and file-operation diagnostics. These are production
boundary witnesses, not exhaustive coverage of all types, options, recovery modes or callbacks.

The dyadic-shortcut revision adds a non-dyadic `decimal_ambiguous` JNI profile, increasing
that suite from 72 to 96 fixtures. Historical counts and probes above describe the earlier
revision; all 96 profiles pass the released-Flink bit/schema/NULL comparisons.
[Expanded JNI probes](double-truncate-expanded-probes-2026-10-01.csv) are retained. Timing
comparisons for the revised helper remain pending.


## Persistent keep-first deduplication probes

`persistent_state` also exercises the production disk-backed keep-first deduplicator. The
48 profiles cover 16, 1,024 and 16,384 input rows, eight-key and unique-key cardinalities,
short and wide UTF-8 payloads (including embedded NUL), and nullable payloads. Pending
candidate insertion and watermark firing are measured together and checked against the
first input row for each key. A second workload prepares fired markers outside measurement,
then measures a later batch against those markers and verifies that no row is emitted.
Its timestamps exceed the previous finite watermark, so it exercises persisted-marker
reads rather than the late-data rejection shortcut.

Database creation and directory cleanup stay outside these timings. Production Arrow-row
encoding, database reads/writes and output reconstruction remain inside. Each Criterion
iteration starts from fresh state; the marker workload verifies its prepared output before
measurement. Allocation counters include only the calling Rust thread, excluding RocksDB
C++ and background-worker allocations. These workloads do not yet cover dedup checkpoint
recovery, TTL expiry or rescaling, and do not establish a whole-job speedup.

Run `python3 bin/bench-native.py --bench persistent_state --filter keep_first --smoke`
to validate these fixtures; omit `--smoke` for retained release timings.

All 48 new deduplication profiles pass release smoke checks on 2026-10-01; the existing
48 sort profiles pass in the same run. [Retained deduplication probes](persistent-dedup-probes-2026-10-01.csv)
show 32,916 allocation requests totaling 2,128,064 Rust bytes for 16,384 rows with eight
keys and short non-null payloads during insertion/firing. Reading emitted markers in that
profile requests 2,124,664 bytes across 32,846 calls despite producing an empty output.
These counters identify input-side encoding/allocation work for further investigation;
they do not establish how much can be removed or quantify database-worker costs. Timing
claims await controlled sequential release measurements.

The current arrival path encodes one owned key buffer per input row, then clones
those owned keys into a sorted/deduplicated probe list. Because the key wrapper
owns a boxed byte slice, this clone copies key payloads, not just references.
Borrowing the distinct probe keys is a concrete follow-up candidate; it needs
matching lookup interfaces and before/after measurements across duplicate and
unique keys before being called an improvement. The allocation totals above
include additional encoding/filtering work and cannot be attributed entirely
to this clone.


## Parquet paired INT96 profiles

The Parquet suite adds 24 INT96 profiles: 16, 1,024 and 16,384 rows, nullable and
non-null timestamp values, ordinary and selected-row encoding, and decoding with
17-row and 1,024-row batches. Timestamp fixtures include year 1, year 9999,
negative and positive epoch neighbors, and sub-millisecond fractions through
999,999 nanoseconds. The short decode batches force partial tails and alignment
between the millisecond and fractional readers. Every encoded profile is decoded
and checked for exact timestamp components, NULLs, row order and the companion
integer column; both decode batch sizes check the entire concatenated output.

Fixtures use the production INT96 encoder and the paired readers plus component
merge used by production decoding. The adapter retains schema reconstruction and
owns an in-memory file copy inside measurement. This measures Rust encode/decode
and conversion work, including both readers; it does not measure Java-owned
filesystem callbacks, JNI export, or whole-job throughput. Existing plain-file
profiles retain their behavior. Retained timing claims require a quiet-host
release run; smoke checks provide only fixture and allocation evidence.

Run `python3 bin/bench-native.py --bench parquet_io --filter int96 --smoke` to
validate the profiles, then omit `--smoke` to retain timing samples.


Decoder inputs are canonical INT96 files prepared with the released low-level
Parquet writer outside measurement, including negative sub-millisecond epoch
neighbors. Production encoder round trips use zero fractional remainder for
negative non-day-aligned timestamps. This is an explicit Flink wire-format limit:
the UTC writer uses Java division/remainder toward zero and can write a negative
nanosecond-of-day; released Flink's reader rejects a negative remainder.
A release oracle call to `TimestampColumnReader.int96ToTimestamp(true, -999999,
2440588)` throws `IllegalArgumentException`, and the matching native merge rejects
that same value. The initial benchmark exposed this matching failure; it does not
justify accepting a value that Flink rejects. Canonical decoder fixtures keep the
full negative-fraction coverage, while encoder profiles measure readable Flink
output. Local-time conversion and failure-path throughput remain outside these
profiles.

All 24 new INT96 profiles and the 27 existing Parquet profiles pass release smoke
checks on 2026-10-01. [All 51 allocation probes](parquet-int96-probes-2026-10-01.csv)
are retained. At 16,384 nullable rows, paired decoding requests 5,305,611 Rust
bytes with 17-row output batches and 1,882,535 bytes with 1,024-row batches.
These profile differences include per-batch reader/output overhead and do not
measure a new optimization or whole-job improvement.


The DOUBLE TRUNCATE JNI suite adds `large_domain` (signed values near 1e12,
scales -3..3) and `rounded_subunit` (scale-18 rounding boundaries near 1e-6 and
zero, scale 6) to each of its reflective/generated and released/shortcut controls.
All existing sliced-input and NULL profiles remain. These add 48 fixtures,
bringing the suite to 144; large values exercise released fallback rather than
assuming the historical small-fraction `outside_domain` label is a true magnitude
exclusion. Exact DOUBLE-bit, NULL and schema checks stay outside timing.

All 144 expanded JNI profiles pass release smoke checks on 2026-10-01.
[All domain/rounding allocation probes](double-truncate-domain-probes-2026-10-01.csv)
are retained. The final smoke run uses the corrected production evaluator and current
benchmark controls with released Flink 2.2.1 dependencies. The generated-code
regression test separately verifies that the released control disables helper
substitution. Smoke mode provides no timing claims;
JVM/native-worker allocations remain outside the Rust counters.


The persistent keep-first deduplication suite also defines 72 TTL profiles:
three batch sizes, repeated/all-distinct keys, two payload sizes, nullable/non-null
payloads and three phases. Live emitted markers suppress duplicates just before
the retention boundary; expired markers admit the next candidate exactly at that
boundary. A half-retention read during setup verifies reads do not refresh expiry.
Pending candidates still emit after that wall-clock interval because Flink's timer
state is not TTL-expired. Fixtures use the production disk store, production TTL
configuration and explicit host-clock readings; construction/priming stays outside
measurement. These profiles extend operation coverage, not production behavior.
All 168 persistent-state profiles (96 existing plus 72 TTL) pass release smoke
checks on 2026-10-02. [All allocation probes](persistent-state-ttl-probes-2026-10-02.csv)
are retained. Smoke checks establish output/schema and boundary behavior, without
timing claims; Rust-thread allocation counters exclude RocksDB C++ and background
workers. Coverage of other persistent stores and restore/rescale operations remains
incomplete.


Keep-first recovery also defines 72 profiles for mixed emitted/pending state:
three batch sizes, repeated/all-distinct keys, two payload widths and nullability,
with checkpoint, aligned restore and key-group-filtered restore. Continuation
checks reject late data using the restored watermark, suppress emitted keys,
preserve pending candidates and admit a new key after the saved sequence.
Checkpoint measurement excludes initial population; restore measurement includes
opening/closing the restored store, but not continuation checks. Single-source,
all-key-group recovery does not establish multi-source rescale coverage.
All 72 new recovery profiles pass release smoke checks on 2026-10-02;
existing fixture allocation/correctness assertions also rerun successfully.
[Recovery allocation probes](persistent-dedup-recovery-probes-2026-10-02.csv)
retain every case. For 16,384 distinct keys with nullable 264-byte payloads,
aligned opening requests 4,245 Rust bytes versus 2,626,189 for key-group rebuilding.
These characterize different existing recovery paths, not an optimization speedup.
RocksDB C++/background allocations and filesystem I/O are outside Rust counters.
The suite now defines 240 profiles; this run filters timing smoke to the 72 new
recovery profiles. Cargo metadata confirms 24 workspace benchmark targets, without
establishing exhaustive operation coverage.


The 2026-10-02 Boolean membership comparison retains all 84 collection fixtures
for both original and final release builds. [All 168 allocation probes](array-distinct-boolean-bits-probes-2026-10-02.csv)
include integer/string controls; every output assertion passes. Width-64 Boolean
profiles eliminate one 48-byte membership allocation while gather buffers remain
unchanged. Timing filters select four 1,024-row Boolean controls only; the
[scalar-kernel ledger](../optimizations/scalar-function-kernels.md) records estimates
and intermediate experiments. JVM/C++ allocations are not included.

## Persistent interval-join probes

`persistent_state` adds 288 interval-state profiles: 48 shapes across six measured boundaries.
Shapes vary 16, 1,024 and 16,384 left rows, eight or batch-sized key cardinality, eight- or
264-character UTF-8 suffixes (with Unicode and embedded NUL), nullable/non-null payloads, and
INNER/LEFT OUTER joins. Inputs have nonzero Arrow slice offsets. Keys and rowtimes are non-null
INT64 columns. There is one right row per even key, so matches are bounded at half the left
row count; left-outer eviction emits the unmatched half. This avoids timing a quadratic output.

The separate phases measure:

- Left append into an empty persistent buffer, including production row/key encoding and writes.
- Right append and probing against the prepared left buffer, including match materialization and
  persistent outer-match flag updates.
- Watermark expiry over both prepared sides, including scans, deletion and outer null-padding.
- Checkpoint creation over both live sides, including stored row-sequence and timer metadata.
- Aligned single-source recovery, adopting files and opening/closing the restored database.
- Single-source rebuild recovery, reading and resequencing the one verified key group and
  opening/closing the destination database.

The source checkpoint, input arrays, prepopulated state and temporary directories are prepared
outside their phase's measurement. Normal operation phases exclude opening/closing the operator;
recovery includes those costs. Checkpoint and restore adapters retain path/configuration handling.
Throughput counts incoming rows for append/probe, and prepared-state rows for expiry, checkpoint
and recovery; probe output can exceed incoming right-side cardinality. Disk and RocksDB worker
costs remain in elapsed timing but outside the Rust-thread allocation counter. Arrow buffer
probes compare each operational output against both original input sides; checkpoint and restore
have no Arrow output, so their buffer-byte columns are inapplicable.

The memory-backed production joiner supplies complete match and eviction reference batches,
with independent cardinality assertions. Restore checks verify the timer deadline, append again
on both sides, require two matches for a repeated key, then append an unmatched left row and
compare complete watermark output. These expose overwritten rows, reused sequence IDs, lost
match flags and repeated outer emission. Both modes restore one source with `max_parallelism=1`;
these do not measure key-group exclusion, multi-source rescale, FULL/RIGHT OUTER joins, NULL keys,
processing-time joins or residual predicates.

Run `python3 bin/bench-native.py --bench persistent_state --filter 'engine/rocksdb/interval' --smoke`
to validate fixtures, then omit `--smoke` to collect timing samples. Smoke mode and allocation
traffic do not establish an optimization or whole-job speedup. Other persistent stores remain
uncovered as listed above.

All 288 new interval profiles and all 240 existing persistent-state profiles pass release smoke
checks on 2026-10-02. [All interval allocation probes](persistent-interval-probes-2026-10-02.csv)
are retained. The largest nullable, batch-cardinality left-outer fixture requests 9,678,187 Rust
bytes during append, 36,523,865 during probing and 21,107,256 during expiry. Its rebuild restore
requests 7,650,531 bytes across 73,779 calls, versus 4,404 bytes across 83 calls for aligned
adoption. These distinguish production boundaries and recovery modes; they do not identify
all requested bytes as avoidable copies or compare a new optimization against a baseline.

A focused sequential release run records the six large, nullable, batch-cardinality LEFT OUTER
profiles as baseline `interval-state-initial`: three seconds of warmup, 100 samples and at least
five seconds targeted measurement per phase, extended by Criterion when required for 100 samples.
The same source checkpoint is reused for recovery with warm filesystem caches. Rust 1.94.0,
Arrow 58.3.0, Linux x86-64/Core i7-12650H, system allocator with the benchmark instrumentation
and the fixed RocksDB options above. Mean durations and 95% confidence intervals:

| Phase | Mean milliseconds | 95% interval milliseconds |
| --- | ---: | ---: |
| append_left | 4.145 | 4.097–4.200 |
| checkpoint | 37.136 | 30.710–43.955 |
| probe_right | 27.973 | 23.915–32.159 |
| restore (rebuild) | 80.690 | 71.076–90.525 |
| restore (aligned) | 10.938 | 10.805–11.150 |
| watermark_expire | 14.130 | 14.024–14.242 |

[All mean estimates](persistent-interval-timing-2026-10-02.csv) and
[all 600 samples](persistent-interval-samples-2026-10-02.csv) are retained. Probe, checkpoint and rebuild
intervals are wide, so these should be treated as initial shared-host characterizations rather
than precise comparative claims. They describe different operational boundaries, not a
before/after optimization or end-to-end Flink comparison. Reproduce with
`--bench persistent_state --filter '16384/domain=16384/bytes=264/nulls=true/outer=true'`.

## Binary ELT argument adaptation

`binary_expressions` adds 720 direct-production-UDF profiles alongside its 72 Calc fixtures.
The matrix varies 16, 1,024 and 16,384 rows, widths 1/16/256, nullable/non-null sliced fixed
input arrays, fixed-size or variable-binary scalar literals, NULL/non-null literals, and five
index modes: dynamic, constant first argument, constant literal, invalid zero and NULL.
Dynamic indices also include negative, zero and NULL values. Each case has a scalar-literal
path and a pre-expanded array control, with complete independent expected-value and result-type
checks. All 792 fixtures pass release smoke checks on 2026-10-02.

Argument cloning, production scalar adaptation, selection and output allocation/disposal are
inside timing. Field metadata and fixtures are built outside it. The controls deliberately
prepare the repeated literal arrays outside measurement; they isolate adaptation cost and are
not a shipped optimization, a Calc comparison, or a whole-job baseline. These adaptation
profiles always have a source array. The separate all-scalar profiles below cover scalar
return behavior. Mixed array/scalar variable-binary results and other variadic arities remain
uncovered. Scalar output materialization for validation is outside measurement; if a function
returns a scalar, Arrow output-buffer metrics are inapplicable.

[All 720 allocation probes](binary-elt-argument-probes-2026-10-02.csv) are retained. For a
1,024-row non-null source, width 16, non-null variable-binary literal and dynamic indices,
the scalar path requests 37,972 Rust bytes across 17 calls versus 17,224 across 11 for the
pre-expanded control. Both produce the same 16,512 new Arrow buffer bytes. The difference
includes scalar cloning and adaptation, rather than identifying every requested byte as a copy.
These are the pre-optimization allocation baselines. The current adapter preserves scalar
literal cardinality as described in the [kernel ledger](../optimizations/scalar-function-kernels.md#preserve-elt-literal-cardinality).

Run `python3 bin/bench-native.py --bench binary_expressions --filter elt_arguments --smoke`
to validate these profiles, then omit `--smoke` for timing. A focused comparison uses
`--filter 'elt_arguments/(scalar|expanded_control)/1024/width=16/nulls=false/fixed_literal=false/literal_null=false/index=dynamic'`.

A sequential release characterization of that 1,024-row case uses three seconds of warmup,
100 samples and at least five seconds of measurement per representation, Rust 1.94.0/Arrow
58.3.0 on Linux x86-64/Core i7-12650H, with the benchmark system allocator. Means and 95%
confidence intervals are:

| Representation | Mean microseconds | 95% interval microseconds |
| --- | ---: | ---: |
| Pre-expanded control | 4.9482 | 4.9336–4.9649 |
| Scalar literal | 5.4754 | 5.4561–5.4968 |

[Mean estimates](binary-elt-argument-timing-2026-10-02.csv) and
[all 200 samples](binary-elt-argument-samples-2026-10-02.csv) are retained. Scalar adaptation
adds roughly 0.54 microseconds per 1,024-row call in this diagnostic, about 11% of the control's
kernel boundary. This is small relative to the measured whole-job ELT gap and does not establish
that removing the allocation would resolve it. The control's excluded literal preparation
must not be treated as an end-to-end optimization or a stock-Flink comparison.

### All-scalar ELT contracts

`elt_all_scalar` adds 168 profiles across variable-binary and fixed-width 1/16 results,
one or three value arguments, variable or fixed-size binary literals, nullable/non-null
values, and indices -1/0/1/2/3/4/NULL. Variable-output fixtures select values of different
lengths, including an empty variable-binary value distinct from NULL. Every result is
checked independently for exact bytes, NULLs, declared type and
scalar representation, with `number_rows=1024` to ensure a larger enclosing batch does not
turn an all-scalar expression into an array.

All 960 binary fixtures pass the final release smoke run on 2026-10-02, including these
168 cases and the prior 792. [All-scalar allocation probes](binary-elt-all-scalar-probes-2026-10-02.csv)
retain every new case. Selecting the first of three non-null variable-binary literals
requests 7,019 Rust bytes in 31 allocation calls for the empty variable result, or 1,956
bytes in 31 calls for a fixed-width-16 result. These include temporary adapter/output
builders and argument clones; they are not measurements of live memory or copied bytes.

The production implementation uses the released DataFusion 54 scalar adapter: it evaluates
one row when every argument is scalar and extracts a scalar result. Argument cloning,
one-row adaptation, selection, result extraction and disposal are inside measurement;
fixture and field construction are outside. There are no input or output Arrow arrays at
this boundary, so the probe's Arrow buffer columns are inapplicable and recorded as zero.
Rust allocation requests still include temporary arrays used inside the adapter and must
not be interpreted as copied bytes. These profiles establish the contract before changing
literal expansion; they introduce no production optimization or Flink throughput claim.

Run `python3 bin/bench-native.py --bench binary_expressions --filter elt_all_scalar --smoke`
for these cases, or remove `--smoke` for timing. Mixed variable-result batches, additional
binary input types, selected-length errors and arities beyond one/three values remain
uncovered by this matrix.

Two sequential release baselines use the same Rust/Arrow, CPU and system allocator as the
adaptation measurements above, with three seconds of warmup, 100 samples and at least five
seconds of measurement each. Both select the first of three non-null variable-binary
literals. The empty variable result averages 0.9583 microseconds (95% interval
0.9545–0.9625); the fixed-width-16 result averages 1.1432 microseconds (1.1377–1.1493).
[Mean estimates](binary-elt-all-scalar-timing-2026-10-02.csv) and
[all 200 samples](binary-elt-all-scalar-samples-2026-10-02.csv) are retained. These are
baseline costs for different result shapes, not an optimization comparison. The filter is
`'elt_all_scalar/width=(0|16)/values=3/fixed_literal=false/nulls=false/index=Some\(1\)'`.

### Scalar-preserving adapter measurements

The current ELT adapter uses released DataFusion 54 `AcceptsSingular` hints for binary
values in mixed scalar/array calls. The index still expands to the batch length, while
binary literals expand to one row. Original argument representation distinguishes scalars
from real one-row arrays. All-scalar calls retain DataFusion's scalar extraction contract.
Calls without mixed scalar/array values allocate no hint vector.

[All 960 current probes](binary-elt-singular-probes-2026-10-02.csv),
[12 timing estimates](binary-elt-singular-timing-2026-10-02.csv) and
[all 1,200 samples](binary-elt-singular-samples-2026-10-02.csv) retain an initial
unconditional-hint candidate, a fresh published-code control (`f0aa2c43`), and the revised
mixed-only-hint candidate. Each variant times the same four cases with three seconds of
warmup, 100 samples and at least five seconds per case, using the same Rust/Arrow versions,
CPU and system allocator as the earlier adaptation baselines.

For the 1,024-row dynamic-index, non-null width-16 variable-literal case, requested Rust
bytes fall from 37,972 to 17,579 (18 calls rather than 17), with unchanged 16,512 new output
Arrow buffer bytes. The pre-expanded control retains 11 allocation calls and requests
17,240 bytes versus the former 17,224. Literal repetition is removed; an extra hint allocation
and larger input descriptors remain. These metrics do not count copied bytes or peak memory.

The fresh published-code means are 5.724 microseconds for the scalar literal and 5.535 for
the pre-expanded control; the revised candidate means are 4.992 and 4.883. Control means
across the three variants vary from 4.883 to 5.535 microseconds, so these sequential runs
do not establish a precise percentage speedup. Revised all-scalar means are 1.041 microseconds
for the empty variable result and 1.155 for the width-16 result, versus fresh controls
1.102 and 1.148; the fixed-result confidence intervals overlap. The certain result is
lower literal-adaptation allocation, with independent value/type/representation checks.
Whole-job evidence and its remaining performance gate belong in the kernel ledger.

### Configurable fixed-BINARY whole-job diagnostics

The separate scalar diagnostic accepts `-Dscalar.binary.width=N` (positive, default 16)
for `ELT_FIXED_BINARY` and `ELT_FIXED_BINARY_GROUPED_COUNT`. Source arrays, literals,
resolved result/sink types and the source-matched identity control all use that same width.
Bytes cycle through zero and high values; the default reproduces the original 16-byte
fixture and literal. Its CSV appends `binary_width` (zero for other source families),
since `scalar.bytes` controls text payloads rather than this fixed-binary source.
Use `SF_BENCHMARK=true mvn test -pl streamfusion-runtime -am -Pbench
'-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dscalar.functions=ELT_FIXED_BINARY
-Dscalar.binary.width=256 -Dscalar.rows=2000000` with the same heap and worker resources
for stock, current native and previous production. This extends the scalar diagnostic;
the Nexmark schemas, queries and harness are unchanged.

## Persistent window rank and deduplication

The same module also defines 216 `engine/memory/window_rank` profiles for ingestion,
updates to populated state, and firing, using the identical 72 input shapes and independent
row-index oracle as the persistent fixtures. This isolates ranking and scalar materialization
from RocksDB write, flush, and filesystem costs when a persistent comparison varies widely.
Operator creation, population, destruction, and complete output comparison are outside timing;
the measured operation includes production ranking or Arrow result construction. Allocation
CSV labels begin with `memory_rank/` to distinguish these probes from the disk-backed cases.
Run a focused comparison with `--bench persistent_state --filter 'engine/memory/window_rank'`.
Both memory and persistent fixtures remain Rust kernel diagnostics; neither measures JNI or
the two row transposes in a complete Flink job.

The original implementation passes all 1,176 release fixtures, including all 216 new memory
profiles. [Memory allocation probes](memory-window-rank-probes-2026-10-02.csv),
[four baseline estimates and confidence intervals](memory-window-rank-timing-2026-10-02.csv),
and [all 400 raw samples](memory-window-rank-samples-2026-10-02.csv) are retained. For 16,384
non-null rows with a 264-character suffix and rank limit one preserving first arrivals, means
are 2.351 ms for ingestion and 2.320 ms for updates with eight keys; with one key per input row,
they are 4.026 ms and 3.049 ms. These characterize the original ranker, not an optimization.
Baseline `window-rank-memory-original` uses three seconds of warmup, 100 flat samples and
at least five seconds of measurement, Rust 1.94.0, Arrow 58.3.0, Linux/Core i7-12650H and the
benchmark-only counting system allocator. Reproduce with `--bench persistent_state --filter
'engine/memory/window_rank/(append|update)/16384/domain=(8|16384)/bytes=264/nulls=false/limit=1/last=false'`.

The opt-in `WindowRankBenchmark` provides a separate row-fed whole-job comparison:
`SF_BENCHMARK=true mvn test -Pbench -Dtest=WindowRankBenchmark`. It uses two event-time
tumble windows, a sequence source emitting Flink rows, and a blackhole sink retaining the
rank, sort value, and payload. Plan checks require native window ranking and both transposes.
Before timing, a separate stock/native collection compares the complete output multiset,
including row kinds, payloads, NULLs, and rank values, for the selected workload.
Use `-Drank.verifyOnly=true` to run only the plan and result checks.
Rows remain pending until the source's terminal watermark; periodic watermarks are disabled.
The default workload has 2,000,000 rows, eight keys, a 264-character payload suffix, rank
limit one, two warmups and five measured trials per engine, with alternating engine order.
Override `rank.rows`, `rank.keys`, `rank.width`, `rank.limit`, `rank.nullable`, `rank.warmup`
and `rank.runs` to cover smaller/wider payloads, unique keys, NULLs, and larger rank limits.
Output includes every measured trial and medians. Use the identical fixture and released
native libraries in the previous-production checkout as a third baseline. This harness is
not a persistent recovery workload, and no whole-job ranking improvement is claimed here.

The `window_rank` module under `persistent_state` defines 432 profiles: 16/1,024/16,384
incoming rows, eight or batch-cardinality keys, UTF-8 payload suffixes of 8/264 bytes,
non-null or nullable sort/payload columns, and three rank modes. The modes keep one row
with first-arrival ties, keep one with last-arrival ties, or retain four ranked rows.
Inputs have a nonzero Arrow slice offset, two window ends (100/200), five repeated sort
values, Unicode/NUL text and independently placed sort/payload NULLs. Each mode projects
the rank number. NULL sort values follow non-null values.

Six phases separate empty-store ingestion, an additional bundle against populated state,
watermark firing, native checkpoint creation, aligned adoption and single-source rebuild.
The update bundle repeats the original input, exercising hydration and persistence of
existing ranked buffers. Firing includes output materialization and closed-group deletion.
Checkpoint preparation fires only the first window; the saved store retains the second
window and the late-data watermark. Recovery verifies the timer deadline, replays both
windows, requires half the replayed rows to be dropped as late, compares the surviving
ranked output and requires repeated firing to be empty.

An independent oracle groups row indices by window/key, sorts by NULL placement, value
and arrival order, takes the configured prefix, and builds the expected rows/ranks with
Arrow `take`. The memory-backed production ranker is checked against that oracle too.
Comparison canonicalizes ordering across independent key groups outside measurements;
rank positions and selected payloads remain part of the complete equality assertion.

Input construction, oracle work, temporary-directory creation and prepopulation are outside
each timed phase. Normal ingestion/firing excludes opening and closing the store; recovery
timing includes both. The one-shot recovery allocation probe measures construction, with
destruction outside its counter. Checkpoint and recovery produce no Arrow batch, so their
output-buffer columns are inapplicable. Throughput reports original input rows, including
for firing/checkpoint/recovery; it is not the number of emitted ranks or retained records.
RocksDB C++/worker allocations remain outside the Rust-thread counters, while their elapsed
cost is retained in timing. These profiles use one source and `max_parallelism=1`; they do
not cover key-group clipping, multi-source rescale, canonical savepoint migration, mixed
schemas, other sort types/directions, or checkpoint compaction.

Run `python3 bin/bench-native.py --bench persistent_state --filter window_rank --smoke`
for fixture validation; omit `--smoke` for Criterion timing. These are benchmarks of the
existing production store, not a runtime optimization or a whole-job speedup claim.

All 432 window-rank profiles and all 528 existing persistent-state profiles pass release
smoke checks on 2026-10-02. [Window-rank allocation probes](persistent-window-rank-probes-2026-10-02.csv)
retain every new case. For non-null, wide 16,384-row keep-first input, eight keys request
12,154,000 Rust bytes on initial ingestion and 12,168,570 on update. Batch-cardinality keys
request 68,350,972 and 81,251,466 bytes respectively. These are total requested allocation
traffic, not copied bytes or peak memory; they include key/row materialization and state
handling. The repeated-key case motivates investigating payload materialization before
rank rejection, but these measurements alone do not prove an optimization or its safety.

Baseline `window-rank-ingestion-initial` records four sequential release profiles on
Rust 1.94.0/Arrow 58.3.0, Linux WSL2/Core i7-12650H, with the counting system allocator
and the fixed RocksDB options above. Each has three seconds of warmup, 100 samples and
at least five seconds targeted measurement, extended by Criterion where required.
The 16,384-row inputs use non-null wide payloads, keep-first ties and rank limit one:

| Keys | Phase | Mean milliseconds | 95% confidence interval |
| ---: | --- | ---: | --- |
| 8 | Initial ingestion | 3.137 | 3.120–3.155 |
| 8 | Update | 3.132 | 3.108–3.158 |
| 16,384 | Initial ingestion | 39.344 | 38.970–39.751 |
| 16,384 | Update | 136.997 | 121.226–153.306 |

[All four estimates](persistent-window-rank-timing-2026-10-02.csv) and
[all 400 raw samples](persistent-window-rank-samples-2026-10-02.csv) retain the broad
update interval. These are initial baselines of the unchanged store, not before/after
results. Key cardinality changes retained state and I/O as well as allocation, so differences
between these rows cannot be attributed solely to discarded payload copies.

### Rejected window-rank payload admission prototype

A single-map-entry prototype checked ranking admission before full-row extraction.
Twenty matched memory-operator cases on 16,384 rows improved repeated-key ingestion
and updates by 35.79–51.75%, but unique-key ingestion regressed 3.01–13.44%.
Unique-key keep-last and limit-four updates regressed 19.45% and 24.06%. The original
production loop is retained; the allocation opportunity remains an investigation.

The comparison includes nullable inputs, narrow/wide Unicode payloads, limits one/four
and first/last ties (last only at limit one). Candidate cases ran before a fresh original
control, sequentially with three-second warmup, 100 samples and at least five seconds
targeted measurement. Rust 1.94.0/Arrow 58.3.0, Linux WSL2/Core i7-12650H, counting
System allocator. Operator setup/population and teardown are outside timing. These
results exclude JNI, transposes, C++ allocations and whole-job costs. All 1,176
allocation fixtures passed for both implementations.

[All 20 comparisons](window-rank-lazy-payload-rejected-comparison-2026-10-02.csv),
[40 estimates and confidence intervals](window-rank-lazy-payload-rejected-timing-2026-10-02.csv)
and [4,000 raw samples](window-rank-lazy-payload-rejected-samples-2026-10-02.csv) preserve
unfavorable controls. The rejected approach and recovery constraints are recorded in
`.claude/wontdos/window-rank-lazy-payload.md`. No whole-job acceleration is claimed.

### Persistent temporal-join lifecycle

The `persistent_state` suite includes 144 temporal-join profiles: 16/1,024/16,384
probe rows, eight or batch-cardinality keys, UTF8 suffix widths 8/264, nullable
payloads, and six phases (build, probe, watermark firing, checkpoint, aligned
restore, clipped restore). Inputs use nonzero Arrow slice offsets. Even keys have
two build versions at times 100/200; odd keys are unmatched. Probes at 150/250
exercise latest-version selection and LEFT outer output. An independent row-index
oracle checks complete values, NULLs, Unicode/embedded-NUL payloads and INSERT row
kinds, canonicalizing only output order. A second firing must be empty.

The thin adapter calls the existing persistent operator and store APIs with the
shared RocksDB options and one key group. Creation, prepopulation, filesystem
fixture construction and operator destruction are outside the counted/timed
boundary. Fire includes output Arrow construction. Checkpoint includes creating
its disk snapshot; restore includes opening/adopting or rebuilding the database,
with output validation outside measurement. The rebuild path filters the full
single-key-group range, so no keys are excluded. These single-source restore
controls are not partial or multi-source rescaling, TTL,
retraction, arbitrary type or compaction coverage.

Run `python3 bin/bench-native.py --bench persistent_state --smoke` for fixture
validation, or filter `engine/rocksdb/temporal_join` without `--smoke` for release
Criterion timing. Allocation counters exclude C++, filesystem/native workers and
JVM costs. This matrix measures existing behavior; it does not claim a new runtime
optimization or a whole-job speedup. All 144 profiles and all 1,176 earlier
profiles pass release fixture validation on 2026-10-02. [All 144 allocation probes](persistent-temporal-join-probes-2026-10-02.csv)
retain every phase and shape. Smoke validation does not produce timing evidence.
The separate release baseline below retains [five estimates and confidence intervals](persistent-temporal-join-timing-2026-10-02.csv)
and [all 500 raw samples](persistent-temporal-join-samples-2026-10-02.csv).

For the focused 16,384-row, unique-key, wide nullable lifecycle baseline, use:

```sh
python3 bin/bench-native.py --bench persistent_state \
  --filter 'engine/rocksdb/temporal_join/temporal_join/(build|probe|fire|restore_aligned|restore_clipped)/16384/domain=16384/bytes=264/nulls=true' \
  --save-baseline temporal-join-lifecycle-original
```

Criterion uses its default 3-second warmup, 5-second measurement and 100 samples.
Each iteration starts with a fresh fixture outside the timed boundary. The
allocation/oracle diagnostics execute for the full suite before the selected
timings; a filter does not skip those checks. Build throughput counts right-side
rows; the other phases count left-side probe rows. The saved baseline measures
these lifecycle boundaries alone, with the benchmark's counting system allocator.
Production mimalloc, JNI, row conversions and whole-job costs require separate
measurements before accepting any optimization.

For the 16,384-row unique-key wide nullable firing fixture, Rust requests
89,643,808 bytes across 382,390 allocation calls while constructing 6,437,741
bytes of new output buffers. Requested bytes sum allocation/reallocation requests;
they are not peak live memory. This gives a baseline for investigating the
store decode, owned scalar materialization and output reconstruction path, not
evidence that all of those requests can be removed.

| Phase | Mean (ms) | 95% confidence interval (ms) |
| --- | ---: | ---: |
| build | 5.535 | 5.408–5.697 |
| fire | 46.381 | 46.083–46.693 |
| probe | 11.695 | 10.724–12.902 |
| restore_aligned | 27.902 | 27.180–28.741 |
| restore_clipped | 38.430 | 38.024–38.869 |


### Persistent running-SUM OVER lifecycle

New `persistent_state` fixtures exercise the existing rowtime unbounded BIGINT
SUM OVER path through its RocksDB store: 16/1,024/16,384 rows, eight or
batch-cardinality partition keys, UTF8 payload suffix widths 8/264 and nullable
values/payloads. All inputs have a nonzero Arrow slice offset. An independent
per-key running-sum oracle checks all five output columns, including NULL before
the first non-null value, raw Unicode/embedded-NUL payloads and preserved rowtimes.
Output order alone is canonicalized; repeated firing must produce no rows.

The 120 profiles separate push, firing, pending-row checkpoint and aligned/rebuilt
restore. The checkpoint source contains pending input rows before any firing.
Creation, prepopulation, fixture destruction and correctness checks are outside
the measurement boundary; firing includes output construction. Push includes the
adapter's shallow `RecordBatch` clone: Arrow buffers remain shared, while metadata
clone costs are counted. The production method consumes its input batch, so this
adapter cost must not be reported as an engine payload copy. Restore adopts
or rebuilds a single source over its full one-key-group range, excluding no keys.
These fixtures do not yet cover checkpointed running folds after firing, continued
arrivals, bounded ROWS/RANGE frames, processing time, DISTINCT, TTL or multi-source
rescaling. Rust allocation counters exclude RocksDB C++ and worker allocations.

All 120 profiles and the 1,320 earlier profiles pass release fixture validation
on 2026-10-02. [All 120 allocation profiles](persistent-over-probes-2026-10-02.csv)
retain every phase and shape. The separate release baseline retains
[five estimates and confidence intervals](persistent-over-timing-2026-10-02.csv)
and [all 500 raw samples](persistent-over-samples-2026-10-02.csv). Smoke validation
does not produce timing evidence. No runtime optimization or whole-job improvement
is claimed by this matrix.

For the 16,384-row unique-key wide nullable OVER fixture, firing requests
30,868,044 Rust allocation bytes across 164,050 calls and creates 4,434,503
bytes of output buffers. Aligned restore requests 5,335 Rust bytes across 91
calls, versus 5,020,657 bytes across 32,832 calls for rebuilding. These are
allocation-request sums, not peak memory or complete database I/O costs; the
C++ and worker exclusions matter particularly for database adoption.

Reproduce the release lifecycle baseline for 16,384 unique-key wide nullable
rows with:

```sh
python3 bin/bench-native.py --bench persistent_state \
  --filter 'engine/rocksdb/over/over/(push|fire|checkpoint|restore_aligned|restore_rebuilt)/16384/domain=16384/bytes=264/nulls=true' \
  --save-baseline over-lifecycle-original
```

The five phases use Criterion's default 3-second warmup, 5-second measurement
target and 100 samples, with a fresh per-iteration fixture outside timing. The
filter selects timed cases but still executes all suite allocation/oracle
diagnostics beforehand. These System/counting-allocator lifecycle timings do
not include JNI, row conversions or the production mimalloc configuration and
are not a substitute for the required whole-job performance comparisons.

| Phase | Mean (ms) | 95% confidence interval (ms) |
| --- | ---: | ---: |
| checkpoint | 120.722 | 98.461–143.604 |
| fire | 26.450 | 26.198–26.701 |
| push | 4.125 | 4.084–4.167 |
| restore_aligned | 66.246 | 63.493–69.273 |
| restore_rebuilt | 27.875 | 24.110–32.377 |

Aligned restore has fewer Rust allocation requests but greater elapsed time
than rebuilding in this fixture. The timed boundary includes native database
opening/adoption work that the Rust allocation counter excludes. Checkpoint
and rebuild timing intervals are broad; retain their raw variability rather
than interpreting this baseline as a precise general ranking of recovery paths.


### Scale-dependent DOUBLE TRUNCATE validation

The expanded decimal-coefficient guard passes all 144 existing release JNI
fixtures: four evaluators, six value profiles, three sizes and two NULL shapes.
Each compares exact result bits, NULL positions and schema with released Flink.
The embedded JVM loads freshly compiled Flink 2.2 candidate classes first;
bytecode and source/class hashes verify the scale-dependent bound. Independent
helper oracle and generated/SQL checks also pass on released Flink 1.18.
[Allocation probes](double-truncate-scale-bound-jni-allocations-2026-10-02.csv)
retain the complete matrix. These count requested Rust allocations with the
benchmark allocator, excluding JVM/native worker allocations; no JVM allocation
saving or Criterion throughput improvement is inferred from this smoke run.
Release/mimalloc whole-job performance and all 210 repeated measurements live
in the [kernel ledger](../optimizations/scalar-function-kernels.md).


### Persistent OVER recovery after partial firing

The additional fixture checkpoints a running-SUM fold after advancing a watermark
through half the initial rows while retaining the other half as pending state.
It verifies aligned and single-source rebuilt restores against an independent
prefix-sum oracle, then submits an already-fired row and a second wave of new
rows. Complete output values and schema must match the oracle, and refiring must
be empty. The 24 existing shapes produce 72 additional allocation profiles for
checkpoint-after-fire and both restore paths. All 72 pass, along with the
1,440 earlier cases: 1,512 unique release fixtures in total.
[All 72 allocation probes](persistent-over-fold-recovery-allocations-2026-10-02.csv)
are retained. These do not establish multi-source rescale, bounded OVER,
equal-time arrival ordering or retention coverage.

For 16,384 unique-key wide nullable initial rows, checkpoint after partial
firing requests 511 Rust bytes in 22 calls; aligned restore requests 5,335
bytes in 91 calls and rebuilt restore 2,766,601 bytes in 32,832 calls.
Continuation firing and oracle comparisons happen after the measured call.
RocksDB C++ allocations, worker threads and filesystem buffers remain outside
the Rust request counter, so these are neither total memory nor copied-byte
measurements. The three lifecycle baselines retain 100 samples each, with a
three-second warmup and five-second target measurement (Criterion extended the
checkpoint collection to approximately 50.5 seconds).

| Lifecycle | Mean | 95% confidence interval |
| --- | ---: | ---: |
| Checkpoint after partial firing | 57.177 ms | 34.865–82.292 ms |
| Aligned restore | 10.575 ms | 10.390–10.804 ms |
| Rebuilt restore | 22.074 ms | 21.615–22.637 ms |

The checkpoint measurements have substantial variability, including 17 outliers
among 100 samples; they do not establish a stable checkpoint throughput claim.
[All timing estimates](persistent-over-fold-recovery-timing-2026-10-02.csv) and
[all 300 samples](persistent-over-fold-recovery-samples-2026-10-02.csv) are retained.
Criterion filtering still runs the full untimed diagnostics before selected
timings. These use the Rust counting allocator rather than production mimalloc;
no whole-job improvement is claimed.


### Persistent GROUP BY lifecycle fixtures

The new benchmark matrix uses the production RocksDB-backed mini-batch
COUNT(*) and nullable Int64 SUM implementation. Its 24 shapes combine 16,
1,024 and 16,384 rows, repeated or unique UTF-8 keys, short or wide keys,
and nullable or nonnullable input. Keys include Unicode and NUL bytes;
input arrays have nonzero slice offsets. Independent key and value null
masks include groups with entirely null SUM inputs.

Four phases measure update plus logical-bundle flush, checkpoint after flush,
aligned restore and single-source rebuilt restore. An independent map oracle
checks complete schema and INSERT/UPDATE_BEFORE/UPDATE_AFTER values, including
a second input wave after each measured call and reopening the actual measured
checkpoint. UPDATE_BEFORE must precede UPDATE_AFTER for each key; unordered
group emission is canonicalized only after this order check. Setup, continuation
and oracle checks remain outside measurement. All 96 new profiles
pass, along with 1,512 earlier profiles: 1,608 unique release fixtures. The final
run verifies unchanged source hashes and the per-key changelog order. These
fixtures do not establish retract/delete, DISTINCT, TTL or multi-source rescale coverage. Existing-group continuation is
validated outside measurement; the ingestion timing starts from empty state.


[All 96 GROUP BY allocation profiles](persistent-group-aggregate-allocations-2026-10-02.csv)
are retained. For 16,384 rows with a 16,384-key domain, wide UTF-8 keys and
nullable input, ingestion plus flush requests 80,232,710 Rust bytes in 182,018
calls and produces 4,387,381 bytes of new output buffers. Null keys coalesce into
one group, so the nullable shape has fewer groups than its configured domain.
Checkpoint requests 511 bytes in 22 calls; aligned restore requests 5,990 bytes
in 94 calls; rebuilt restore requests 5,136,455 bytes in 29,855 calls. These
counters exclude RocksDB C++ allocations, worker threads and filesystem buffers;
they measure requested Rust allocations rather than copied bytes or peak memory.
Four lifecycle baselines retain 100 samples each, with a three-second warmup
and five-second target measurement. Criterion extended checkpoint sample
collection to approximately 14 seconds.

| Lifecycle | Mean | 95% confidence interval |
| --- | ---: | ---: |
| Ingestion and logical-bundle flush | 45.974 ms | 45.684–46.267 ms |
| Checkpoint | 40.502 ms | 33.368–48.495 ms |
| Aligned restore | 9.882 ms | 9.742–10.087 ms |
| Rebuilt restore | 28.361 ms | 26.588–30.164 ms |

[All four timing estimates](persistent-group-aggregate-timing-2026-10-02.csv) and
[all 400 samples](persistent-group-aggregate-samples-2026-10-02.csv) are retained.
Checkpoint variability remains substantial. The Rust counting allocator uses
System rather than production mimalloc; all 1,608 untimed diagnostics execute
before the filtered timing cases. No whole-job acceleration is inferred from
this benchmark extension.


### Persistent key ownership experiment

The measured candidate consumes the batch's owned missing-key list when
installing fetched or absent state in the RocksDB working set. It moves each
key into its next owner instead of cloning the bytes; TTL companion cleanup
still clones a key when two owners are required. Pinned values remain borrowed
only through decoding. No extra hash lookup or state representation is added.

[Eight unchanged-source ingestion baselines](persistent-group-key-move-original-timing-2026-10-02.csv)
and [all 800 original samples](persistent-group-key-move-original-samples-2026-10-02.csv)
cover repeated and unique domains, short and wide UTF-8 keys, and both nullability
modes at 16,384 rows. [Candidate estimates](persistent-group-key-move-candidate-timing-2026-10-02.csv)
and [all 800 candidate samples](persistent-group-key-move-candidate-samples-2026-10-02.csv)
retain all eight matched cases. The candidate passed 103 release Rust RocksDB
tests; two profiling diagnostics were explicitly ignored. Java parity and
whole-job performance have not yet been established.

The [allocation comparison](persistent-group-key-move-allocation-comparison-2026-10-02.csv)
contains all 1,608 diagnostics: requested Rust allocation bytes decreased in
24 GROUP ingestion fixtures and were unchanged in 1,584 fixtures. Output-buffer
bytes were identical in every fixture. These counters exclude C++ allocations
and background threads and do not measure copied bytes or peak memory.
At 16,384 rows, the unique wide non-null fixture saved 16,384 allocation
requests and 4,841,664 requested bytes; the repeated short non-null fixture
saved eight requests and 256 bytes. Allocation savings alone are insufficient
evidence of a throughput improvement.

Timing results were mixed. Unique wide-key cases slowed by 7.9% and 15.4%,
and the repeated short non-null case slowed by 97.7%; other cases ranged from
11.4% faster to 6.5% slower. The [original-source control estimates](persistent-group-key-move-original-control-timing-2026-10-02.csv)
and [all 800 control samples](persistent-group-key-move-original-control-samples-2026-10-02.csv)
were collected after restoring the original implementation, with the same
source fixtures, allocator and Criterion settings. The repeated short non-null
case returned to 1.576 ms, compared with 1.585 ms originally and 3.133 ms for
the candidate. Unique wide nullable keys returned to 45.788 ms, compared with
46.337 ms originally and 53.456 ms for the candidate. These repeated original
controls do not explain the regression's mechanism, but contradict accepting
the allocation reduction as a throughput win. The original production
implementation remains in place; the measured candidate is rejected and has
no established whole-job speedup.


The opt-in `PersistentGroupAggregateBenchmark` adds a separate row-fed
COUNT(*)/SUM whole-job witness using the configured native RocksDB backend,
one-phase logical mini-batches of 1,024 rows, identical 256 MiB task off-heap
and 128 MiB fixed-per-slot RocksDB budgets, a blackhole sink and both
transposes. Stock/native trials alternate after two warmups and retain five
measurements by default. Each trial times SQL submission through job completion,
including planning and startup; setup of the environment and source view stays
outside the timer. These are end-to-end job timings rather than isolated
steady-state throughput. Source keys and values use independent null masks;
key cardinality and payload width are configurable. Test compilation and a
16,384-row runtime sanity checks passed on released Flink 2.2.1 and 1.18.1
with the archived release/mimalloc library: each executed one test, with no
failures or skips. The
sanity run used no warmup and one trial, so it is not performance evidence.
A separate 30-second CPU profile after two warmups on the 2,000,000-row,
16,384-key wide nullable fixture sampled `updateRocksDBGroupAggregator`
(14,617 of 35,888 samples), Rust `RocksStore` frames and both transpose
operators. This establishes execution of the intended direct RocksDB path.
These are inclusive, overlapping stack counts, not additive costs. Profiler
runs are separate from timing comparisons; representative repeated whole-job
performance measurements remain pending.


### Persistent tumbling SUM lifecycle

A benchmark-only adapter now exposes the production tumbling SUM store's
update, watermark firing, checkpoint and aligned/rebuilt restore entry points.
It derives state field types from the production aggregate definitions and
uses one singleton key group with UTF-8 keys. The new fixture defines 120
profiles: 16/1,024/16,384 rows, repeated/unique key domains, short/wide keys,
both nullability modes and five phases (update, firing, checkpoint after
partial firing, aligned restore and rebuilt restore).

The independent SUM oracle checks all four output columns and their types,
NULLs, ascending window ends and multiplicity. Inputs use sliced buffers and
Unicode/embedded-NUL keys. Recovery retains the second open window after
firing the first, verifies the saved timer deadline and rejects first-window
late input before advancing the restored watermark. Continued processing and
empty repeated firings are checked outside measurement. All 120 new profiles pass release smoke validation, alongside the existing
1,608 profiles, for 1,728 unique diagnostics (1,512 persistent and 216 memory
window-rank profiles). Source hashes were unchanged at completion.
Five focused baseline timing estimates and all 500 samples are retained below;
these characterize existing execution, with no performance improvement claimed. Hopping/cumulative windows, retractions, other aggregate kinds,
TTL and multi-source rescaling are outside this fixture's scope.


[All 120 tumbling allocation profiles](persistent-tumbling-sum-allocations-2026-10-02.csv)
are retained. For 16,384 rows with a unique key domain, wide keys and NULLs,
the update requests 54,122,090 Rust bytes across 278,919 allocation calls;
firing the remaining second window requests 15,353,503 bytes across 105,428
calls and creates 2,245,838 output-buffer bytes. Post-firing checkpointing
requests 511 bytes across 22 calls; aligned restore requests 7,542 across
138 calls; rebuilt restore requests 2,464,392 across 15,007 calls. These are
System counting-allocator diagnostics, excluding C++ and worker allocations,
not copied-byte or peak-memory measurements. Firing measures only the second
window after the first was prepared and fired outside measurement.
For these tumbling fixtures, directory creation, preparation of the initial
state, validation, continuation, final operator close and directory cleanup
are outside the measured closures. Restore timing includes checkpoint
copy/rebuild and database opening. The update phase measures the production
write-through update; its later watermark firings are verification steps.


For the same wide unique nullable 16,384-row shape, release Criterion used
three seconds of warmup, 100 samples and at least five seconds of collection
per phase. All 1,728 untimed diagnostics ran before the filtered timing cases.
Production source and fixture hashes were unchanged at completion. Rust 1.94.0,
Arrow 58.3.0, Linux/Core i7-12650H, System counting allocator and the linked
RocksDB options apply; C++ and worker allocation costs are not counted.

| Phase | Mean | 95% confidence interval |
| --- | ---: | ---: |
| Write-through update | 52.524 ms | 52.168–52.898 ms |
| Remaining-window firing | 10.753 ms | 10.324–11.396 ms |
| Checkpoint after partial firing | 14.807 ms | 13.934–15.924 ms |
| Aligned restore | 13.266 ms | 12.342–14.281 ms |
| Rebuilt restore | 21.300 ms | 21.076–21.528 ms |

[All five timing estimates](persistent-tumbling-original-timing-2026-10-02.csv)
and [all 500 samples](persistent-tumbling-original-samples-2026-10-02.csv)
retain variability. These are operator lifecycle baselines, not release/mimalloc
whole-job comparisons against stock Flink or an optimization candidate.


The opt-in `PersistentTumblingAggregateBenchmark` defines a separate
row-fed COUNT(*)/SUM whole-job witness with two one-second event-time windows,
periodic watermark emission disabled and terminal-watermark firing. It retains
the same off-heap/RocksDB budgets, mini-batch settings, blackhole sink and both
transposes as the persistent GROUP witness. Stock/native trials alternate and
profiling is separate. The harness compiles and passes a 16,384-row sanity run
on released Flink 2.2.1 and 1.18.1 (8 keys, 8-byte suffix, nullable inputs,
no warmup and one trial per engine). These checks verify the native plan and both
transposes. A separate release/mimalloc CPU profile on Flink 2.2.1, with two
warmup jobs and a 30-second profiling target over 2,000,000 rows, 16,384 keys,
264-byte suffixes and nullable inputs, verifies direct RocksDB execution:
26,825 of 38,467 CPU samples include `pushRocksDBWindowAggregator`, 6,976 include
`RocksWindowAggStore`, and both transpose operators appear.
[The runtime witness counts](persistent-tumbling-runtime-witness-2026-10-02.csv)
are inclusive and overlap; they must not be added or treated as latency shares.
The repeated stock/current/previous-production comparison is recorded below.
Neither the profile nor the single-trial sanity runs are performance evidence.

## Persistent tumbling whole-job baseline

A release/mimalloc Flink 2.2.1 run of the row-fed COUNT(*)/SUM harness used
2,000,000 rows, 16,384 keys, 264-byte key suffixes and independent key/value NULL
masks. The two fixed one-second event-time windows fire on the terminal
watermark. Parallelism is one, task off-heap memory is 256 MiB and fixed-slot
RocksDB memory is 128 MiB for both engines; the sink is blackhole and the native
plan retains both transposes. Two warmup jobs per engine precede five retained
trials per engine, alternating engine order. The measured boundary is
`executeSql(...).await()`, including job startup, conversion, JNI and state work;
environment/view construction and plan validation precede measurement.

The byte-identical harness runs sequentially against current production
(`33052baf`, with buffer reuse reverted), previous production (`1b1b5ed8`) and
current production again. Each checkout uses its own verified release/mimalloc
library and production Java classes; the only added previous-checkout source is
the same benchmark harness. [All 30 measured trials](persistent-tumbling-whole-job-trials-2026-10-02.csv)
are retained.

| Run | Stock median (range), s | Native median (range), s | Native vs stock |
| --- | ---: | ---: | ---: |
| Current before previous | 7.744 (7.420–8.009) | 8.152 (7.718–8.202) | 5.3% slower |
| Previous production | 7.178 (7.170–7.268) | 7.686 (7.572–7.838) | 7.1% slower |
| Current after previous | 7.060 (7.015–7.252) | 7.810 (7.559–7.868) | 10.6% slower |

Native trails stock in all three runs for this profile. The first run's ranges
overlap; the other two runs' stock/native ranges do not. Stock also changes
between runs, so these observations establish no clean cross-revision speed
claim or explanation for the drift. This single nullable wide-key profile does
not cover other cardinalities, widths, NULL modes or checkpoint behavior. The
measurements characterize an existing production path; no new window-state
optimization is accepted. A separate previous-production profile also verifies
the direct RocksDB path: 26,452 of 38,717 CPU samples include
`pushRocksDBWindowAggregator`, 6,731 include `RocksWindowAggStore`, and both
transposes appear. [The previous runtime witness](persistent-tumbling-previous-runtime-witness-2026-10-02.csv)
retains inclusive overlapping counts, which must not be added or used as latency
shares. Both profiles are separate from the reported timings.

## Window-state value buffer reuse experiment

The unchanged-production buffer-reuse baseline additionally measured all eight
16,384-row update shapes (domain 8/16,384, key suffix 8/264 bytes, nullable/non-null),
with 100 samples, a three-second warmup and a five-second target per case.
[The eight estimates](persistent-tumbling-buffer-original-timing-2026-10-02.csv)
and [all 800 samples](persistent-tumbling-buffer-original-samples-2026-10-02.csv)
are retained. Fixture validation interleaves with benchmark registration: the
first selected timing followed 1,689 diagnostics, with 39 diagnostics still to
run. All 1,728 diagnostics passed by completion, and source hashes stayed
unchanged throughout the run. These baseline results establish no speedup.

The within-call buffer-reuse experiment passed 104 release Rust state tests
(two profiling tests ignored) and the full 1,728 Criterion fixture diagnostics.
[Allocation requests decreased in 24 profiles and stayed unchanged in 1,704](persistent-tumbling-buffer-allocation-comparison-2026-10-02.csv);
output buffer counts matched throughout.
[Eight candidate timing estimates](persistent-tumbling-buffer-candidate-timing-2026-10-02.csv)
and [800 candidate samples](persistent-tumbling-buffer-candidate-samples-2026-10-02.csv)
show mixed results. The [restored-original estimates](persistent-tumbling-buffer-control-timing-2026-10-02.csv)
and [800 control samples](persistent-tumbling-buffer-control-samples-2026-10-02.csv)
complete a sequential original/candidate/control experiment, with all 1,728
fixtures passing in each run. [The three-run comparison](persistent-tumbling-buffer-timing-comparison-2026-10-02.csv)
shows that large repeated-key changes also appeared in unchanged production:
the original-to-candidate 56.5% decrease for short non-null keys accompanied a
56.7% original-to-control decrease; the 25.6% candidate increase for wide nullable
keys accompanied a 35.3% control increase. Unique-key candidate/control changes
were mixed and within 2%; repeated short nullable keys were 6.1% slower than control.
These observations establish no broadly repeatable speedup or drift mechanism.
Original/control allocation diagnostics differed in one 1,024-row case by one
request and 568 bytes; all output buffer counts and the other 1,727 cases matched.
The production loop is restored. The [rejection record](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/.claude/wontdos/window-state-value-buffer-reuse.md)
retains the design and correctness checks. Candidate released Flink parity and
release/mimalloc whole-job improvement were not established.

## Persistent session SUM lifecycle

The session SUM fixture calls the production session aggregator and direct
RocksDB store. A separate event-list oracle groups nullable UTF-8 keys, sorts
timestamps and merges gap-connected intervals. Inputs prepare separated sessions
at 100 and 2,100 ms; a 1,100-ms batch bridges both, while an 8,100-ms session
remains after partial firing. The intended measured boundaries are bridge merge,
pending-session firing, checkpoint after partial firing and aligned/rebuilt
single-source recovery. Variable widths, repeated/unique keys, independent NULL
masks and nonzero-offset input slices match the tumbling matrix. Saved watermark,
timer deadline, late-row rejection, continued processing and no refiring are
checked outside timing. Release compilation and the full 1,848-diagnostic smoke
run pass, including all 120 session lifecycle profiles; guarded sources remain
unchanged throughout. [All session allocation profiles](persistent-session-sum-allocations-2026-10-02.csv)
are retained. These counters exclude RocksDB C++ and background workers and do
not measure copied bytes or peak memory. The focused original timing run also
passes all 1,848 diagnostics with unchanged guarded sources. This adds validated
lifecycle coverage and an original baseline, not a speed improvement.

Each profile runs its untimed oracle and allocation diagnostic before its
Criterion registration. Selected profiles then run their timing before later
profiles finish validation. `iter_batched_ref` prepares a fresh database and
operator for each iteration outside the measured boundary; that preparation
and cleanup can make wall-clock runtime substantially longer than Criterion's
reported measurement estimate. A partial log does not establish that every
fixture or timing has completed.

The gap is 1,000 ms and SUM uses nullable Int64 values with UTF-8 grouping keys.
Input events arrive out of timestamp order when the bridge is applied; the
oracle checks exact session bounds, nullable sums, multiplicity and output schema.
The checkpoint retains the 3,100-ms watermark and timer deadline 12,345. Recovery
includes checkpoint-file copy/rebuild and opening the restored database; operator
closing, temporary-directory cleanup, prepared state and continued-output
validation stay outside timing. These singleton-key-group, single-source fixtures
do not cover other aggregate kinds, retractions, TTL, processing time, arbitrary
gaps, overflow boundaries or multi-source rescaling.

For the 16,384-row unique-key nullable session profile with 264-byte suffixes,
the current-thread Rust allocation requests are:

| Phase | Requests | Requested bytes | Newly allocated output buffers |
| --- | ---: | ---: | ---: |
| Bridge merge | 1,252,871 | 158,966,734 | 0 |
| Remaining-session firing | 240,533 | 36,545,705 | 4,491,646 |
| Checkpoint after partial firing | 35 | 827 | 0 |
| Aligned recovery | 168 | 8,307 | 0 |
| Key-group rebuild recovery | 29,901 | 4,921,903 | 0 |

The same 16,384-row unique-key, wide nullable profile has the following original
Criterion means from 100 samples per phase (3-second warmup, 5-second target,
extended automatically where needed):

| Phase | Mean (ms) | 95% interval (ms) |
| --- | ---: | ---: |
| Checkpoint after partial firing | 12.693 | 12.045–13.367 |
| Remaining-session firing | 57.409 | 46.823–69.203 |
| Bridge merge | 291.554 | 264.566–321.756 |
| Aligned recovery | 14.008 | 13.834–14.279 |
| Key-group rebuild recovery | 36.745 | 36.410–37.096 |

[All five estimates](persistent-session-original-timing-2026-10-02.csv) and
[500 raw samples](persistent-session-original-samples-2026-10-02.csv) are retained.
Merge and firing each have 18 high outliers out of 100 measurements, so their
variability must remain visible in any subsequent candidate/control comparison.
These measurements use the counting System allocator and do not establish
release/mimalloc whole-job performance against stock Flink.

These characterize existing paths. They are neither a before/after comparison
nor evidence that all requested bytes are copies that can be removed. Disk I/O,
C++ allocations and worker threads remain outside these counters.

The opt-in `PersistentSessionAggregateBenchmark` prepares a separate
row-fed COUNT(*)/SUM witness using legacy `SESSION` SQL. Per-key input timestamps
cycle through 0, 2 and 1 seconds, so the third timestamp phase bridges the separated
one-second sessions; periodic watermark emission is disabled and the terminal
watermark fires results. The harness retains identical stock/native state
budgets, both transposes, blackhole output, alternating trials and separate
profiling. Compilation and one unskipped sanity test pass on both released
Flink 2.2.1 and 1.18.1 using a verified release/mimalloc library (16,384 rows,
eight keys, eight-byte suffixes, nullable input, no warmup and one trial).
These checks assert the session operator and both transposes in the plan and
nonzero runtime substitutions. Normal checks with Javadocs enabled also pass
on both released versions. Representative repeated whole-job timings remain
pending. The sanity trials establish harness execution, not performance or SQL
result parity; the
blackhole sink does not compare output values.


The original session CPU recording also completes on released Flink 2.2.1 using
release/mimalloc, two warmups and a 30-second recording request. Of 41,158 CPU
samples, inclusive markers include session push JNI (33,922), flush JNI (136),
`RocksSessionAggStore` (2,840), input transpose (1,344), output transpose (10),
session aggregation (32,978) and Arrow `take` (723). Markers overlap and are not
additive; the [runtime witness counts](persistent-session-runtime-witness-2026-10-02.csv)
prove runtime routes, not throughput or copied bytes. Session-inclusive leaf
frames also include unresolved libc frames, RocksDB skip-list/index work and
allocation calls; the profile does not justify attributing all session cost
to Arrow gathering or any particular copy.

A rejected contiguous-row selection candidate uses owned Arrow slices after
restoring arrival order, retaining `take` for scattered selections. Both new
regressions and all 16 release session tests pass. They cover sequential FLOAT
SUM arrival order, Unicode/NUL string DISTINCT, independently nullable values,
singleton/contiguous/scattered groups, nonzero input offsets, input release
before firing, snapshot recovery and no refiring. Existing tests also check
persistent merge tombstones, saved watermark recovery and memory budgets.
The matched candidate Criterion run completes with all 1,848 diagnostics,
unchanged fixtures and frozen sources. The restored-original control also
passes all 1,848 diagnostics. Production retains its original selection loop;
released candidate parity and whole-job improvement remain unestablished.

DOUBLE SUM delegates to Arrow aggregation; the cancellation array
`[1e16, -1e16, 1, NULL]` returns zero in both the original and candidate native
batch tests rather than the sequential-fold result one. That failed initial
assertion does not distinguish the candidate from production. The retained
arrival-order regression instead uses the explicit sequential FLOAT accumulator.
A separate legacy SESSION SQL cancellation probe for DOUBLE and FLOAT passes
with one unskipped test on each released Flink version against the original
release/mimalloc library. It uses non-monotonic timestamps, NULLs and terminal
watermark firing and requires native runtime substitutions. This checks the
SQL harness output, not identical native batch partitioning; it does not prove
arbitrary DOUBLE batch parity or parity with a rebuilt candidate library.

The cancellation probe now additionally requires the specific native columnar
session operator in its explained plan on a separate fresh environment. Plan
inspection installs native execution, so it runs outside the shared environment
factory to preserve the independent stock result. The strengthened test passes
with one unskipped test and normal Javadoc checks on both released Flink 2.2.1
and 1.18.1 against the original release/mimalloc library.


The complete [allocation comparison](persistent-session-slice-allocation-comparison-2026-10-02.csv)
retains all 1,848 cases: 14 have fewer allocation calls, 1,834 are unchanged and
none have more; all output-buffer counters match. [All 120 candidate session
profiles](persistent-session-slice-candidate-allocations-2026-10-02.csv) are saved.
Wide nullable unique-key merge changes from 1,252,871 calls/158,966,734 requested
bytes to 1,178,401 calls/156,226,238 requested bytes, saving 74,470 calls and
2,740,496 requested bytes. These counters do not measure copied bytes.

[Five candidate estimates](persistent-session-slice-candidate-timing-2026-10-02.csv)
and [500 samples](persistent-session-slice-candidate-samples-2026-10-02.csv) preserve
all phases. Means in milliseconds are merge 290.088, firing 35.085, checkpoint
72.652, aligned recovery 14.871 and rebuilt recovery 36.941. Merge overlaps the
original 291.554-ms interval, firing is lower than the variable original and
checkpoint is substantially higher with a broad interval. The update-only
change does not justify attributing these other phase differences to removed
copies.

The [completed three-run comparison](persistent-session-slice-timing-comparison-2026-10-02.csv)
verifies all 1,500 samples and matching original/control source hashes. The
[control estimates](persistent-session-slice-control-timing-2026-10-02.csv) and
[500 control samples](persistent-session-slice-control-samples-2026-10-02.csv)
retain every selected phase:

| Phase | Original, ms | Candidate, ms | Restored original, ms |
| --- | ---: | ---: | ---: |
| Merge | 291.554 | 290.088 | 229.110 |
| Fire | 57.409 | 35.085 | 58.016 |
| Checkpoint after fire | 12.693 | 72.652 | 9.958 |
| Aligned restore | 14.008 | 14.871 | 14.302 |
| Rebuilt restore | 36.745 | 36.941 | 36.518 |

Merge has no repeatable benefit: the candidate overlaps the first original run
and is slower than the restored-original control. Other phases are mixed;
these sequential runs do not establish the cause of drift or a causal gain
from slicing. The [complete control allocation comparison](persistent-session-slice-control-allocation-comparison-2026-10-02.csv)
retains all 1,848 cases: two 1,024-row unique short-key merge cases each differ
by one allocation and 56 requested bytes; output counters match in every case.
The candidate is rejected despite lower allocation counts. No candidate whole-job
speedup is claimed. The repository decision record at
`.claude/wontdos/session-contiguous-value-slices.md` retains the scope and
conditions for revisiting this experiment.


## Persistent window-join lifecycle

The validated matrix has 60 profiles: 16/1,024/16,384 rows, unique UTF8 keys
with 8/264-byte suffixes, nullable/non-null inputs and five phases (right
arrival, firing, checkpoint after partial firing, aligned restore and rebuilt
restore). Independent tuples check both sides, exact output schema, window
bounds, nullable payloads and NULL-key non-matches. Inputs have nonzero slice
offsets. Duplicate keys, outer joins, residual predicates, TTL and multi-source
rescaling are outside this matrix.

Each iteration starts with a fresh temporary destination database. Input batch
construction, initial arrivals and the first window firing are setup work;
right arrival times the remaining right-side push, and firing times the pending
window flush. Checkpoint timing includes the production snapshot after partial
firing. Restore timing includes opening/copying the checkpoint or rebuilding its
key group, with the source checkpoint prepared outside timing. Production state
serialization and RocksDB work within these calls remain measured. Fresh-store
setup and fixture validation can make wall time substantially exceed Criterion's
measurement target.

The untimed oracle checks the complete output multiset, schema and window bounds,
then continues through pending and future windows. Recovery checks new matching
arrivals before watermark replay and rejects arrivals after replay; repeated
flushes without new input must emit nothing. Allocation requests describe the
Rust calling thread under the System counting allocator, excluding RocksDB C++
and background workers. They are not measurements of bytes copied or peak memory.

The first full release smoke run passed 1,908 unique diagnostics, including all
60 new profiles, with frozen sources. Its facade artificially restored an
event-time watermark, so those results remain provisional and are not accepted
as production recovery coverage. That assignment has been removed. Production
window-join snapshots preserve side buffers, arrival sequences and the
processing-time timer deadline; watermarks are replayed after recovery.

The corrected fixture checks acceptance before watermark replay, late-row
rejection afterward, retained pending rows, continued arrivals and no refiring.
A released-host harness independently checks this lifecycle alongside raw-native
and direct RocksDB cases, asserting the native backend before and after restore.
All three unskipped cases pass on both released Flink 2.2.1 and 1.18.1,
including normal Javadoc checks. The initial stock harness key-representation error is retained in
its separate log; it did not test recovery semantics. The corrected full release
smoke run passes all 1,908 unique diagnostics, including the 60 window-join
profiles, with unchanged sources. [All allocation diagnostics](persistent-window-join-allocations-2026-10-02.csv)
are retained. For 16,384 rows with 264-byte key suffixes and nullable input:

| Phase | Rust allocation requests | Requested bytes | New output-buffer bytes |
| --- | ---: | ---: | ---: |
| Right arrival | 16,407 | 10,271,856 | 0 |
| Pending-window firing | 157,142 | 171,184,850 | 8,979,512 |
| Checkpoint after partial firing | 32 | 796 | 0 |
| Aligned restore | 93 | 4,734 | 0 |
| Rebuilt restore | 65,585 | 10,545,645 | 0 |

These are baseline allocation observations, not before/after savings.

The same wide nullable profile has five completed release Criterion timings,
using the System counting allocator, 100 samples per phase, three seconds of
warmup and a five-second measurement target. Fresh-store setup causes Criterion
to exceed that target when collecting the minimum samples. All 1,908 fixture
diagnostics pass at completion, and source hashes remain unchanged. No heavy
local workload overlaps timing. Fixture validation and benchmark registration
interleave; this is not a full untimed prelude preceding every timing.

| Phase | Mean, ms | 95% confidence interval, ms |
| --- | ---: | ---: |
| Checkpoint after partial firing | 38.343 | 33.733–43.467 |
| Pending-window firing | 74.263 | 73.906–74.628 |
| Right arrival | 65.676 | 45.167–88.212 |
| Aligned restore | 17.697 | 17.384–18.053 |
| Rebuilt restore | 51.356 | 49.225–54.074 |

[All five estimates](persistent-window-join-original-timing-2026-10-02.csv) and
[all 500 samples](persistent-window-join-original-samples-2026-10-02.csv) are
retained, including variability and outliers. These baseline timings establish
no before/after or whole-job speedup. No window-join production optimization
is accepted from this coverage work.

## CI smoke runtime

The release fixture job has a 150-minute timeout, including compilation and
all suites. The previous 60-minute job was cancelled during the persistent-state
suite on a run with no restored Rust cache. This is a wall-time allowance, not
a performance threshold or a reason to skip fixtures. Full-suite completion
and retained diagnostics remain required.

## Persistent updating-join state

The 360-profile matrix covers immediate multiset INNER joins and admitted
unique-key mini-batch INNER joins using paired production RocksDB state. Shapes
combine 16, 1,024 and 16,384 incoming rows, declared key domains of 1, 8 and
the incoming row count, 8/264-byte ASCII key suffixes, nullable/nonnullable
values, both execution modes and five phases: right insertion, right
retraction, checkpoint, aligned restore and rebuilt restore.

In unique-key mode, right-side UPDATE_AFTER rows replace prior values; an
independent oracle retains the final value per non-NULL key. Both sides receive
uniqueness hints matching planner admission. Immediate mode retains duplicate
multiplicity. Both inputs use the same explicitly nullable schema and sliced
arrays. Continuation checks retract retained state, clear both sides, reinsert
rows and compare the output schema and full multiset after recovery.

Two failed fixture investigations are retained separately: a non-unique
changelog mini-batch configuration excluded by production admission, and a
schema incorrectly inferred from a left input without NULLs. Neither
establishes a production bug. The validated fixture corrects both conditions
and registers updating joins first while retaining the complete existing matrix.

The corrected full release smoke run passed all 2,268 profiles, including
all 360 updating-join profiles covering
nullable wide inputs, duplicate immediate-mode changelogs, unique-key mini-batch
replacement/retraction and paired aligned/rebuilt recovery continuation. The
allocation diagnostics are exported in
[`persistent-updating-join-allocations-2026-10-02.csv`](persistent-updating-join-allocations-2026-10-02.csv).
All eight source-provenance hashes matched the tested build. Recovery validation
passed against released Flink 2.2.1 and 1.18.1: eight SQL cases crossing two
restores and one raw keyed-state recovery case per version, with zero failures,
errors or skips. Source and native-library hashes matched before and after each
run. These recovery tests validate the production recovery paths; they do not
use the exact Criterion fixture data. The timing baseline also passed; these
allocation counts do not establish a speedup.

The completed timing baseline selects 30 cases: 16,384 incoming rows, declared
key domains of 1, 8 and 16,384, a 264-byte ASCII suffix plus a numeric prefix,
Unicode and an embedded NUL, nullable keys/payloads, both execution modes, and
all five phases. Each case uses 100 samples, a three-second warmup and a
five-second measurement target.
Criterion extends collection beyond that target when 100 iterations require
more time; the sample count remains 100. The target is not a wall-time cap.
Fresh databases, prepopulated input state and
the recovery source checkpoint are prepared outside timing with
`iter_batched_ref`; this setup can dominate wall time. Push and retraction
include flushing and output materialization. Checkpoint and recovery measure
their production state operations. The complete 2,268-profile registration
remains enabled, with validation and allocation diagnostics interleaved with
the selected timings.

The two modes represent different changelog contracts: immediate mode retains
duplicate rows, while unique-key mini-batches retain the final right-side value
per key. Their output sizes therefore differ. These timings characterize each
contract separately and cannot establish that one mode speeds up the other.
Counting covers allocations requested on the measured Rust thread using the
System allocator; it excludes RocksDB C++ allocations and background workers.
Requested bytes are neither copied bytes nor peak resident memory. These are
native operation baselines, without Java/JNI or either row/Arrow transpose;
whole-job acceleration still requires separate end-to-end comparisons.

Updating-join coverage here is limited to INNER joins with one key column,
no residual predicate, disabled TTL and one checkpoint source. It does not
cover outer, semi or anti joins, insert-only mini-batches, pending unflushed
bundles, expiration, multiple-source recovery, rescaling, or the in-memory
backend. The largest declared domain also does not make every incoming right
row unique: adjacent right rows share a key and payload. Completing these
fixtures does not complete Criterion coverage across all Rust operations or
close the remaining updating-join performance work.

The full timing command and exporter passed with unchanged source guards and
all 2,268 diagnostics. Retained artifacts contain
[30 baseline estimates](persistent-updating-join-original-timing-2026-10-02.csv)
and [3,000 samples](persistent-updating-join-original-samples-2026-10-02.csv).
For the large-domain, nullable wide-key immediate workload, right insertion
averaged 169.347 ms and retraction averaged 5,698.637 ms (95% mean confidence
interval 5,679.415–5,720.787 ms). This identifies retraction as a profiling
target; it does not attribute the cost to copying or demonstrate an optimization.

### Host-reader callback profiles

`ParquetBenchmarkInput` is the Java test fixture for the `parquet_host_reader`
Criterion workload. Its memory and local-file modes implement the synchronous
`readFully(long, ByteBuffer)` signature consumed by the production Rust Parquet
reader, with read-call and byte-count witnesses. Fixture construction and local
file creation belong outside timing; decoder construction and decoding must
retain production JNI calls and direct-buffer creation. The suite has 48 profiles: 16, 1,024 and 16,384 rows; 8- and 264-byte ASCII
suffixes plus Unicode and embedded NUL; nullable string payloads; memory and
local-file callbacks; batch sizes 64 and 4,096; open/close and decode phases. It does not emulate
remote filesystems, credentials, or Paimon checkpoint semantics.
The three Java 17 fixture tests pass with zero failures, errors or skips:
requested direct-buffer range fidelity, out-of-bounds rejection without changing
counters, and rejection of callbacks after the host input closes.
The Rust `HostDecoder` benchmark adapter calls the production JNI create,
next-batch and close entry points, retaining the host callback and Arrow C Data
export/import. It ties the native handle to the live JVM and closes it on drop;
the host input must remain open until the decoder is dropped. Its locked Cargo
check and all 48 release smoke cases pass. Decode setup creates the decoder
outside allocation measurement and timing; the measured loop retains callback
reads, Arrow export/import, output release and decoder teardown. Open/close
measures construction and teardown. Exact Arrow output validation runs outside
both probes and timing; the retained validation batches provide the separate
Arrow-buffer ownership diagnostics.

[Allocation probes](parquet-host-reader-allocation-2026-10-04.csv) and
[callback witnesses](parquet-host-reader-callbacks-2026-10-04.csv) retain all
48 cases. The runner prepares the runtime test classes and JVM classpath.
Smoke mode provides no timing evidence or whole-job acceleration claim.
These local warm-cache fixtures do not cover projections or nested columns.

### Host-reader release timing baseline

A focused optimized/mimalloc run retains [four decode estimates](parquet-host-reader-timing-2026-10-04.csv)
and [400 samples](parquet-host-reader-samples-2026-10-04.csv) for 16,384 rows
and the nullable 264-byte suffix fixture. Source and executable hashes remain
unchanged; symbol inspection confirms `malloc` and `mi_malloc` resolve to the
same address. Each profile uses a three-second warmup, five-second measurement
target and 100 samples. JVM setup and host input creation stay outside timing.

| Host input | Batch rows | Mean ms | 95% interval ms |
|---|---:|---:|---:|
| Memory | 64 | 1.0625 | 1.0585–1.0665 |
| Memory | 4,096 | 0.4775 | 0.4756–0.4794 |
| Local file | 64 | 1.1096 | 1.1032–1.1165 |
| Local file | 4,096 | 0.5095 | 0.5071–0.5120 |

These describe existing host callback decoding with warm local caches. Batch
size changes the output-batch and callback behavior; this comparison neither
isolates an avoidable copy nor establishes a production optimization. It has no
stock Flink or previous-version whole-job comparison, no row/Arrow transposes,
and no remote filesystem or projection workload.

### Persistent aggregate retractions

The shared GROUP BY fixtures add 48 persistent COUNT(*)/SUM retraction profiles
across 16/1,024/16,384 rows, repeated/unique keys, Unicode keys with 8/264-byte suffixes and
nullable keys/values. Each starts from an already flushed aggregate outside
measurement, then retracts half or all original rows through production
update/flush. A separate map oracle rebuilds surviving groups and checks schema,
UPDATE_BEFORE/UPDATE_AFTER versus DELETE, per-key order and empty repeated flush.
Reinserting removed rows verifies state deletion and NULL-aware SUM continuation.
Fixture construction, prior inserts, database creation, reinsertion checks and
directory cleanup remain outside timing; retraction reads/writes and output
Arrow construction are included.

The focused `persistent_group` optimized release executable passes all 48
retraction smoke cases. Its mimalloc malloc/calloc aliases and checked free/realloc
shims are verified. [Allocation probes](group-retractions-allocation-2026-10-04.csv)
record Rust-thread allocation requests and Arrow output sharing; they exclude
RocksDB C++ and background threads and do not measure copied bytes. The nullable
16,384-row wide unique-key case requests 37,268,964 bytes for half-row retraction
and 74,995,124 bytes for all-row retraction. These characterize existing behavior,
not a before/after optimization. The comprehensive persistent-state smoke also
completes successfully: 2,316 allocation probes, with all 48 retraction profiles
matching the focused executable's allocation metrics.

Focused optimized release/mimalloc timings retain three seconds of warmup,
100 samples per case and a five-second target measurement, on a quiet host.
Source/executable hashes and allocator aliases pass before/after guards.
Database creation and prior inserts are outside the measured update/flush path;
Criterion's wall-clock run still includes that setup. Nullable unique-key inputs
use 264-byte suffixes with Unicode/prefix/embedded-zero key bytes.

| Initial rows | Retract half: mean | Retract all: mean |
| --- | --- | --- |
| 1,024 | 0.917 ms | 2.052 ms |
| 16,384 | 20.140 ms | 47.657 ms |

[Mean confidence intervals](group-retractions-timing-2026-10-04.csv) and
[all 400 samples](group-retractions-samples-2026-10-04.csv) retain variability and
outliers. These are baseline measurements of current COUNT/SUM retractions,
not a before/after comparison or whole-job acceleration claim.

The runner-level release smoke with `--features mimalloc` also passes all 48
retraction cases. Its retained metadata records the requested feature and exact
Cargo command; feature selection does not change the allocation-counter scope.


### Persistent extrema profiles

The shared persistent GROUP BY fixtures add 144 COUNT(*)/Int64 MIN/MAX profiles:
insertion, half/all retraction, checkpoint after partial retraction, and
aligned/rebuilt single-source recovery. Shapes vary 16/1,024/16,384 initial rows,
repeated/unique nullable UTF-8 keys with 8/264-byte suffixes and nullable values.
An independent per-key value-list oracle computes extrema without reusing the
production multiset implementation. It checks complete schemas and changelogs,
NULL-only groups, per-key transition order, deleted-state reinsertion, duplicate
values and continued arrivals after recovery. Other extrema value types,
DISTINCT, TTL and multi-source recovery are still unmeasured here.

The thin benchmark adapter selects the existing production COUNT/MIN/MAX
operator and codec. Existing COUNT/SUM construction and recovery retain their
configuration. The optimized release/mimalloc build and all 144 new smoke cases
pass, together with 144 existing COUNT/SUM allocation/output probes. The focused
and comprehensive targets share these fixtures. [All 144 allocation probes](persistent-group-extrema-allocations-2026-10-04.csv)
retain Rust-thread requests and Arrow buffer metrics, excluding RocksDB C++ and
background threads; these are not copied-byte measurements.

Six focused release cases retain three-second warmup, a five-second measurement
target and 100 samples each. Source/executable hashes and mimalloc allocator
aliases pass before/after guards. Expensive fixtures extend the target time to
retain all 100 samples. Database creation, prior state population, source
checkpoint creation and validation remain outside timing. Measured paths include
production update/flush, checkpoint I/O or recovery reads and database opening;
restored operator destruction and directory cleanup remain outside timing.
All cases use 16,384 initial rows, 264-byte key suffixes and nullable keys/values.

| Case | Mean |
| --- | --- |
| Repeated keys, half-row retraction | 7.530 ms |
| Repeated keys, all-row retraction | 15.602 ms |
| Unique keys, half-row retraction | 153.764 ms |
| Unique keys, all-row retraction | 559.380 ms |
| Unique keys, aligned recovery | 16.192 ms |
| Unique keys, rebuilt recovery | 54.940 ms |

The unique-key all-row case is variable: per-sample median 339.839 ms, range
322.469–3,213.546 ms and mean 95% interval 458.357–674.998 ms. Every sample and
outlier is retained in [600 samples](persistent-group-extrema-samples-2026-10-04.csv)
and [mean intervals](persistent-group-extrema-timing-2026-10-04.csv). These measure
existing behavior, not a before/after optimization or SQL acceleration claim.

### Deleted-group MIN/MAX reseeks

The aggregate update path skips resolving replacement extrema when the updated
record count is zero. Deleted groups emit their cached preimage, so opening
MIN/MAX RocksDB iterators immediately before removing the group is unnecessary.
Liveness is captured inside the existing mutable-state borrow, without another
state lookup. Surviving groups retain their normal extrema resolution.

Validation includes 17 RocksDB multiset tests and 24 broader GROUP BY tests,
plus all 144 COUNT/MIN/MAX Criterion fixtures checked against an independent
value-list oracle. All 288 allocation probes, including legacy COUNT/SUM,
are present. The SQL deletion/reinsertion fixtures pass on released Flink 2.2.1
and 1.18.1 under JDK 17 (two checks per line, no skips). They compare full
kinded changelogs for immediate and deterministic two-row mini-batches,
including duplicate minima, surviving-extreme reseeks, complete deletion,
reinsertion, NULL-only groups, a NULL key and a wide Unicode/NUL key.
An explicit rowwise blackhole INSERT plan must contain native GROUP BY and
both transposes before the SELECT changelog comparison runs.

The [144-profile allocation comparison](deleted-group-extrema-allocation-comparison-2026-10-04.csv)
retains unchanged output-buffer metrics across all cases and unchanged
non-retraction allocation probes. Full deletion of 16,384 unique groups with
264-byte key suffixes and nullable values reduces Rust-thread allocation
requests from 839,399 calls / 218,098,581 bytes to 454,219 calls /
173,771,325 bytes. Half deletion reduces 416,236 calls / 108,655,867 bytes
to 224,776 calls / 86,573,155 bytes. The repeated-eight-key half-deletion
control remains exactly 60,513 calls / 21,468,954 bytes. These counts exclude
RocksDB C++ and background allocations; they do not measure copied bytes.

Release/mimalloc kernel comparisons use immutable executables in matched
candidate/original/original/candidate order, 100 samples per profile, three
seconds of warmup and a five-second collection target (extended by Criterion
when needed). All [24 estimates](deleted-group-extrema-timing-2026-10-04.csv)
and [2,400 samples](deleted-group-extrema-samples-2026-10-04.csv) are retained.
Unique-group half/full deletion combined means improve 39.70%/34.79%;
repeated-eight-key full deletion improves 4.84%. The surviving-group half
control initially increases 2.73%, and the final rebuilt-recovery candidate
round measures 102.712 ms versus 45–48 ms in the other rounds. These
unfavorable observations are retained rather than discarded.

Focused repeats retain [eight recovery estimates](deleted-group-extrema-recovery-repeat-timing-2026-10-04.csv)
and [800 recovery samples](deleted-group-extrema-recovery-repeat-samples-2026-10-04.csv),
plus [four surviving-group estimates](deleted-group-extrema-surviving-repeat-timing-2026-10-04.csv)
and [400 surviving-group samples](deleted-group-extrema-surviving-repeat-samples-2026-10-04.csv).
Rebuilt recovery means are original 48.389/46.522 ms versus candidate
49.631/45.840 ms (combined +0.59%); aligned recovery improves 5.89%.
The surviving-group control is original 7.872/7.582 ms versus candidate
7.700/7.625 ms (combined -0.83%). Neither earlier slowdown reproduces in
these focused repeats; the cause of the original observations remains unproven.

The whole-job harness has an opt-in `-Dpersistent.group.extrema=true` mode.
Each cycle inserts a low and high value per key, retracts the low value,
then retracts the high value and deletes the group. Repeated cycles exercise
reinsertion. Nullable inputs include NULL-only groups and a shared NULL key.
Rows must be divisible by four times `persistent.group.keys`. This mode defaults
to immediate emission; `persistent.group.miniBatch=true` permits the existing
1,024-row mini-batch setting. The legacy append-only COUNT/SUM mode is unchanged.

A separate worktree isolates this production change from other pending
experiments. Both release/mimalloc libraries are immutable, hashed, and have
verified allocator aliases. All four timed rounds retain JVM load traces
explicitly identifying the intended library and its hash. The small smoke
check compiles the harness and verifies its native GROUP BY and both-transpose
plan; its startup-dominated result is excluded from the sustained comparison.

Whole-job ABBA uses released Flink 2.2.1/JDK 17, 2,097,152 changelog rows,
16,384 keys, 264-byte suffixes, nullable inputs, parallelism one, immediate
emission, two warmups and five alternating measurements per engine per round.
Each engine has two active processors, a 2-GB heap, 256-MB task off-heap budget
and 128-MB RocksDB budget. Both transposes, JNI, state and the rowwise blackhole
sink stay in the measured path. All [40 trials](deleted-group-extrema-wholejob-trials-2026-10-04.csv)
and [four summaries](deleted-group-extrema-wholejob-summary-2026-10-04.csv) are retained.

| Round | Library | Stock median (s) | Native median (s) |
| --- | --- | ---: | ---: |
| 1 | Candidate | 29.853597 | 26.361456 |
| 2 | Original | 30.221409 | 45.242764 |
| 3 | Original | 30.329171 | 44.758378 |
| 4 | Candidate | 29.504734 | 25.843561 |

The candidate rounds improve 11.70–12.41% versus stock. Pooling ten native
trials per variant gives candidate median 26.104207 s versus original
45.103323 s (42.12% lower). Corresponding pooled stock medians differ by
2.08%; the complete native trial ranges remain disjoint. These results establish
this deletion optimization for the tested workload, not other aggregate types,
mini-batch performance, TTL, rescaling, or complete Criterion coverage of all
Rust operations. They do not close the pending BINARY/collection/floating work.

### Fixed BINARY to variable BYTES cast allocation baseline

The binary expression suite adds 144 production Calc fixtures for fixed BINARY
to variable BYTES casts. It covers both the legacy unbounded marker and the
normal SQL BYTES length marker, with 0/16/1,024/16,384 rows, widths 1/16/256,
nullable/non-null inputs and slice offsets 0/1/5. Independent byte-vector
oracles check variable Binary output, NULLs, embedded zero and high bytes.
Each fixture validates output before measuring allocations and Criterion time.

All 144 fixtures pass against the existing implementation in a release build
with verified mimalloc aliases. [Original allocations](fixed-to-variable-sql-original-allocations-2026-10-04.csv)
show that nullable 16,384-row width-256 offset-one casts request 4,263,399 bytes
in 27 calls for normal SQL BYTES, and 8,441,319 bytes in 35 calls for the legacy
marker. Neither output shares input buffers; both produce 3,662,852 new output
buffer bytes. Requested allocation bytes measure allocator requests, not copied
bytes. The existing fixed-to-variable cast rebuilds payload through a binary
builder; these fixtures establish evidence for evaluating buffer reuse.

[Legacy baseline timings](fixed-to-variable-baseline-timing-2026-10-04.csv)
and [all 400 samples](fixed-to-variable-baseline-samples-2026-10-04.csv) cover
nullable offset-one casts with 1,024/16,384 rows at widths one and 256, using
100 samples, three-second warmup and five-second measurement. Means are
5.17/69.64 microseconds at width one and 14.10/300.95 microseconds at width
256. These are Calc-kernel baselines, with immutable executables and source
hashes retained during validation. They establish no whole-job acceleration.

### Fixed-binary cast buffer-sharing experiment

The new SQL fixture checks widths 1/16/256, 5,003 runtime rows, NULLs and high
bytes. Original and candidate libraries each pass three cases on released
Flink 2.2.1 and 1.18.1 with Java 17. Plans assert NativeCalc and both transposes;
exact candidate-library load witnesses are retained. Select the benchmark with
scalar.functions=FIXED_BINARY_TO_BYTES; its fixed-binary identity control runs
alongside it.

[Candidate source](fixed-to-variable-candidate.patch) is retained as an
experimental patch. It shares fixed payload and validity buffers for unbounded
variable casts while constructing offsets. Normal SQL casts retain compacting
fallback for oversized fixed buffers. Four release Rust tests cover slices,
empty/all-NULL arrays, raw bytes and output lifetime after input drop. All
144 Calc fixtures pass.

[All 400 trials](fixed-to-variable-wholejob-trials-2026-10-04.csv) and
[20 summaries](fixed-to-variable-wholejob-summary-2026-10-04.csv) retain stock,
prior native, candidate and identity controls. Each configuration runs
candidate/original/original/candidate with two warmups and five alternating
stock/native trials per query/round. Release/mimalloc runs use Java 17,
Flink 2.2.1, two active processors, 2 GiB heap, disabled local-exchange
zero-copy, a rowwise source, both transposes and a blackhole sink. All rounds
pass source/library hash guards and exact load checks; Surefire dumpstreams
retain load witnesses when logging bypasses Maven stdout.

| Rows | Width | NULL interval | Prior native s | Candidate native s | Stock in candidate rounds s |
| --- | --- | --- | --- | --- | --- |
| 2,000,000 | 256 | 7 | 0.439189 | 0.414566 | 0.306929 |
| 20,000,000 | 16 | none | 2.510410 | 2.434274 | 1.978099 |
| 20,000,000 | 16 | 7 | 2.612453 | 2.527540 | 2.037520 |
| 20,000,000 | 256 | none | 3.702058 | 3.460246 | 2.436618 |
| 20,000,000 | 256 | 7 | 3.719436 | 3.571197 | 2.450422 |

The candidate improves native medians 3.0–6.5%, but large cases remain
23.1–45.7% slower than stock. Identity medians change from a 1.4% reduction to
a 1.8% increase. Kernel savings do not establish whole-job acceleration;
production acceptance remains unresolved.

[Profile frames](fixed-to-variable-profile-frames-2026-10-04.csv) come from four
validated 45-second CPU/allocation recordings after two warmups, comparing
stock and candidate on nullable 20-million-row BINARY(256) casts with identical
resources. Native exit conversion appears in 34.7% of CPU samples, writer
creation in 6.8%, buffer zeroing in 6.6% and JNI in 3.1%. Native allocation
profiles attribute 32.7% of sampled weights to host binary-row copying and
19.7% to variable-binary getters. Frames are inclusive and overlap; do not sum
them. Allocation weights are sampled estimates, not copied bytes. Totals are
not normalized by completed job count. Each recording has positive relevant
events, a non-skipped passing test, exact library witnesses and retained hashes.

### Host-reader projection profiles

The release/mimalloc host-reader smoke run passes all **192 fixtures**, including
144 new projection cases. Requests cover the integer column alone, nullable UTF-8
alone, both columns, and reversed column order. Each decode compares the complete
output against an independent projection of the original input batch.

The matrix retains three row counts (16, 1,024, 16,384), two payload widths (8, 264),
memory and local-file inputs, two output batch sizes (64, 4,096), and open/close
versus decode. [All 192 allocation probes](parquet-host-reader-projection-allocation-2026-10-04.csv)
and [192 callback witnesses](parquet-host-reader-projection-callbacks-2026-10-04.csv)
are retained; every case executes positive Java read-call and byte-count witnesses.
The linked records measure Rust allocation requests and callback traffic, not
Java heap allocation or exact copied bytes. Original full-schema case names remain
stable. This smoke run validates fixtures and establishes allocation baselines;
it provides no latency or acceleration claim. Remote filesystems and nested
columns remain gaps.

### Fixed BINARY payload initialization experiment

The isolated Java prototype allocates fresh fixed-BINARY data and validity buffers
for each batch, clearing validity up front and writing every non-NULL payload in
full. Fresh ownership follows Comet's retained-batch requirement; previous batch
buffers are never reused. The initial variant left NULL payload bytes untouched.
A deterministic allocator that fills fresh buffers with `0x55` exposed those bytes
in Arrow IPC: all three widths (1, 16, 256) failed the physical-NULL-slot check,
while the existing implementation passed. Logical NULL masks alone had hidden
this difference. The revised [reviewable prototype patch](fixed-binary-initialization-candidate.patch)
clears each NULL payload slot explicitly. It is not enabled in production.

The revised variant passes 33 serialization, bridge, growth, input-reuse and
retained-buffer checks on each released Flink version (2.2.1 and 1.18.1). Fixed
BINARY SQL parity passes 20 cases on 2.2.1 and 14 on 1.18.1, with six explicit
ELT-syntax skips on the older release. The six new serialization and ownership
regression cases also pass against the unchanged main Java implementation.

Each variant was measured separately with 160 whole-job trials: 20 million rows,
NULL every seventh row, widths 16 and 256, CAST and identity controls, two warmups,
five alternating stock/native runs, and candidate/original/original/candidate
blocks. Both transposes and the rowwise blackhole remain in the executed native
plan. Runs use Java 17, Flink 2.2.1, two CPUs, a 2 GiB heap, release/mimalloc native
code, and disabled local zero-copy exchange. The same immutable cast-sharing
native library is used throughout; only Java initialization and NULL writes vary.
No heavy builds or profiles overlap these timed trials.

[All unsanitized trials](fixed-binary-initialization-wholejob-trials-2026-10-04.csv),
[all NULL-cleared trials](fixed-binary-initialization-sanitized-wholejob-trials-2026-10-04.csv),
and [pooled medians and ranges](fixed-binary-initialization-summary-2026-10-04.csv)
are retained. The safe variant's native CAST median at width 256 improves from
3.662714 s to 3.455568 s (5.66%), but remains 37.00% slower than its stock median
of 2.522272 s. Its identity control improves 6.22%; stock CAST drifts 1.60%.
At width 16, native CAST is 1.10% slower (2.507410 s to 2.535036 s), with stock
at 2.085497 s. The unsafe variant's timings do not establish performance for the
revised code. The prototype remains unaccepted: the narrow case offers no gain
and the wide case does not satisfy the stock-Flink performance gate.

### BIGINT array exit projection experiment

Adding ARRAY<BIGINT> to the generated owned binary-row exit projection passes
31 ownership and SQL checks on each released Flink version, but fails the
performance gate. With the same native INT-to-BIGINT array cast and entry writer,
the exit prototype's median is 1.645335 s, versus 1.292001 s for the previous native
exit and 1.212250 s for matched stock Flink (27.35% and 35.72% slower respectively).
[All 40 trials](array-bigint-exit-projection-trials-2026-10-04.csv) cover two million
rows, width 64, domain 65536, parent NULL every eighth row and child NULL every
seventh element, two warmups and five repetitions per engine in each of four
candidate/original/original/candidate blocks. Both transposes and the rowwise
blackhole remain measured, using Java 17, Flink 2.2.1, two CPUs, a 2 GiB heap,
release/mimalloc native code and disabled local zero-copy exchange. The same
immutable native library is used throughout, with no overlapping heavy work.
The exit extension is removed and recorded as a rejected investigation; array
widening remains experimental pending the complete stock/prior performance gate.

### INT-array bulk entry experiment

The [reviewable Java writer prototype](array-int-bulk-entry-candidate.patch)
reserves child-vector capacity once per INT array and writes directly into that
reserved range. The existing path uses a generic writer and a safe vector setter
for each element. Fresh-batch buffers, parent/child NULL masks and ownership remain
unchanged. Comet's Arrow writer pattern was consulted before this experiment.
The original unbounded prototype is not enabled in production. The revised
length-aware writer is documented in [bulk INT array entry](../optimizations/bulk-int-array-entry.md).
Expanded ownership, transpose, failure and SQL tests pass 45 checks on each
released Flink version (2.2.1 and 1.18.1), with no skips. The three new ownership
regressions also pass against the unchanged writer on both versions; they cover
primitive/nullable elements, declared non-null elements, nested arrays, input
mutation, growth and retained buffers across fresh batches.

[All 480 trials](array-int-bulk-entry-trials-2026-10-04.csv) and
[48 pooled engine summaries](array-int-bulk-entry-summary-2026-10-04.csv) retain
12 matched candidate/original/original/candidate experiments. Each block uses two
warmups and five repetitions per engine, Java 17, Flink 2.2.1, two CPUs, a 2 GiB
heap, release/mimalloc native code, disabled local zero-copy exchange, both
transposes and a rowwise blackhole. No heavy work overlaps timed trials. The same
immutable native library is used throughout. Entry-only comparisons vary the
three writer files while holding the experimental native array cast and exit
fixed; shipping comparisons hold the entry writer fixed and vary cast admission
between that prototype and the shipping Flink fallback.

At two million rows, width 64 and domain 65536, the nullable entry-only native
median falls from 1.480813 s to 1.181899 s (20.19%), versus matched stock 1.270637 s.
The non-NULL case improves 19.87% versus the previous native path and 9.58% versus
stock. Both width-eight entry-only cases improve, whereas width-one results vary.
A fresh nullable wide-array shipping comparison records 1.164308 s native,
1.239928 s previous fallback and 1.280440 s matched stock (6.10% and 9.07% faster).
With domain eight, nullable width 64 improves 12.14% versus shipping and 14.32%
versus stock.

The longer controls prevent broad array-cast admission. At ten million rows and
width one, native is 21.01% slower than shipping without NULLs and 21.73% slower
with NULLs; matched stock is roughly level with native, but stock controls differ
substantially between candidate and original blocks. Nullable width eight with
domain eight is 5.80% slower than shipping despite beating its matched stock
control by 3.55%. The width-zero configuration is the fixture's default
three-element array, not an empty array: that case is 11.90% slower than shipping.
The CSV preserves configured width and effective width separately. No unfavorable
trials are discarded or normalized away. Production keeps the array-cast fallback;
performance validation of the entry writer on already-supported operations
continues independently.


## Narrow primitive exit rejection

A profile-guided attempt to bypass the generated binary-row projection for up
to four primitive fields passed 39 ownership, parity and failure checks on each
released Flink version (2.2.1 and 1.18.1), but slowed all nine COSH/SINH/TANH
comparisons by 10.45–14.54% against the previous native exit. The original exit
is restored. This experiment does not admit the hyperbolic prototype.

[All 480 trials](narrow-primitive-exit-trials-2026-10-04.csv) and
[12 summaries with ranges and stock controls](narrow-primitive-exit-summary-2026-10-04.csv)
retain candidate/original/original/candidate blocks for ten million TINYINT,
nullable SMALLINT and DOUBLE rows. Each block uses two warmups and five repeats
per engine, Java 17, Flink 2.2.1, two CPUs, a 2 GiB heap, release/mimalloc, both
transposes and a rowwise blackhole. No heavy work overlaps timing. Native
alternatives use the same hyperbolic planner and immutable native library; only
the Java exit changes. The previous native exit is experimental, not the
shipping fallback. Stock controls also shift substantially between blocks; all
results remain retained without normalization or a causal claim about that shift.

For reproduction, apply the [experimental hyperbolic baseline patch](strict-hyperbolic-baseline-prototype.patch)
to commit be309d83, then the [exit and ownership-test patch](narrow-primitive-exit-prototype.patch).
Both patches apply cleanly together and pass whitespace checks. Neither patch
is production code. Profile evidence also rules out local StreamRecord reuse
as a substantial allocation improvement: its own temporary carrier is about
0.01% of weighted allocation samples, while downstream Flink ownership copies
account for most remaining record carriers. The broader floating-function
performance work remains pending.


## Typed hyperbolic conversion diagnostics

The isolated COSH/SINH/TANH prototype now has 816 Java-oracle Criterion fixtures:
96 DOUBLE/scalar cases, 360 typed explicit-cast controls and 360 typed fused CALL
cases. Types include TINYINT, SMALLINT, INTEGER, BIGINT and FLOAT; NULLs, empty
batches, sliced offsets, integer extrema and BIGINT rounding above 2^53 remain
checked. All fixtures and [816 allocation probes](hyperbolic-fusion-allocations-2026-10-04.csv)
pass with release/mimalloc; eight runtime SQL checks pass without skips on each
released Flink version, 2.2.1 and 1.18.1. Production admission is unchanged.

Fusing widening into the function removes the intermediate DOUBLE array. At
16384 non-NULL rows, each typed COSH case drops from 26 allocation requests /
263704 requested bytes to 21 / 132331. The same 131072-byte output is produced.
This measures allocation traffic, not copied bytes or peak memory. The first
paired [30-case kernel summary](hyperbolic-fusion-criterion-summary-2026-10-04.csv)
and [600 raw samples](hyperbolic-fusion-criterion-samples-2026-10-04.csv) retain
one-second warmup and measurement with 20 samples per case. TANH improves
1.23–7.42%; some COSH/SINH cases regress by up to 9.46%.

[All 1680 complete-job trials](hyperbolic-fusion-trials-2026-10-04.csv) and
[28 summaries with ranges and all stock controls](hyperbolic-fusion-summary-2026-10-04.csv)
compare fusion, the previous native cast prototype and shipping fallback in
candidate/previous/shipping/shipping/previous/candidate order. Each block has
two warmups and five repeats per engine, ten million rows, Java 17, Flink 2.2.1,
two CPUs, a 2 GiB heap, release/mimalloc, disabled local zero-copy exchange, both
transposes and a rowwise blackhole. No heavy work overlaps timing. The matrix
covers all six numeric input types, with non-NULL and nullable DOUBLE profiles.
Source/library hashes, load witnesses and all block outputs were verified.
An inherited final aggregation assertion expected 800 trials; independent
validated aggregation recovered all 1680 from completed blocks without rerunning
or discarding measurements.

Integer TANH remains 8.31–10.69% slower than shipping fallback despite modest
previous-native improvements. All three BIGINT and FLOAT functions still lose
to shipping fallback. DOUBLE small-range cases beat shipping by 7.45–10.03%,
and nullable larger-range cases by 3.86–5.78%. These gains do not justify broad
primitive admission. This experiment establishes an allocation reduction,
not completion of the floating-function performance work.

For reproduction from be309d83, apply the [baseline prototype](strict-hyperbolic-baseline-prototype.patch),
the [typed benchmark extension](strict-hyperbolic-typed-benchmarks.patch), then
the [fusion and paired-control patch](strict-hyperbolic-fusion-prototype.patch).
The patches apply together and the resulting source passes whitespace checks.
These remain experimental patches rather than shipping implementations.


## Reserved primitive entry diagnostic

The [reserved primitive entry experiment](primitive-reserved-entry-experiment.md)
preserves a rejected Comet-style bounded writer prototype. Forty checks pass
on each released Flink version. All 480 full-job trials show small, mixed
effects: TINYINT math improves 0.92–1.46%, nullable SMALLINT regresses
0.38–1.63%, and DOUBLE changes -0.73% to +0.77%. This does not resolve typed
math regressions, so production admission is unchanged.


## Hyperbolic compiler specialization

The [compiler specialization diagnostic](hyperbolic-specialization.md) retains
all 1680 full-job trials and 1200 interleaved Criterion samples. Integer TANH
kernels improve 11.92–14.11%, but complete jobs still lose to shipping by
4.69–8.84%. All BIGINT and FLOAT functions also remain slower than shipping
and stock. The 816 oracle fixtures and allocation probes and both released
Flink SQL suites pass; allocation traffic is unchanged. Production admission
remains unchanged and the broader performance goal remains pending.


## Typed NULL hyperbolic scalar coverage

A [portable benchmark extension](strict-hyperbolic-typed-null-benchmarks.patch)
adds 72 production Calc cases for typed NULL scalar inputs: all six numeric
types, COSH/SINH/TANH, and batch sizes 0/16/1024/16384. Each case uses the
planner's kind-31 typed NULL encoding with an Arrow IPC stream schema, checks
one DOUBLE output column, row count and a completely NULL result, and keeps
evaluation and output allocation/disposal inside its Criterion closure.
Inputs and encoded schemas are prepared outside measurement.

All 888 cases and [888 allocation probes](hyperbolic-typed-null-allocations-2026-10-04.csv)
pass release/mimalloc smoke validation. The new expected results follow NULL
propagation independently of the production helper; the existing numerical
fixtures retain their frozen Java 17 bit oracle. These are correctness and
allocation witnesses, not timing or complete-job speedup evidence. Apply the
extension after the baseline, typed, fusion and specialization prototype
patches described in the [specialization diagnostic](hyperbolic-specialization.md).
Production admission is unchanged.
