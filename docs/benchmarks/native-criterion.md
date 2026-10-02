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
same instrumentation. These executables do not reproduce the production mimalloc configuration.

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
| Persistent group aggregation | `persistent_state` | Mini-batch COUNT(*) and nullable Int64 SUM with UTF-8 keys; ingestion/flush, checkpoint and aligned/rebuilt single-source restore; independent continued-changelog oracle and per-key row-kind order checks |
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

Persistent tumbling SUM lifecycle fixtures are now validated. Window aggregation
with other shapes or aggregate kinds, session aggregation, updating joins and
window joins still need dedicated persistent witnesses. GROUP BY fixtures cover COUNT(*)
and SUM only; other aggregate kinds, retractions, DISTINCT views and TTL still
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
  Arbitrary UDF types and reader callbacks still need dedicated workloads. Existing release
  integration harnesses remain the whole-job timing authority for those boundaries.
- Persistent stores beyond temporal sort, keep-first deduplication, interval join, window rank,
  temporal join, rowtime running-SUM OVER, COUNT(*)/SUM GROUP BY and tumbling SUM
  need dedicated workloads. Partial-firing recovery is covered for the running-SUM
  OVER and tumbling SUM fixtures described below; other aggregate/window shapes,
  multi-source RocksDB rescale/compaction, TTL migration/compaction and unlisted
  checkpoint variants remain unmeasured. Memory checkpoint probes cannot stand in
  for disk I/O and native worker allocations.
- ORC file reading remains JVM-backed; normalization is not an ORC decoder throughput benchmark.
  Parquet INT96 fixtures cover paired decoding from an in-memory file; host filesystem callbacks
  and nested timestamp profiles remain unmeasured.
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
are retained. The full 1,728 untimed fixtures ran before these selected timings;
source hashes stayed unchanged throughout the run. These baseline results
establish no speedup.

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
