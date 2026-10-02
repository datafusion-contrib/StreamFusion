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
| Fixed binary expressions | `binary_expressions` | Production Calc casts from string/variable/fixed binary and fixed ELT; sliced inputs, nulls, invalid indices, byte truncation/padding, widths 1/16/256 |
| Calc and column movement | `data_movement` | Compiled projection, grouping-set EXPAND, inner/left array UNNEST, Arrow IPC encode/decode |
| Stateful processing | `operator_allocations` | Filter, local/global SUM, tumble/session aggregate, running/bounded OVER, append/retract Top-N, first/last dedup, normalize, updating/interval/window joins, Paimon upsert merge |
| Further stateful processing | `data_movement`, `keys_and_checkpoints` | First-N, event-time sort, temporal join, window rank |
| Key materialization | `keys_and_checkpoints` | Arrow-row encode/decode, Flink BinaryRow hash; primitive and wide nullable string composite keys |
| Memory checkpoints | `keys_and_checkpoints`, `data_movement` | Group aggregate and append Top-N snapshot/restore, temporal-join snapshot |
| Persistent state | `persistent_state` | Production RocksDB event-time-sort write/read, checkpoint creation, aligned file adoption and clipped restore; nullable sliced wide rows, order and resumed-arrival assertions; fixed options in `engine/benches/fixtures/rocks-options.json` |
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
- Persistent stores beyond temporal sort and keep-first deduplication, multi-source RocksDB rescale/compaction, TTL migration/compaction, and each
  operator's checkpoint variants need dedicated workloads; memory checkpoint probes cannot stand in
  for their disk I/O and native worker allocations.
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
