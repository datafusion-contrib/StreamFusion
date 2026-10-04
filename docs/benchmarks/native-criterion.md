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
For `scalar_registry`, the runner compiles the production Java classes and resolves their released
Maven dependencies. An embedded JVM keeps SQL/JSON buffer-recycler calls in the measured path; VM
startup is outside measurement. The JVM uses a 128 MiB initial and 256 MiB maximum heap, UTF-8,
and UTC. Maven setup logs and resolved classpath are retained. Set `SF_NATIVE_BENCH_CLASSPATH`
to reuse a classpath when invoking the Criterion executable directly.
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
| Persistent state | `persistent_state` | Production RocksDB event-time-sort write/read and checkpoint file creation; fixed options in `engine/benches/fixtures/rocks-options.json` |
| Registered Flink scalar functions | `scalar_registry` | Every registered numeric opcode; completeness assertion requires a fixture for new registrations; ASCII/null/Unicode profiles |
| Parameterized scalar kernels | `scalar_registry` | Decimal cast/round/truncate/arithmetic/float conversion, integer parse/format/divide, FROM_UNIXTIME, array item, literal/dynamic map lookup, random, clock, float comparison |
| JSON decode | `json_decode`, `json_codecs` | Direct production decode, projection, wide messages, historical nested Nexmark corpus |
| Raw decode | `raw_decode` | All admitted primitive/string/binary types, endianness, null bodies, slices |
| CSV decode | `csv_decode` | Quoted wide strings, nullable schema, strict/ignore-errors configuration |
| Avro decode | `avro_decode` | Bare and Confluent framing, wide strings |
| Protobuf decode | `protobuf_decode` | Direct descriptor-based decode, primitive/string fields, wide strings |
| Sink format encoding | `format_encode`, `kafka_sink` | Production JSON, CSV, raw, Avro, Confluent Avro, Protobuf; historical JSON/timestamp comparisons |
| Parquet file operations | `parquet_io` | Production encode, selected-row encode, ordinary decode, nullable wide strings |
| ORC post-decode normalization | `normalization` | CHAR trimming, string pass-through, full-range timestamp conversion |
| Historical operator experiments | `operators`, `calc_selection`, `scalar_functions` | Typed distinct, mini-batch sizes, aggregate layouts, selection strategies, DATE_FORMAT and string comparisons |

This is operation-level coverage, not exhaustive coverage of every SQL type, expression opcode,
codec option, state backend, and recovery mode. Expand profiles alongside implementation changes.
The main remaining boundaries are:

- JVM upcalls and host-owned reader callbacks: the scalar registry retains SQL/JSON recycler
  calls in an embedded JVM, but arbitrary UDF upcalls and reader callbacks still need dedicated
  workloads. Existing release
  integration harnesses remain the timing authority for those boundaries.
- Persistent stores beyond temporal sort, RocksDB restore/rescale/compaction, TTL expiry, and each
  operator's checkpoint variants need dedicated workloads; memory checkpoint probes cannot stand in
  for their disk I/O and native worker allocations.
- ORC file reading remains JVM-backed; normalization is not an ORC decoder throughput benchmark.
  Parquet fixtures currently use ordinary timestamp encoding, not the paired INT96 reader.
- Codec option/type combinations, CDC envelopes, nested encoder profiles, temporal arithmetic,
  decimal division, and parameterized scalar variants beyond the listed witnesses need additional cases. Registry completeness covers the factory, not all Calc opcodes.

`native-build`, `raw` (the deployment shim over shared raw decoding), and `integration-tests` do not
introduce an independent data-plane hot loop. Their operational implementation is measured in its
owning crate; build tooling and correctness tests are not timed as operators.
