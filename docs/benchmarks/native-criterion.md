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
python3 bin/bench-native.py --bench handoffs --filter c_data
```

`--package` and `--bench` are repeatable. Cargo's benchmark profile is optimized; do not change it
to a debug profile when collecting timings. Criterion retains its samples and baseline data under
`native/target/criterion`. The runner writes each suite's combined output, `allocations.csv`, and
`metadata.json` under a timestamped `native/target/native-benchmarks/` directory, or `--output`.
Metadata includes commit, dirty status, toolchain, platform, commands, and relevant build settings.
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

Suites use representative batch sizes and payload widths. Extend workload profiles alongside
implementation changes; this inventory identifies the currently measured boundaries.

| Production boundary | Suite | Current witnesses |
| --- | --- | --- |
| Arrow C Data ownership | `handoffs` | Full-schema and cached-schema export/import; nullable sliced string and integer batches |
| Shared bridge transforms | `handoffs` | Timestamp unit conversion, float canonical ordering, partition splits |
| JSON decode | `json_decode`, `json_codecs` | Direct production decode, projection, wide messages, historical nested Nexmark corpus |
| Raw decode | `raw_decode` | All admitted primitive/string/binary types, endianness, null bodies, slices |
| CSV decode | `csv_decode` | Quoted wide strings, nullable schema, strict/ignore-errors configuration |
| Avro decode | `avro_decode` | Bare and Confluent framing, wide strings |
| Protobuf decode | `protobuf_decode` | Direct descriptor-based decode, primitive/string fields, wide strings |
| Sink format encoding | `format_encode`, `kafka_sink` | Production JSON, CSV, raw, Avro, Confluent Avro, Protobuf; historical JSON/timestamp comparisons |
| Parquet file operations | `parquet_io` | Production encode, selected-row encode, ordinary decode, nullable wide strings |
| ORC post-decode normalization | `normalization` | CHAR trimming, string pass-through, full-range timestamp conversion |
| Historical operator experiments | `operators`, `calc_selection`, `scalar_functions` | Typed distinct, mini-batch sizes, aggregate layouts, selection strategies, DATE_FORMAT and string comparisons |

Coverage expansion remains for operations and workload variants outside this table.
Criterion isolates the listed boundary; host-backed I/O, arbitrary UDF upcalls, other persistent
stores, recovery/rescale, TTL, and additional SQL type/codec options need dedicated workloads.
