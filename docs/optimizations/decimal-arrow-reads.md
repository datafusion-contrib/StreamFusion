# Read long-backed Arrow decimals without temporary integers

The Arrow-backed JVM decimal reader checks whether a decimal128 value's upper word
is the sign extension of its lower word. If so, the unscaled value fits a signed
long. Compact Flink decimals are constructed directly from that long; wider
declared precisions use `BigDecimal.valueOf` at the requested scale. This avoids
Arrow's temporary byte array and `BigInteger`, including for small values stored
in a precision-38 column. It follows Comet's compact decimal reader.

The check observes the actual value rather than assuming that its declared
precision proves it fits. Values outside signed-long range and big-endian hosts
retain the existing Arrow reconstruction. NULLs, requested precision/scale,
compact rounding carries and wide precision overflow keep their existing behavior.
The reader retains vector objects rather than buffer addresses, and every returned
decimal owns its scalar value independently of the Arrow batch. No JNI, collector,
lifecycle or native admission contract changes.

## One profiling pass

The bounded investigation for
[#425](https://github.com/datafusion-contrib/StreamFusion/issues/425) allows ten
minutes for a capture and its analysis, followed by one small prototype when the
profile identifies a straightforward change. The retained initial capture covers
five million input rows each for DECIMAL(18,8), DECIMAL(38,18) and TIMESTAMP_LTZ(9),
one warmup pair and one measured pair. async-profiler 4.6 samples CPU every 1 ms
and Java allocations every 512 KiB. Its elapsed times are excluded from throughput
claims.

Filtering stacks to the native table-function pipeline gives 6,706 CPU samples;
978 include the decimal reader and 776 include Arrow's BigDecimal reconstruction.
Decimal reads account for 6.60 GB of 18.30 GB sampled allocation weight. Inclusive
counts overlap, and allocation weight is not copied bytes. Large-value conversion,
temporal conversion and owned exit serialization have no equally small change
identified in this pass and retain their current paths. The previously rejected
[binary exits and lazy RowKind experiment](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/379-typed-table-function-boundaries.md)
were not repeated.

## Complete-job comparison

The comparison uses canonical main `07fbffca` and the same benchmark fixture with
symmetric EXPLAIN preflight for stock and native execution. Each implementation
runs in a separate JVM with its own alternating stock control. Linux x86-64/WSL2,
Intel i7-12650H, OpenJDK 17.0.20.1, release/mimalloc, parallelism one, 2 GiB heap,
1,024-row batches, two warmups and five measured pairs. The stateless native build
disables the RocksDB feature; both revisions use the identical library. Build and
test processes do not overlap timing. Desktop activity is uncontrolled.

Timing includes complete `executeSql`/`await` jobs, the runtime RowData source,
both row/Arrow transposes, production IPC, the synchronous JVM function and its
row blackhole sink. Inputs include NULLs, signed decimal extremes, small unscaled
values and reused mutable output rows. Each admitted run asserts the physical
table function, both transposes and consumed native table-function input.

Two million source rows, medians in seconds:

| Flink | Type | Prior SF | Updated SF | Prior stock | Updated stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 2.2.1 | DECIMAL(18,8) | 0.571593 | 0.487287 | 0.245821 | 0.234126 |
| 2.2.1 | DECIMAL(38,18) | 0.619181 | 0.561240 | 0.259654 | 0.237681 |
| 2.2.1 | TIMESTAMP_LTZ(9), unchanged control | 0.568542 | 0.564877 | 0.273373 | 0.267293 |
| 1.18.1 | DECIMAL(18,8) | 0.597716 | 0.472577 | 0.244375 | 0.240720 |
| 1.18.1 | DECIMAL(38,18) | 0.654064 | 0.568023 | 0.257856 | 0.235989 |
| 1.18.1 | TIMESTAMP_LTZ(9), unchanged control | 0.592308 | 0.572697 | 0.279760 | 0.275084 |

Flink 2.2 decimal medians decrease 14.7% and 9.4%; Flink 1.18 decreases 20.9% and
13.2%. Each updated decimal native range is entirely below its prior range.
Timestamp control ranges overlap. Separate JVM stock controls drift, limiting
causal attribution. All six native cases remain
slower than stock under the accepted UDF performance exception. These short jobs
include startup and planning and do not establish sustained kernel throughput.

[All warmups, trials, ranges and profile counters](../benchmarks/results/table-function-conversions-425-2026-10-22.json)
retain the library and reader identities. Existing exact/temporal support remains
default-enabled. The maintainer accepted closing #425 after the bounded pass found
no other simple optimization. The
[deferred work and reopening criteria](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/425-remaining-table-function-conversions.md)
retain the larger-value, temporal and owned-exit costs as measured limits.

The focused selection passes 87 checks on each released Flink line with JDK 17:
signed-long limits and adjacent wider values, requested precision/scale and NULLs,
mutable table-function fanout, LEFT/predicate forms, decimal rounding/overflow,
empty input, explicit fallback, scalar consumers and row/Arrow conversion. These
local runs exclude the typed table-function RocksDB recovery method because the
measurement library disables RocksDB. The required full CI and upstream suites
retain their normal feature-complete native builds and recovery checks.

```sh
# Build the released native core and compile the benchmark fixture.
mvn -pl streamfusion-runtime -Pbench test-compile -DskipTests \
  -Dnative.cargo.packages='-p streamfusion' \
  -Dnative.cargo.args='build --release --no-default-features --features mimalloc'

SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench surefire:test \
  -Dnative.build.skip=true -Dtest=TypedTableFunctionBenchmark \
  '-Dtablefunc.types=DECIMAL(18,8);DECIMAL(38,18);TIMESTAMP_LTZ(9)' \
  -Dtablefunc.cases=project -Dtablefunc.rows=2000000 \
  -Dtablefunc.warmup=2 -Dtablefunc.runs=5 '-Dsf.extraJvmArgs=-Xmx2g'
```

Add `flink-1.18` to both profile lists for Flink 1.18.1 and use a separate build
directory/worktree per line. Profiling uses the same single Surefire goal,
five million rows, one warmup and one measured pair, with a 600-second process
timeout and `-agentpath` pointing to async-profiler. Use
`start,event=cpu,alloc=512k,interval=1ms,file=profile-%p.jfr`; the PID-specific name
prevents an unrelated empty test fork from overwriting a completed recording.
