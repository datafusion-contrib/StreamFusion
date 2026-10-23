# Doris sink materialization

These are complete SQL job measurements on the pinned Doris 4.1.4.1 fixture and released
Doris connector 26.3.0, Flink 2.2.1 and Java 17. Native builds use release optimization and
mimalloc. Each mode runs identical queries, data and resources at parallelism two, with two
warmups and six measured trials in rotating order. Negative trial numbers are retained warmups.
The fixture has one replica, four buckets, a 1 GiB FE heap, 3 GiB BE memory limit and 256 MiB
page cache. The test JVM has a 2 GiB heap and runs in UTC.

The source contains repeated IDs. A streaming GROUP BY counts rows per ID and produces updates
for a Unique Key sink with forced upsert materialization. Scalar payloads contain the ID, count,
string and decimal; nested payloads add a timestamp, an array with a NULL element and a ROW.
All three paths use Arrow Flight reads and Arrow Stream Load writes. Output aggregate checks
include timestamps and nested values; native plans and positive materializer record counters
are required for each native job.

- **stock:** released Flink operators and released Doris connector.
- **previous:** native Doris source and SQL operators, then the previous stock materializer/sink
  boundary. Only `streamfusion.operator.dorisSink.enabled` is disabled.
- **arrow:** native source, SQL operators, Rust materializer and Arrow writer boundary.

Timing starts before SQL submission and ends when committed Doris output becomes queryable.
It includes planning, job startup, JNI, state, conversions, network and commit/visibility costs.
A fresh MiniCluster with retained operator metrics is started before timing for each job and
closed afterwards. The JVM remains warm. These are end-to-end latency measurements, not an
isolated materializer throughput measurement; small inputs remain sensitive to startup and I/O.
The ordinary update path forwards Arrow value buffers; state ownership still encodes retained
rows and predecessor restoration gathers output.

## Results

Median seconds, with the full measured range in brackets:

| Payload / strategy | Rows | Stock Flink | Previous | Native Arrow | Stock / native | Previous / native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| scalar / LEGACY | 100,000 | 0.361 [0.265–0.390] | 0.364 [0.329–0.408] | 0.324 [0.269–0.396] | 1.11× | 1.12× |
| scalar / LEGACY | 1,000,000 | 0.764 [0.659–1.444] | 0.687 [0.614–0.983] | 0.599 [0.489–0.712] | 1.28× | 1.15× |
| scalar / LEGACY | 10,000,000 | 6.640 [5.917–7.184] | 7.315 [6.019–11.412] | 6.480 [5.942–7.345] | 1.02× | 1.13× |
| nested / LEGACY | 100,000 | 0.351 [0.326–0.502] | 0.401 [0.375–0.627] | 0.380 [0.333–0.623] | 0.92× | 1.06× |
| nested / LEGACY | 1,000,000 | 0.958 [0.909–1.030] | 1.256 [1.130–1.310] | 0.979 [0.907–1.134] | 0.98× | 1.28× |
| nested / MAP | 1,000,000 | 1.063 [0.964–1.165] | 1.595 [1.356–2.191] | 1.100 [0.960–1.427] | 0.97× | 1.45× |

The scalar 1m case establishes the clearest end-to-end gain. The 10m scalar and nested cases
have overlapping ranges; nested medians remain slightly slower than Flink. These measurements
do not establish a nested or sustained-stream speedup. All native medians improve on the previous
boundary in these runs. Startup and scheduler/GC/fixture variation remain visible in every table.
The small 100k cases are particularly sensitive to those costs.

Final controls include upstream main `88c90a34`. The host is Linux/WSL2 on a Core i7-12650H
with 16 logical CPUs and 9.7 GiB RAM. Runs use a warm JVM, rotating mode order and no periodic
checkpoints; recovery is tested separately. Raw data includes every warmup and measured trial
in [results.json](results.json) and the scenario CSVs. Native library hashes accompany each
scenario; the scalar measurement precedes a defensive empty-input-key guard that does not apply
to its primary-key query. The nested writer additionally normalizes only represented columns.
Subsequent correctness fixes canonicalize generic array/map NaN payloads and reproduce Flink's
boxed MAP-value comparison, including the host JVM's integer cache. The benchmark uses BIGINT
keys and INT arrays, so these fixes do not change its matching or normalization behavior.
These are shared-host connector measurements, not isolated operator timings.

## Reproduction

From a checkout with Docker and Java 17, build the release library and run the pinned fixture:

```sh
cargo build --manifest-path native/Cargo.toml --release --features mimalloc -p streamfusion
SF_DORIS_NATIVE_BUILD_SKIP=true \
  bin/doris-suite.sh benchmark \
  -Dsf.doris.benchmark.materialize=true \
  -Dsf.doris.benchmark.strategy=LEGACY \
  -Dsf.doris.benchmark.rows=100000,1000000,10000000 \
  -Dsf.doris.benchmark.cardinality=1000 \
  -Dsf.doris.benchmark.warmups=2 -Dsf.doris.benchmark.trials=6 \
  -Dsf.doris.benchmark.output=target/doris-materializer-benchmark/scalar
```

For nested payloads set `-Dsf.doris.benchmark.complex=true`, cardinality 128 and sizes
100000,1000000. Use the same query and resources for each strategy comparison. Keep compilation,
other tests and profiling outside the measured runs.

## Retained development measurements

All completed and interrupted measurements remain available:

- `initial-retained-metrics-oom/`: the initial shared MiniCluster retained every job's metric
  objects and exhausted its heap. Partial timings and failure are retained; the final harness
  closes a separate cluster after each job.
- `before-output-sharing/`: complete scalar measurements before forwarding incoming buffers.
  The profiling note distinguishes the previous-path JVM sample from native operator profiling.
- `before-strategy-parity/`: complete scalar measurements after output sharing but before the
  final MAP/ADAPTIVE state implementation. These are development diagnostics.

The initial nested query used an untyped SQL NULL in ARRAY and failed validation before producing
any measurements. The final query casts the NULL to INT on all Flink paths.

- `before-deferred-replacements/`: the final strategy implementation before batching candidate
  replacements. Its 10m scalar run was slower than Flink and prompted CPU/JFR inspection.
- `before-vectorized-routing/`: the first replacement batching implementation, retaining the
  per-arrival Flink binary key encoder. Its 10m run was near parity with the controls.
- `before-column-normalization/`: nested measurements with whole-batch row normalization.
- `nested-profile/`: JFR-instrumented diagnostics, kept separate from unprofiled results. The
  profile records scalar strings and decimals being materialized at a represented collection's
  writer boundary; the final writer transfers those ordinary columns.

The initial generated-collection benchmark was rejected by the existing row-sink representation
rule. SQL parity tests now validate the Doris writer's declared collection capability for
constructed arrays, rows and maps. Other columnar writers retain their existing gate.

The `scalar-profile/` directory preserves the earlier CPU/JFR diagnostic and its instrumented
trials. `nested-planner-fallback/` and `nested-planner-diagnostic/` preserve the completed control
jobs from rejected planner runs. Those incomplete comparisons do not enter the final table.
