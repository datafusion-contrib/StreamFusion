# StreamFusion

!!! note
    StreamFusion is not part of Apache Flink or Apache DataFusion.

StreamFusion runs Apache Flink SQL faster by executing supported operators natively — Rust and
Apache Arrow/DataFusion, invoked over JNI — while Flink continues to own planning, coordination,
and everything not yet supported. A query is planned by Flink as usual; every part we can
reproduce **exactly** is swapped for a native implementation, and anything else keeps running on
stock Flink. Substitution is transparent (no query rewriting) and conservative (a single
unsupported operator falls the whole query back), so a job's correctness never depends on how much
of it accelerated.

## The impact

Flink's per-record, row-at-a-time execution model spends most of its CPU on interpretation
overhead — boxed objects, virtual dispatch, one hash/compare per field — rather than the actual
computation. Moving the same operators to vectorized Rust kernels over Arrow column batches turns
that per-record overhead into per-batch overhead: on the [Nexmark benchmarks](benchmarks.md),
accelerated queries run **1.2–3× the throughput of stock Flink on the same hardware**, with some
stateful shapes over 9×. In a deployment paying for compute by the core, that throughput
translates directly into fewer task managers for the same job, or the same task managers handling
more jobs.

## How it works

- **[Connectors](connectors/index.md)** get data in and out — currently Kafka, at production
  quality, across every wire format Flink itself supports.
- **[Operators](operators/index.md)** are where the acceleration happens — the per-operator pages
  mark exactly what's native, what's partial, and what still falls back, with the precise
  condition in each case.
- **[Backends](backends/index.md)** hold state for the stateful operators — an in-memory backend
  by default, with an experimental persistent backend for durability without RocksDB's per-record
  serialization tax.
- **[Deployment](deployment.md)** and **[Configuration](configuration.md)** cover installing
  StreamFusion into a Flink cluster and the runtime flags that control it.

## Inspiration

StreamFusion is built by porting established engines rather than reinventing operators:

- **[DataFusion Comet](https://github.com/apache/datafusion-comet)** — the model for the whole
  project (a native columnar accelerator behind an unchanged SQL planner) and the reference for
  the JNI / Arrow C Data Interface bridge, off-heap memory accounting, and fallback-reason
  reporting.
- **[Arroyo](https://github.com/ArroyoSystems/arroyo)** — the streaming-operator implementations
  we port (it already runs on DataFusion); the reference for join/window/changelog logic.
- **[Apache DataFusion](https://github.com/apache/datafusion)** — the native execution and
  expression engine underneath (hash joins, aggregates, Arrow kernels).
- **[RisingWave](https://github.com/risingwavelabs/risingwave)** — the reference for changelog
  semantics and memcomparable arrow-row state encoding.
- **[Apache Flink](https://github.com/apache/flink)** — the **parity target**: every operator is a
  faithful port of Flink's own, verified for identical output by a parity harness.

## Determinism

Results are byte-identical to stock Flink for everything admitted, with one necessary exception:
an inherently non-deterministic function (`PROCTIME()`, `NOW()`, random) has no well-defined
"correct" value to match, since Flink's own output for these depends on wall-clock and execution
timing. StreamFusion uses its own reasonable implementation for these rather than chasing an
undefined target, and does not gate or refuse a query for observing one. Everything whose result
*is* deterministic — including an operator that merely orders by processing time, such as a
proctime dedup or `OVER` — still produces output identical to Flink, because that depends only on
arrival order, not the clock value.

## Related work

Three native Flink accelerators exist, all **closed source**:

- **Flash** (Alibaba Cloud) — a C++ native + SIMD vectorized engine with a custom state backend
  (ForStDB). Stateful, production-deployed at scale; claims 5–10× on streaming Nexmark, 3×+ on
  batch TPC-DS, and ~50% cost reduction across 100k+ compute units. Proprietary, on Alibaba Cloud.
  ([blog](https://www.alibabacloud.com/blog/flash-a-next-gen-vectorized-stream-processing-engine-compatible-with-apache-flink_602088))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native vectorized engine
  with a drop-in compatibility layer and a new state store. Stateful; claims 5–10× on Nexmark SQL
  and ~52% lower resource usage. Implementation undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))
- **Iron Vector** (Irontools) — the same stack as us (Rust + Arrow + DataFusion over zero-copy
  JNI, Substrait plan serialization, transparent fallback), but **stateless only** today
  (projections, filters, expressions); windows, joins, and exactly-once are described as planned.
  Claims ~97% higher throughput on a stateless ETL pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))

Where StreamFusion differs: it is **open source**, and every substitution is gated and verified
for identical results against stock Flink by a parity harness rather than asserted. It is already
native on stateful windowing, joins, and changelog processing — the hard, closed part of the field
— where Iron Vector is stateless-only; it is earlier-stage than Flash and Vera X and doesn't match
their operator breadth or published benchmarks, but its acceleration is auditable and parity-first
by construction.

## License

Licensed under the Apache License, Version 2.0
([LICENSE](https://github.com/datafusion-contrib/StreamFusion/blob/main/LICENSE) or
<https://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any
additional terms or conditions.
