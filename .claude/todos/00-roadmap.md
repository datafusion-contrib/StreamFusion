# Roadmap / backlog index

Read this first. It is the ordered map of what is done and what remains; each item points to a
detailed ticket. Keep it in step as tickets are completed or added; delete a ticket's pointer
here when the ticket is deleted.

## Where we are (done + parity-verified)
- **Operators, native + identical to Flink:** filter/`WHERE` (via the native expression engine),
  tumbling / hopping / session / cumulative window aggregates (one- and two-phase), Parquet sink
  (exactly-once), Parquet source. Compatibility chart in `readme.md` is the source of truth.
- **Columnar-flow mechanism (ticket 21):** `ArrowBatch` stream type + IPC serializer, the two
  transpose operators, the transition-inserter pass, `ColumnarInput`/`ColumnarOutput` markers.
  **Columnar today:** filter (in+out), Parquet sink (in), Parquet source (out). **Not yet
  columnar:** the window operators (gated on the shuffle — see below).
- **Profiling/benchmarks (ticket 20):** Criterion native micro-benches; `ThroughputBenchmark`
  vs Flink; the `bench` Maven profile (release native — mandatory for benchmarks).
- **Release benchmarks vs Flink:** Parquet copy (columnar→columnar) 3.19×, tumbling 1.21×,
  Parquet sink (row source) 1.05×, bare filter 0.83×.

## Next, roughly in order
1. **Columnar windows end to end** (tickets 10, 21). **Built + tested in isolation:** the columnar
   shuffle mechanism (native by-key split + `SplitByKeyGroupOperator` + `ColumnarKeyGroupPartitioner`)
   AND the columnar window operator (`NativeColumnarWindowAggregateOperator`). The hash is a plain
   consistent hash (no Flink-hash reproduction — it only distributes work; the keyed consumer is our
   own native operator). **Blocked on columnar watermarks:** the planner wiring to connect them
   (drop the keyed exchange, route the window through the columnar operator) can't trigger because a
   windowed query's watermark arrives via a *rowwise* `WatermarkAssignerOperator` (transposes the
   columnar source back to rows). Flink's filesystem source does **not** emit watermarks (verified:
   no `SupportsWatermarkPushDown`), so the Flink-faithful fix is a **columnar watermark-assigner**
   (the Arrow→Arrow analog of Flink's operator), not source-emitted watermarks (ticket 10). Deferred.
2. **Parquet sink file coalescing** (ticket 22). The sink writes one file per batch; buffer and
   roll by size/row-count so output file count is independent of read-batch size. Independent of
   the shuffle; improves output quality and throughput.
3. **Expression layer stages 2–3** (ticket 19): general/computed projection (unblocks the
   constant-folded `=` filter), fuse projection+filter, widen the op set; plus the residual
   narrow-int (`TINYINT`/`SMALLINT`) arithmetic-overflow parity check.
4. **Wider value/key types** (ticket 04): SMALLINT/TINYINT/FLOAT `SUM`/`AVG`, DECIMAL/TIMESTAMP
   grouping keys, multiple value columns, `COUNT(*)`.
5. **Richer columnar endpoints** (ticket 24): beyond local Parquet — Iceberg and remote
   filesystems (`hdfs:`/`s3:`) for the native source/sink; currently `file:` only. **Deferred by
   direction until generalized operator support lands** — broaden what we can run (items 3–4 and the
   ticket 11 operators) before broadening where we read/write.
6. **Operator-level perf** (ticket 20 backlog): Java column-vectorized whole-row converter (the
   transpose is cell-at-a-time), per-row `GroupKey` allocation in aggregators, session `update`
   one-row `take` batching.
7. **Acceleration policy** (ticket 09): now that lone stateless islands measure < 1× (bare filter
   0.83×), decide whether to refuse substituting an operator that would be an isolated island with
   row endpoints on both sides, vs always substitute. Benchmark-informed.

## Production-readiness (not yet load-bearing)
- **Memory accounting** (ticket 05): native `RootAllocator`s per operator are not accounted
  against Flink's `MemoryManager`.
- **Mailbox threading** (ticket 01): native execution should integrate with the task mailbox
  (non-blocking), not block the task thread.
- **Changelog / retract** (ticket 06): only insert-only streams are native today.

## Breadth / longer horizon
- **Two-phase cumulative windows** (ticket 13).
- **Arroyo operator coverage tracker** (ticket 11): joins, OVER/window functions, non-windowed
  GROUP BY, etc.
- **Fully native Kafka source, no JNI** (back burner, noted in ticket 21): subscribe in Rust,
  decode Avro→Arrow, only if the connector semantics can be lifted from Arroyo.
