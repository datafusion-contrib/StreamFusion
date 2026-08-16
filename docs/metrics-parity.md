# Native metric parity

This page is the source-grounded metric contract for every native runtime operator. The comparison
was made against the local Apache Flink 2.2-SNAPSHOT checkout and, where Flink has no equivalent
columnar operator, Apache DataFusion Comet. A metric is **complete** only when its name, type, and
value semantics match the reference. Merely registering the name is called out as **surface only**.

All native `ArrowBatch` operators correct Flink's standard `numRecordsIn` and `numRecordsOut`
counters from physical Arrow batches to logical rows. The correction applies on pass-through and
fan-out paths too, including mini-batch assigners, watermark assigners, shared subplans, native
shuffle partitions, Kafka serialization, Fluss, and Parquet partition routing.

## Flink peers

| Native operator | Reference operator | Reference-specific metrics | Current state |
| --- | --- | --- | --- |
| Calc and filter | Flink calc/filter operators | None beyond standard operator I/O | **Complete** |
| Expand | Flink expand | None beyond standard operator I/O | **Complete** |
| Unnest/correlate | Flink correlate | None beyond standard operator I/O | **Complete** |
| Share/union path | Flink pass-through/union | None beyond standard operator I/O | **Complete**; fan-out output counts logical rows for every emitted branch |
| Processing-time mini-batch assigner | `ProcTimeMiniBatchAssignerOperator` | `currentBatch` gauge | **Complete** |
| Row-time mini-batch assigner | `RowTimeMiniBatchAssignerOperator` | None beyond standard operator I/O | **Complete** |
| Group aggregate | map-bundle aggregate | `bundleSize`, `bundleRatio` gauges | **Complete**; size is logical buffered rows and ratio uses currently surviving keys |
| Local group aggregate | local map-bundle aggregate | `bundleSize`, `bundleRatio` gauges | **Complete** |
| Changelog normalize | map-bundle normalize | `bundleSize`, `bundleRatio` gauges | **Complete** |
| Keep-last deduplicate | map-bundle deduplicate | `bundleSize`, `bundleRatio` gauges | **Complete** |
| Non-bundled deduplicate | row-time deduplicate | `numLateRecordsDropped` counter where row-time applies | **Complete** |
| Window table function | aligned window TVF | `numNullRowTimeRecordsDropped` counter | **Complete**; null row-time rows are now dropped natively as Flink does |
| Window aggregate | `WindowAggOperator` | `numLateRecordsDropped`, `lateRecordsDroppedRate`, `watermarkLatency` | **Complete** for event time; processing-time variants expose the surface with zero late rows |
| Session window aggregate | `WindowAggOperator` | `numLateRecordsDropped`, `lateRecordsDroppedRate`, `watermarkLatency` | **Complete** for the supported session contract |
| Window rank/deduplicate | window rank operator | `numLateRecordsDropped`, `lateRecordsDroppedRate`, `watermarkLatency` | **Complete** |
| OVER aggregate | row-time OVER functions | `numLateRecordsDropped` | **Complete** for row-time; Flink does not add the window rate/latency metrics here |
| Updating regular join | `MiniBatchStreamingJoinOperator` | `leftBundleReducedSize`, `rightBundleReducedSize` | **Complete** when mini-batch is enabled; gauges report input transitions removed by folding |
| Window join | `WindowJoinHelper` | left/right late counters, left/right late meters, `watermarkLatency` | **Complete** |
| Interval join | Flink interval join | None beyond standard operator I/O | **Complete** |
| Temporal join | Flink temporal join | None beyond standard operator I/O | **Complete** |
| Synchronous lookup join | Flink lookup join | None beyond standard operator I/O | **Complete** |
| Key-ordered async lookup join | `TableAsyncExecutionController` | `aec_inflight_size`, `aec_blocking_size`, `aec_finish_size` gauges | **Surface complete**. In-flight and finished are live; blocking is always zero because the native implementation admits one batch at a time instead of maintaining Flink's cross-batch same-key queue |
| Top-N | Flink Top-N functions | `topn.invalidTopSize`, `topn.cache.hitRate`, `topn.cache.size` | **Surface complete, value partial**: invalid-size is exact and resident size is live. Flink reports its configured/cache-shaped capacity and request hit ratio; native size currently counts resident rows and hit rate is fixed at `1.0` |
| Event-time sort | Flink temporal sort | None beyond standard operator I/O | **Complete** |
| Watermark assigner | Flink watermark assigner | None beyond standard operator I/O | **Complete** |
| Local two-phase window aggregate | Flink local window aggregate | None beyond standard operator I/O | **Complete** |
| Global two-phase window aggregate | Flink global `WindowAggOperator` | `numLateRecordsDropped`, `lateRecordsDroppedRate`, `watermarkLatency` | **Complete**; attached partials are accepted at the closing watermark, so late count remains zero |
| Native decode | Flink format deserialization | None on the table operator beyond connector and standard I/O metrics | **Complete** |

StreamFusion-specific mini-batch counters (`miniBatchInputRows`, bundle and flush-reason totals,
cancelled changes, physical splits, and transient bytes) remain additive. They do not replace or
rename Flink's `bundleSize` and `bundleRatio` gauges.

## Connectors

| Native connector path | Flink reference | Current state | Remaining work |
| --- | --- | --- | --- |
| Kafka source | Flink Kafka Source | Flink owns the source and exposes its complete standard and kafka-clients metric surface unchanged | **Complete** for connector metrics; native decode counters are additive |
| Kafka serialization | Flink sink serialization | Standard logical input plus native batch/row/byte/time counters | No separate Flink operator metric surface exists; the counters are additive |
| Kafka sink | Flink Kafka Sink V2 | Flink owns all delivery guarantees and exposes its standard and kafka-clients producer metrics unchanged | **Complete** for connector metrics; native serialization counters are additive |
| Fluss source | Flink Fluss source | Standard logical input, per-table-bucket `currentOffset`, and `currentFetchEventTimeLag` are live through Fluss's own `FlinkSourceReaderMetrics` shape | **Partial**: bridge the full Fluss client `FlinkMetricRegistry` catalog |

## Native-only operators and Comet analogues

| Native operator | Comet analogue | Mirrored surface | Current state |
| --- | --- | --- | --- |
| RowData to Arrow | `CometSparkToColumnarExec` | `numInputRows`, `numOutputBatches`, `conversionTime` | **Complete** |
| Arrow to RowData | `CometNativeColumnarToRowExec` | `numInputBatches`, `numOutputRows`, `convertTime` | **Complete** |
| Columnar key-group exchange | Comet native shuffle writer | `elapsed_compute`, `repart_time`, `encode_time`, `decode_time`, `spill_count`, `spilled_bytes`, `input_batches` | **Complete** for the current non-spilling exchange. Encode time is measured by the network serializer; spill metrics correctly remain zero because this path cannot spill |
| Native Parquet write | `CometNativeWriteExec` | `files_written`, `bytes_written`, `rows_written` | **Partial**: rows are live. Files and bytes are surface-only because Flink's legacy `StreamingFileSink` creates and writes part files below the bulk-writer factory without exposing an operator metric context |

The Parquet write zero-valued counters are intentional capability markers, not estimates.

## Work remaining for exact parity

1. Move the Parquet sink to a metric-aware Sink V2 writer (or add a metric context to the current
   legacy writer boundary) so part-file completion can update `files_written`, `bytes_written`, and
   the sink operator's own logical standard counters.
2. Add request/hit probes to every Top-N store and report Flink's cache-shaped size (`cached
   partitions * N`, or the configured update-fast cache size) rather than native resident rows.
3. Replace the batch-scoped async lookup admission model with Flink's key-accounting queue if exact
   blocking/finished controller behavior is required, then drive the three `aec_*` gauges from it.
4. Translate the dynamic Fluss client registry. It is a catalog rather than a
   fixed handful of operator metrics, so parity tests must compare discovered metric identifiers as
   well as values.

## Verification contract

Metric parity tests should assert metric identifier, metric kind (counter, gauge, or meter), and
value after a deterministic trace. The minimum traces are: multi-row Arrow batches for standard I/O,
count and watermark mini-batch flushes, late and null-rowtime records, left/right join lateness,
Top-N cache access, ordered async lookup completion, Parquet write, Kafka source/sink, and Fluss
split progress. Connector catalog tests need the corresponding broker/service and belong in their
integration-test modules; operator surfaces belong in the core harness tests.
