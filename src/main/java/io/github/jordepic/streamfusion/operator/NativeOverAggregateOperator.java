package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar event-time {@code OVER (… ORDER BY rt RANGE UNBOUNDED PRECEDING)} aggregation: Arrow in,
 * Arrow out. Each input batch is buffered natively; on a watermark the native aggregator emits the
 * rows it has completed (rowtime past the watermark) with the running aggregate column(s) appended,
 * the input columns passing through — so the data stays columnar end to end. The buffering, the
 * per-key running fold, and the late-data drop all live in the native operator; this layer only
 * moves batches across the bridge and owns the handle's checkpointed state.
 */
public class NativeOverAggregateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;
  private final boolean proctime;
  private final int[] keyTimestampPrecisions;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeOverAggregateOperator(
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset,
      boolean proctime,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
    this.proctime = proctime;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native OVER state requires a positive max parallelism");
    }
    this.maxParallelism = maxParallelism;
  }

  @Override
  protected boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    java.util.List<byte[]> rawSnapshots = RawKeyedState.restore(context);
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    if (!rawSnapshots.isEmpty()) {
      handle =
          Native.restoreOverAggregatorPartitions(
              valueTypes,
              aggregateKinds,
              timeColumn,
              valueColumns,
              keyColumns,
              frameKind,
              frameOffset,
              proctime,
              rawSnapshots.toArray(new byte[0][]),
              memoryBudget.bytes());
    } else {
      handle =
          Native.createOverAggregator(
                valueTypes,
                aggregateKinds,
                timeColumn,
                valueColumns,
                keyColumns,
                frameKind,
                frameOffset,
                proctime,
              memoryBudget.bytes());
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (proctime) {
        // Proctime: fold in arrival order and emit this batch's rows immediately (no watermark).
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          Native.pushProctimeOverAggregator(
              handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress());
          VectorSchemaRoot out =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
          if (out.getRowCount() > 0) {
            output.collect(new StreamRecord<>(new ArrowBatch(out)));
          } else {
            out.close();
          }
        }
      } else {
        // Rowtime: the native aggregator imports and keeps the batch (buffered until a watermark
        // completes these rows), so this side hands it off and closes its own view.
        Native.pushOverAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.overAggregatorStateBytes(handle));
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (proctime) {
      super.processWatermark(mark); // proctime emits eagerly in processElement; nothing to flush
      return;
    }
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushOverAggregator(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // nothing completed this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    RawKeyedState.snapshotPartitions(
        context,
        Native.snapshotOverAggregatorPartitions(
            handle, maxParallelism, keyTimestampPrecisions));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeOverAggregator(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
