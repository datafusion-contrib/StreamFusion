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
 * Columnar append-only keep-first deduplication (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime
 * ASC) = 1`): Arrow in, Arrow out. The Arrow-batch analog of the host's insert-only {@code
 * RowTimeDeduplicateKeepFirstRowFunction}. Each input batch is buffered natively; on a watermark the
 * deduplicator emits each key's minimum-rowtime row once the watermark reaches that rowtime, and
 * drops every later row for the key. Insert-only — the watermark guarantees no smaller-rowtime row
 * can still arrive once a key's row fires. Keys are co-located by the columnar shuffle; the per-key
 * candidate state and the late-data drop live in the native deduplicator, and this layer moves
 * batches across the bridge and owns the handle's checkpointed state.
 */
public class NativeColumnarDeduplicateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int[] keyTimestampPrecisions;
  private final int rowtimeColumn;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarDeduplicateOperator(
      int[] partitionColumns, int[] keyTimestampPrecisions, int rowtimeColumn, int maxParallelism) {
    this.partitionColumns = partitionColumns;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.rowtimeColumn = rowtimeColumn;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native deduplication state requires a positive max parallelism");
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
    java.util.List<byte[]> snapshots = RawKeyedState.restore(context);
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        snapshots.isEmpty()
            ? Native.createKeepFirstDeduplicator(
                partitionColumns, keyTimestampPrecisions, rowtimeColumn, memoryBudget.bytes())
            : Native.restoreKeepFirstDeduplicatorPartitions(
                partitionColumns,
                keyTimestampPrecisions,
                rowtimeColumn,
                snapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
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
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      Native.pushKeepFirstDeduplicator(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushKeepFirstDeduplicator(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no key's first row was completed by this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.keepFirstDeduplicatorStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    RawKeyedState.snapshotPartitions(
        context,
        Native.snapshotKeepFirstDeduplicatorPartitions(
            handle, maxParallelism, keyTimestampPrecisions));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeKeepFirstDeduplicator(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
