package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
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
 * Append-only streaming Top-N, fed Arrow batches and emitting Arrow batches. The changelog flows
 * Arrow with no per-operator transpose; each partitioned shuffle stays columnar where the input is a
 * columnar producer, and the row↔Arrow conversion happens only at the host edges. The output batch
 * carries the changelog kind on its {@code $row_kind$} column.
 */
public class NativeColumnarTopNOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int[] keyTimestampPrecisions;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long offset;
  private final long limit;
  private final boolean outputRankNumber;
  private final boolean retracting;
  private final boolean netDiff;
  private final long miniBatchSize;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarTopNOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      boolean netDiff,
      long miniBatchSize,
      int maxParallelism) {
    this.partitionColumns = partitionColumns;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.offset = offset;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.retracting = retracting;
    this.netDiff = netDiff;
    this.miniBatchSize = miniBatchSize;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native Top-N state requires a positive max parallelism");
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
            ? Native.createTopNRanker(
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                offset,
                limit,
                outputRankNumber,
                retracting,
                netDiff,
                memoryBudget.bytes())
            : Native.restoreTopNRankerPartitions(
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                offset,
                limit,
                outputRankNumber,
                retracting,
                netDiff,
                snapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    if (netDiff) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    if (!netDiff) {
      try {
        push(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }

    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      if (rows == 0) {
        push(in);
      } else {
        int offset = 0;
        while (offset < rows) {
          boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
          int length = boundary.nextSliceLength(rows - offset);
          if (length < rows - offset) {
            miniBatchMetrics.onPhysicalBatchSplit();
          }
          if (offset == 0 && length == rows) {
            push(in);
          } else {
            try (VectorSchemaRoot slice = in.slice(offset, length)) {
              push(slice);
            }
          }
          miniBatchMetrics.onSlice(length, firstContribution);
          offset += length;
          if (boundary.onSlice(length)) {
            flushBundle(FlushReason.COUNT);
          }
        }
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  private void push(VectorSchemaRoot in) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.pushTopNRanker(
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
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (netDiff) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (netDiff) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    if (netDiff) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes = Native.topNRankerStagingBytes(handle);
    long touchedPartitions = Native.topNRankerStagedPartitions(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTopNRanker(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedPartitions, transientBytes);
      if (outputRows > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
    boundary.reset();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.topNRankerStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    int[] keyGroups =
        Native.topNRankerSnapshotKeyGroups(handle, maxParallelism, keyTimestampPrecisions);
    RawKeyedState.snapshot(
        context,
        keyGroups,
        keyGroup ->
            Native.snapshotTopNRankerKeyGroup(
                handle, keyGroup, maxParallelism, keyTimestampPrecisions));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeTopNRanker(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
