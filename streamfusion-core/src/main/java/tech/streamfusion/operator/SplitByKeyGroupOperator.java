package tech.streamfusion.operator;

import java.util.UUID;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.ColumnarShuffleOutput;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Splits each incoming Arrow batch for a keyed shuffle. Aligned-only jobs emit one sub-batch per
 * destination channel. Unaligned-enabled jobs emit one independently recoverable fragment per key
 * group with the metadata {@link OrderedKeyGroupReassembler} needs to restore parent order.
 */
public class SplitByKeyGroupOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final int[] timestampPrecisions;
  private final int[] compactTimestampColumns;
  private int maxParallelism;
  private final boolean recoverable;
  private final boolean projectedKeys;
  private final boolean batch;

  private transient int parallelism;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handleOwner;
  private transient Counter elapsedCompute;
  private transient Counter repartTime;
  private transient Counter encodeTime;
  private transient Counter decodeTime;
  private transient Counter inputBatches;
  private transient long parentEpochHigh;
  private transient long parentEpochLow;
  private transient long parentSequence;

  public SplitByKeyGroupOperator(int[] keyColumns, int[] timestampPrecisions, int maxParallelism) {
    this(keyColumns, timestampPrecisions, maxParallelism, false);
  }

  public SplitByKeyGroupOperator(
      int[] keyColumns, int[] timestampPrecisions, int maxParallelism, boolean recoverable) {
    this(keyColumns, timestampPrecisions, maxParallelism, recoverable, new int[0]);
  }

  public SplitByKeyGroupOperator(
      int[] keyColumns,
      int[] timestampPrecisions,
      int maxParallelism,
      boolean recoverable,
      int[] compactTimestampColumns) {
    this(
        keyColumns,
        timestampPrecisions,
        maxParallelism,
        recoverable,
        compactTimestampColumns,
        false);
  }

  public SplitByKeyGroupOperator(
      int[] keyColumns,
      int[] timestampPrecisions,
      int maxParallelism,
      boolean recoverable,
      int[] compactTimestampColumns,
      boolean projectedKeys) {
    this(
        keyColumns,
        timestampPrecisions,
        maxParallelism,
        recoverable,
        compactTimestampColumns,
        projectedKeys,
        false);
  }

  public SplitByKeyGroupOperator(
      int[] keyColumns,
      int[] timestampPrecisions,
      int maxParallelism,
      boolean recoverable,
      int[] compactTimestampColumns,
      boolean projectedKeys,
      boolean batch) {
    if (batch && recoverable) throw new IllegalArgumentException("Batch shuffle uses stage replay");
    this.batch = batch;
    this.keyColumns = keyColumns;
    this.timestampPrecisions = timestampPrecisions;
    this.compactTimestampColumns = compactTimestampColumns;
    this.maxParallelism = maxParallelism;
    this.recoverable = recoverable;
    this.projectedKeys = projectedKeys;
  }

  @Override
  public void open() throws Exception {
    super.open();
    int[] layout =
        ColumnarShuffleOutput.layout(
            this, maxParallelism, recoverable, keyColumns.length == 0, batch);
    parallelism = layout[0];
    maxParallelism = layout[1];
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    handleOwner = ArrowBatchHandles.newOwner();
    long owner = handleOwner;
    getRuntimeContext()
        .registerUserCodeClassLoaderReleaseHookIfAbsent(
            "streamfusion-arrow-handles-" + owner, () -> ArrowBatchHandles.forgetOwner(owner));
    elapsedCompute = getMetricGroup().counter("elapsed_compute");
    repartTime = getMetricGroup().counter("repart_time");
    getMetricGroup().gauge("target_channels", () -> parallelism);
    getMetricGroup().gauge("target_key_groups", () -> maxParallelism);
    encodeTime = getMetricGroup().counter("encode_time");
    decodeTime = getMetricGroup().counter("decode_time");
    getMetricGroup().counter("spill_count");
    getMetricGroup().counter("spilled_bytes");
    inputBatches = getMetricGroup().counter("input_batches");
    UUID epoch = UUID.randomUUID();
    parentEpochHigh = epoch.getMostSignificantBits();
    parentEpochLow = epoch.getLeastSignificantBits();
    parentSequence = 0;
  }

  @Override
  public void close() throws Exception {
    if (getContainingTask().isCanceled() || getContainingTask().isFailing()) {
      ArrowBatchHandles.releaseOwner(handleOwner);
    }
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    long computeStarted = System.nanoTime();
    inputBatches.inc();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    if (compactTimestampColumns.length != 0 && in.getRowCount() != 0)
      in = copyCompactTimestamps(in);
    if (!recoverable && parallelism == 1) {
      try {
        if (in.getRowCount() == 0) {
          in.close();
        } else {
          // Any key group routes to the sole channel. Aligned recovery restores keyed state,
          // whereas unaligned recovery still needs the per-key-group fragments below.
          ColumnarRecordMetrics.emit(
              output, getMetricGroup(), new ArrowBatch(in, 0, handleOwner, encodeTime::inc));
        }
      } finally {
        elapsedCompute.inc(System.nanoTime() - computeStarted);
      }
      return;
    }
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    long handle;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      long repartStarted = System.nanoTime();
      handle =
          projectedKeys
              ? Native.splitByProjectedKey(
                  inArray.memoryAddress(),
                  inSchema.memoryAddress(),
                  keyColumns,
                  timestampPrecisions,
                  maxParallelism,
                  parallelism,
                  recoverable)
              : Native.splitByKey(
                  inArray.memoryAddress(),
                  inSchema.memoryAddress(),
                  keyColumns,
                  timestampPrecisions,
                  maxParallelism,
                  parallelism,
                  recoverable);
      repartTime.inc(System.nanoTime() - repartStarted);
    } finally {
      in.close(); // the input batch is consumed by the split
    }
    try {
      long sequence = parentSequence++;
      while (true) {
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          int keyGroup =
              Native.nextSplit(handle, outArray.memoryAddress(), outSchema.memoryAddress());
          if (keyGroup < 0) {
            break;
          }
          long decodeStarted = System.nanoTime();
          VectorSchemaRoot sub =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
          decodeTime.inc(System.nanoTime() - decodeStarted);
          ColumnarRecordMetrics.emit(
              output,
              getMetricGroup(),
              recoverable
                  ? new ArrowBatch(
                      sub,
                      keyGroup,
                      handleOwner,
                      encodeTime::inc,
                      parentEpochHigh,
                      parentEpochLow,
                      sequence,
                      Native.currentSplitOrdinals(handle),
                      Native.currentSplitKeyGroups(handle))
                  : new ArrowBatch(sub, keyGroup, handleOwner, encodeTime::inc));
        }
      }
    } finally {
      Native.closeSplit(handle);
      elapsedCompute.inc(System.nanoTime() - computeStarted);
    }
  }

  private VectorSchemaRoot copyCompactTimestamps(VectorSchemaRoot in) {
    BufferAllocator inAllocator = in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.copyCompactTimestampFields(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          compactTimestampColumns);
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close();
    }
  }
}
