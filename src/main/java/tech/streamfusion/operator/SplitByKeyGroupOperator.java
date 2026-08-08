package tech.streamfusion.operator;

import tech.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.metrics.Counter;

/**
 * Splits each incoming Arrow batch into per-key-group sub-batches. Paired with {@link
 * tech.streamfusion.planner.ColumnarKeyGroupPartitioner}, the stable key-group tag lets Flink route
 * the whole record both normally and while filtering channel state after unaligned-checkpoint
 * rescaling. Every emitted record is independently reroutable under a new parallelism.
 */
public class SplitByKeyGroupOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final int[] timestampPrecisions;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handleOwner;
  private transient Counter elapsedCompute;
  private transient Counter repartTime;
  private transient Counter encodeTime;
  private transient Counter decodeTime;
  private transient Counter inputBatches;

  public SplitByKeyGroupOperator(
      int[] keyColumns, int[] timestampPrecisions, int maxParallelism) {
    this.keyColumns = keyColumns;
    this.timestampPrecisions = timestampPrecisions;
    this.maxParallelism = maxParallelism;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    handleOwner = ArrowBatchHandles.newOwner();
    elapsedCompute = getMetricGroup().counter("elapsed_compute");
    repartTime = getMetricGroup().counter("repart_time");
    encodeTime = getMetricGroup().counter("encode_time");
    decodeTime = getMetricGroup().counter("decode_time");
    getMetricGroup().counter("spill_count");
    getMetricGroup().counter("spilled_bytes");
    inputBatches = getMetricGroup().counter("input_batches");
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
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    long handle;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      long repartStarted = System.nanoTime();
      handle =
          Native.splitByKey(
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              keyColumns,
              timestampPrecisions,
              maxParallelism);
      repartTime.inc(System.nanoTime() - repartStarted);
    } finally {
      in.close(); // the input batch is consumed by the split
    }
    try {
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
              new ArrowBatch(sub, keyGroup, handleOwner, encodeTime::inc));
        }
      }
    } finally {
      Native.closeSplit(handle);
      elapsedCompute.inc(System.nanoTime() - computeStarted);
    }
  }
}
