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

/**
 * Splits each incoming Arrow batch by key into per-channel sub-batches, emitting each tagged with
 * its destination channel. Paired with {@link tech.streamfusion.planner.ColumnarKeyGroupPartitioner}
 * to keep a keyed exchange columnar: this makes each emitted record homogeneous in destination, so
 * the partitioner can route a whole batch to one channel (Flink's partitioner is one record → one
 * channel). Every row with a given key goes to the same channel.
 */
public class SplitByKeyGroupOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final int[] timestampPrecisions;
  private final int maxParallelism;
  private final int numChannels;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public SplitByKeyGroupOperator(
      int[] keyColumns, int[] timestampPrecisions, int maxParallelism, int numChannels) {
    this.keyColumns = keyColumns;
    this.timestampPrecisions = timestampPrecisions;
    this.maxParallelism = maxParallelism;
    this.numChannels = numChannels;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    long handle;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      handle =
          Native.splitByKey(
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              keyColumns,
              timestampPrecisions,
              maxParallelism,
              numChannels);
    } finally {
      in.close(); // the input batch is consumed by the split
    }
    try {
      while (true) {
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          int channel =
              Native.nextSplit(handle, outArray.memoryAddress(), outSchema.memoryAddress());
          if (channel < 0) {
            break;
          }
          VectorSchemaRoot sub =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
          ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(sub, channel));
        }
      }
    } finally {
      Native.closeSplit(handle);
    }
  }
}
