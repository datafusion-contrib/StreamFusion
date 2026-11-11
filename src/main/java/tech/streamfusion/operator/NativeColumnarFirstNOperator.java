package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.state.RocksDBNativeStateSupport;

/** Arrival-ordered first-N with one native counter per partition and no retained payload rows. */
public class NativeColumnarFirstNOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final int[] partitionColumns;
  private final int limit;
  private final boolean outputRank;
  private final long ttlMillis;

  public NativeColumnarFirstNOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int limit,
      boolean outputRank,
      long ttlMillis,
      int maxParallelism) {
    super("first-n", keyTimestampPrecisions, maxParallelism);
    this.partitionColumns = partitionColumns;
    this.limit = limit;
    this.outputRank = outputRank;
    this.ttlMillis = ttlMillis;
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return true;
  }

  @Override
  protected RocksDBNativeStateSupport resolveRocksDBState(boolean rawStateRestored) {
    return resolveRocksDB(rawStateRestored, () -> true, ttlMillis);
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb, byte[][] restored) {
    return Native.createRocksDBFirstN(
        partitionColumns,
        keyTimestampPrecisions(),
        limit,
        outputRank,
        ttlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        memoryBudgetBytes(),
        rocksdb.tableDirectory(),
        maxParallelism(),
        rocksdb.optionsJson(),
        rocksdb.sharedResourcesHandle(),
        rocksdb.sourceDirectories(),
        rocksdb.sourceSnapshotTokens(),
        rocksdb.keyGroupStart(),
        rocksdb.keyGroupEnd(),
        rocksdb.aligned(),
        restored);
  }

  @Override
  protected long createHandle() {
    return restoreRawHandle(new byte[0][]);
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.createFirstN(
        partitionColumns,
        keyTimestampPrecisions(),
        limit,
        outputRank,
        ttlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotFirstNPartitions(handle, maxParallelism());
  }

  @Override
  protected String[] checkpointRocksDBHandle(String directory) {
    return Native.checkpointRocksDBFirstN(handle, directory);
  }

  @Override
  protected long stateBytesHandle() {
    return Native.firstNStateBytes(handle);
  }

  @Override
  protected java.util.function.LongConsumer memoryStateCopier() {
    final long source = handle;
    return target -> Native.copyFirstNCanonicalMemoryState(source, target);
  }

  @Override
  protected void closeHandle() {
    closeCapturedHandle(handle, directRocksDBState());
  }

  @Override
  protected void closeCapturedHandle(long handle, boolean persistent) {
    Native.closeFirstN(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    getMetricGroup().counter("numRankEndChanged");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inputAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
        ArrowSchema inputSchema = ArrowSchema.allocateNew(inputAllocator);
        ArrowArray outputArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(
          inputAllocator,
          MapListRepresentation.forExport(in),
          dictionaries,
          inputArray,
          inputSchema);
      Native.pushFirstN(
          handle,
          inputArray.memoryAddress(),
          inputSchema.memoryAddress(),
          getProcessingTimeService().getCurrentProcessingTime(),
          outputArray.memoryAddress(),
          outputSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }
}
