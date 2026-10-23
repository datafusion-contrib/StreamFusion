package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.state.RocksDBNativeStateSupport;

/** Ordered sink-key candidates with Arrow input/output and native checkpointed state. */
public class NativeSinkMaterializer extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final int[] keys;
  private final int[] inputUpsertKeys;
  private final long ttlMillis;

  public NativeSinkMaterializer(
      int[] keys, int[] keyPrecisions, int[] inputUpsertKeys, long ttlMillis, int maxParallelism) {
    super("sink-upsert-materialize", keyPrecisions, maxParallelism);
    this.keys = keys;
    this.inputUpsertKeys = inputUpsertKeys == null ? new int[0] : inputUpsertKeys;
    this.ttlMillis = ttlMillis;
  }

  protected int strategyMode() {
    return 0;
  }

  protected long adaptiveHighThreshold() {
    return 0;
  }

  protected long adaptiveLowThreshold() {
    return 0;
  }

  protected int[] matchTimestampPrecisions() {
    return new int[0];
  }

  @Override
  protected RocksDBNativeStateSupport resolveRocksDBState(boolean rawStateRestored) {
    return resolveRocksDB(rawStateRestored, () -> true, ttlMillis);
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return true;
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocks, byte[][] restoredPartitions) {
    return Native.createRocksDBSinkMaterializer(
        keys,
        keyTimestampPrecisions(),
        inputUpsertKeys,
        ttlMillis,
        strategyMode(),
        adaptiveHighThreshold(),
        adaptiveLowThreshold(),
        matchTimestampPrecisions(),
        memoryBudgetBytes(),
        getProcessingTimeService().getCurrentProcessingTime(),
        rocks.tableDirectory(),
        maxParallelism(),
        rocks.optionsJson(),
        rocks.sharedResourcesHandle(),
        rocks.sourceDirectories(),
        rocks.sourceSnapshotTokens(),
        rocks.keyGroupStart(),
        rocks.keyGroupEnd(),
        rocks.aligned(),
        restoredPartitions);
  }

  @Override
  protected String[] checkpointRocksDBHandle(String directory) {
    return Native.checkpointRocksDBSinkMaterializer(handle, directory);
  }

  @Override
  protected long createHandle() {
    return Native.createSinkMaterializer(
        keys,
        keyTimestampPrecisions(),
        inputUpsertKeys,
        ttlMillis,
        strategyMode(),
        adaptiveHighThreshold(),
        adaptiveLowThreshold(),
        matchTimestampPrecisions(),
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreSinkMaterializer(
        keys,
        keyTimestampPrecisions(),
        inputUpsertKeys,
        ttlMillis,
        snapshots,
        getProcessingTimeService().getCurrentProcessingTime(),
        strategyMode(),
        adaptiveHighThreshold(),
        adaptiveLowThreshold(),
        matchTimestampPrecisions(),
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotSinkMaterializer(handle, maxParallelism());
  }

  @Override
  protected void closeHandle() {
    Native.closeSinkMaterializer(handle);
  }

  @Override
  protected long stateBytesHandle() {
    return Native.sinkMaterializerStateBytes(handle);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot input = element.getValue().root();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), input.getRowCount());
    var inputAllocator =
        input.getFieldVectors().isEmpty() ? allocator : input.getVector(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inputAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inputAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(
          inputAllocator, MapListRepresentation.forExport(input), dictionaries, inArray, inSchema);
      Native.pushSinkMaterializer(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          getProcessingTimeService().getCurrentProcessingTime(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (result.getRowCount() == 0) {
        result.close();
      } else {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(result));
      }
    } finally {
      input.close();
    }
    publishStateBytes();
  }
}
