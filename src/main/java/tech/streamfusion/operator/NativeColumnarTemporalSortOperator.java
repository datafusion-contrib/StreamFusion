package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import java.nio.ByteBuffer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar event-time sort (`ORDER BY rowtime`): Arrow in, Arrow out. The Arrow-batch analog of the
 * host's {@code RowTimeSortOperator}. Each input batch is buffered natively; on a watermark the
 * native sorter emits the rows it has completed (rowtime at or before the watermark) in ascending
 * rowtime order and keeps the rest. Insert-only — the watermark guarantees no earlier-rowtime row
 * can still arrive, so the emitted order is final. The buffering, the sort, and the watermark-driven
 * release all live in the native sorter; this layer moves batches across the bridge and owns the
 * handle's checkpointed state. On the RocksDB backend the buffered rows live in the persistent
 * store (written through on arrival) and the watermark firing is a range read; memory state travels
 * as one raw keyed-state blob.
 */
public class NativeColumnarTemporalSortOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  // Like Flink's EmptyRowDataKeySelector, temporal sort owns exactly one canonical empty key, so
  // its whole buffer lives in raw key group zero: it can move — but never split — across a rescale.
  private static final int SINGLETON_KEY_GROUP = 0;

  private final int rowtimeColumn;
  private final RowType rowType;

  public NativeColumnarTemporalSortOperator(int rowtimeColumn, RowType rowType) {
    super("temporal sort", new int[0], 1);
    this.rowtimeColumn = rowtimeColumn;
    this.rowType = rowType;
  }

  // The sort is purely watermark-driven, so every supported row shape can take the typed RocksDB
  // data plane.
  @Override
  protected boolean usesDirectRocksDBState() {
    return withRowSchema(rowType, s -> Native.rocksdbTemporalSorterSupported(s) ? 1L : 0L) != 0L;
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    return withRowSchema(
        rowType,
        s ->
            Native.createRocksDBTemporalSorter(
                rowtimeColumn, memoryBudgetBytes(), s,
                rocksdb.tableDirectory(), maxParallelism(), rocksdb.optionsJson(),
                rocksdb.sharedResourcesHandle(), rocksdb.sourceDirectories(),
                rocksdb.sourceSnapshotTokens(), rocksdb.keyGroupStart(), rocksdb.keyGroupEnd(),
                rocksdb.aligned()));
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBTemporalSorter(handle, snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected long createHandle() {
    return Native.createTemporalSorter(rowtimeColumn, memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    if (snapshots.length > 1) {
      throw new IllegalStateException(
          "temporal sort has one canonical empty key and cannot restore multiple key groups");
    }
    return Native.restoreTemporalSorter(rowtimeColumn, snapshots[0], memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    byte[] snapshot =
        directRocksDBState()
            ? Native.snapshotRocksDBTemporalSorter(handle)
            : Native.snapshotTemporalSorter(handle);
    if (snapshot.length == 0) {
      return new byte[0][];
    }
    ByteBuffer framed = ByteBuffer.allocate(Integer.BYTES + snapshot.length);
    framed.putInt(SINGLETON_KEY_GROUP);
    framed.put(snapshot);
    return new byte[][] {framed.array()};
  }

  @Override
  protected void closeHandle() {
    if (directRocksDBState()) {
      Native.closeRocksDBTemporalSorter(handle);
    } else {
      Native.closeTemporalSorter(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbTemporalSorterStateBytes(handle)
        : Native.temporalSorterStateBytes(handle);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      // The native sorter imports and keeps the batch (it buffers until a watermark releases the
      // rows), so this side hands it off and closes its own view.
      if (directRocksDBState()) {
        Native.pushRocksDBTemporalSorter(handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.pushTemporalSorter(handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (directRocksDBState()) {
        Native.flushRocksDBTemporalSorter(
            handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.flushTemporalSorter(
            handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close(); // nothing completed this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

}
