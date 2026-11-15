package tech.streamfusion.paimon;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.Native;
import tech.streamfusion.operator.NativeAllocator;

/** Reuses Paimon's hash-index persistence, feeding only distinct keys from a native flush. */
final class PaimonDynamicBucketIndex {
  private final DynamicBucketIndexMaintainer maintainer;
  private final PaimonKeyValueLayout layout;
  private final int[] keyColumns;

  PaimonDynamicBucketIndex(
      FileStoreTable table, BinaryRow partition, int bucket, PaimonKeyValueLayout layout) {
    IndexFileHandler handler = table.store().newIndexFileHandler();
    maintainer =
        new DynamicBucketIndexMaintainer.Factory(handler)
            .create(
                partition,
                bucket,
                handler
                    .scanHashIndex(
                        table.snapshotManager().latestSnapshotFromFileSystem(), partition, bucket)
                    .orElse(null));
    this.layout = layout;
    this.keyColumns = IntStream.range(0, layout.keyFieldCount()).toArray();
  }

  void add(VectorSchemaRoot root) {
    BufferAllocator allocator = root.getFieldVectors().get(0).getAllocator();
    byte[][] keys;
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
      keys =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              keyColumns,
              layout.keyTimestampPrecisions,
              IntStream.range(0, root.getRowCount()).toArray());
    }
    KeyValue record = new KeyValue();
    for (byte[] key : keys) {
      maintainer.notifyNewRecord(
          record.replaceKey(PaimonPartitions.fromBytes(key, keyColumns.length)));
    }
  }

  List<IndexFileMeta> prepareCommit() {
    return maintainer.prepareCommit();
  }
}
