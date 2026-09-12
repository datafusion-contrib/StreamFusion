package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.utils.UriReaderFactory;
import tech.streamfusion.Native;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Keeps append batches columnar after Paimon's writer-count threshold. The stock writer owns files,
 * sequences, compaction and checkpoints; a native buffer replaces only its row spill. Existing
 * files are finished at the transition. Buffered buckets are then replayed one at a time, so only
 * one Parquet encoder needs memory while preparing a checkpoint.
 */
public final class NativeAppendSinkWrite implements StoreSinkWrite, AutoCloseable {
  private record Bucket(BinaryRow partition, int bucket) {}

  private final StoreSinkWrite delegate;
  private final String[] directories;
  private final RowType rowType;
  private final Map<Bucket, Integer> buffers = new LinkedHashMap<>();
  private CoreOptions options;
  private long handle;
  private int nextId;

  NativeAppendSinkWrite(FileStoreTable table, StoreSinkWrite delegate, String[] directories) {
    this.delegate = delegate;
    this.directories = directories;
    this.rowType = LogicalTypeConversion.toLogicalType(table.rowType());
    this.options = table.coreOptions();
  }

  static StoreSinkWrite.Provider provider(StoreSinkWrite.Provider provider) {
    return (table, user, state, io, memory, metrics) ->
        new NativeAppendSinkWrite(
            table,
            provider.provide(directTable(table), user, state, io, memory, metrics),
            io.getSpillingDirectoriesPaths());
  }

  private static FileStoreTable directTable(FileStoreTable table) {
    return table.copy(Map.of(CoreOptions.WRITE_MAX_WRITERS_TO_SPILL.key(), "2147483647"));
  }

  private AbstractFileStoreWrite<?> fileWrite() {
    return (AbstractFileStoreWrite<?>) ((StoreSinkWriteImpl) delegate).getWrite().fileStoreWrite();
  }

  /** Takes ownership of the routed batch on both success and failure. */
  public void writeBundle(BinaryRow partition, int bucket, VectorSchemaRoot root) throws Exception {
    try (root) {
      if (root.getRowCount() == 0) {
        return;
      }
      if (handle == 0) {
        Map<BinaryRow, List<Integer>> active = fileWrite().getActiveBuckets();
        boolean existing = active.getOrDefault(partition, List.of()).contains(bucket);
        int count = active.values().stream().mapToInt(List::size).sum();
        if (!existing && count >= options.writeMaxWritersToSpill()) {
          // Unlike Paimon's conversion, retain these valid files instead of decoding and rewriting
          // them. That preserves their encoding and avoids assigning their sequence numbers twice.
          for (var entry : active.entrySet()) {
            for (int activeBucket : entry.getValue()) {
              delegate.compact(entry.getKey(), activeBucket, false);
            }
          }
          handle =
              Native.createAppendBuffer(
                  directories,
                  spillCodec(options),
                  options.spillCompressOptions().zstdLevel(),
                  options.writeBufferSpillDiskSize().getBytes(),
                  spillCodec(options) == 3 ? new PaimonLzoCompressor() : null);
        }
      }
      if (handle == 0) {
        writeDirect(partition, bucket, root);
        return;
      }
      Bucket key = new Bucket(partition.copy(), bucket);
      int id = buffers.computeIfAbsent(key, ignored -> nextId++);
      BufferAllocator allocator =
          root.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        Native.appendBufferPush(handle, id, array.memoryAddress(), schema.memoryAddress());
      }
      while (Native.appendBufferBytes(handle) > options.writeBufferSize()) {
        int flushId = Native.appendBufferSpillLargest(handle);
        if (flushId >= 0) {
          Bucket flushKey =
              buffers.entrySet().stream()
                  .filter(entry -> entry.getValue() == flushId)
                  .findFirst()
                  .orElseThrow()
                  .getKey();
          flush(flushKey);
        }
      }
    }
  }

  static int spillCodec(CoreOptions options) {
    return switch (options.spillCompressOptions().compress().toLowerCase(java.util.Locale.ROOT)) {
      case "none" -> 0;
      case "zstd" -> 1;
      case "lz4" -> 2;
      case "lzo" -> 3;
      default -> throw new IllegalArgumentException("Unsupported native Arrow spill compression");
    };
  }

  private void writeDirect(BinaryRow partition, int bucket, VectorSchemaRoot root)
      throws Exception {
    ((StoreSinkWriteImpl) delegate)
        .getWrite()
        .writeBundle(partition, bucket, new ArrowBatchBundle(root, rowType));
  }

  private void flush(Bucket key) throws Exception {
    Integer id = buffers.get(key);
    if (id == null) {
      return;
    }
    while (true) {
      try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        try {
          if (!Native.appendBufferNext(handle, id, array.memoryAddress(), schema.memoryAddress())) {
            break;
          }
          try (VectorSchemaRoot root =
              VectorSchemaRoot.create(
                  Data.importSchema(
                      NativeAllocator.SHARED,
                      ArrowSchema.wrap(schema.memoryAddress()),
                      NativeAllocator.DICTIONARIES),
                  NativeAllocator.SHARED)) {
            Data.importIntoVectorSchemaRoot(
                NativeAllocator.SHARED,
                ArrowArray.wrap(array.memoryAddress()),
                root,
                NativeAllocator.DICTIONARIES);
            writeDirect(key.partition(), key.bucket(), root);
          }
        } finally {
          if (array.snapshot().release != 0) {
            array.release();
          }
          if (schema.snapshot().release != 0) {
            schema.release();
          }
        }
      }
    }
    delegate.compact(key.partition(), key.bucket(), false);
    buffers.remove(key);
  }

  @Override
  public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
      throws IOException {
    try {
      for (Bucket key : new ArrayList<>(buffers.keySet())) {
        flush(key);
      }
      nextId = 0;
      return delegate.prepareCommit(waitCompaction, checkpointId);
    } catch (Exception failure) {
      throw new IOException(failure);
    }
  }

  @Override
  public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
    flush(new Bucket(partition, bucket));
    delegate.compact(partition, bucket, fullCompaction);
  }

  @Override
  public void setWriteRestore(WriteRestore restore) {
    delegate.setWriteRestore(restore);
  }

  @Override
  public void setBlobDescriptorReaderFactory(UriReaderFactory factory) {
    delegate.setBlobDescriptorReaderFactory(factory);
  }

  @Override
  public SinkRecord write(InternalRow row) {
    throw new UnsupportedOperationException("The native append writer requires Arrow batches");
  }

  @Override
  public SinkRecord write(InternalRow row, int bucket) {
    return write(row);
  }

  @Override
  public void notifyNewFiles(
      long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
    delegate.notifyNewFiles(snapshotId, partition, bucket, files);
  }

  @Override
  public void snapshotState() throws Exception {
    delegate.snapshotState();
  }

  @Override
  public boolean streamingMode() {
    return delegate.streamingMode();
  }

  @Override
  public void replace(FileStoreTable table) throws Exception {
    if (!buffers.isEmpty()) {
      throw new IllegalStateException("Writer options must refresh after preparing a checkpoint");
    }
    delegate.replace(directTable(table));
    options = table.coreOptions();
    if (handle != 0) {
      Native.closeAppendBuffer(handle);
      handle = 0;
      handle =
          Native.createAppendBuffer(
              directories,
              spillCodec(options),
              options.spillCompressOptions().zstdLevel(),
              options.writeBufferSpillDiskSize().getBytes(),
              spillCodec(options) == 3 ? new PaimonLzoCompressor() : null);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (handle != 0) {
        Native.closeAppendBuffer(handle);
        handle = 0;
      }
      buffers.clear();
    } finally {
      delegate.close();
    }
  }
}
