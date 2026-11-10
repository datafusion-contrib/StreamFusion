package tech.streamfusion.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.Counter;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/**
 * The existing native decoder under Flink's file discovery, fetch queue and checkpoint lifecycle.
 */
public final class NativeParquetBulkFormat
    implements BulkFormat<ArrowBatch, FileSourceSplit>, AutoCloseable {
  private final byte[] schema;
  private final String[] names;
  private final int batchRows;
  private final transient Counter rows;
  private final transient Set<BatchIterator> pending = ConcurrentHashMap.newKeySet();
  private transient boolean closed;

  public NativeParquetBulkFormat(byte[] schema, String[] names, int batchRows, Counter rows) {
    if (batchRows <= 0) throw new IllegalArgumentException("Positive Parquet batch size required");
    this.schema = schema.clone();
    this.names = names.clone();
    this.batchRows = batchRows;
    this.rows = rows;
  }

  private Object readResolve() {
    return new NativeParquetBulkFormat(schema, names, batchRows, null);
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public TypeInformation<ArrowBatch> getProducedType() {
    return ArrowBatchTypeInformation.INSTANCE;
  }

  @Override
  public Reader<ArrowBatch> createReader(Configuration config, FileSourceSplit split)
      throws IOException {
    return new FileReader(split, 0);
  }

  @Override
  public Reader<ArrowBatch> restoreReader(Configuration config, FileSourceSplit split)
      throws IOException {
    var position = split.getReaderPosition().orElseThrow();
    if (position.getOffset() != CheckpointedPosition.NO_OFFSET)
      throw new IOException("Native Parquet restores whole-file logical row positions only");
    return new FileReader(split, position.getRecordsAfterOffset());
  }

  /** Public because the JNI decoder invokes readFully on its host-owned input. */
  public final class FileReader implements Reader<ArrowBatch> {
    private final FSDataInputStream input;
    private final Schema output = Schema.deserializeMessage(ByteBuffer.wrap(schema));
    private final byte[] chunk = new byte[64 * 1024];
    private long handle;
    private TaskOffHeapMemory.Reservation memory;
    private long position;
    private long skip;

    FileReader(FileSourceSplit split, long skip) throws IOException {
      if (split.offset() != 0 || split.length() != split.fileSize())
        throw new IOException("Native Parquet requires a whole-file split");
      this.skip = skip;
      input = split.path().getFileSystem().open(split.path());
      try (ArrowSchema exported = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportSchema(NativeAllocator.SHARED, output, NativeAllocator.DICTIONARIES, exported);
        handle =
            NativeParquet.createParquetDecoder(
                this, split.fileSize(), exported.memoryAddress(), names, batchRows);
        // Metadata is read first; reserve decoder workspace before any row group is decoded.
        memory =
            TaskOffHeapMemory.reserve(
                "parquet",
                split.path().toString(),
                NativeParquet.parquetDecoderMaxRowGroupBytes(handle));
      } catch (Throwable failure) {
        try {
          close();
        } catch (Throwable closing) {
          failure.addSuppressed(closing);
        }
        throw failure;
      }
    }

    public void readFully(long offset, ByteBuffer target) throws IOException {
      input.seek(offset);
      while (target.hasRemaining()) {
        int count = input.read(chunk, 0, Math.min(chunk.length, target.remaining()));
        if (count < 0) throw new EOFException("Truncated Parquet file");
        if (count == 0) throw new IOException("Parquet input made no progress");
        target.put(chunk, 0, count);
      }
    }

    @Override
    public RecordIterator<ArrowBatch> readBatch() throws IOException {
      if (handle == 0) throw new IOException("Parquet reader is closed");
      for (; ; ) {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedIOException();
        VectorSchemaRoot root = next();
        if (root == null) {
          if (skip != 0) throw new IOException("Parquet checkpoint exceeds file row count");
          return null;
        }
        int count = root.getRowCount();
        position = Math.addExact(position, count);
        if (skip >= count) {
          skip -= count;
          root.close();
          continue;
        }
        if (skip != 0) {
          try (VectorSchemaRoot original = root) {
            root = original.slice(Math.toIntExact(skip), count - Math.toIntExact(skip));
          }
          skip = 0;
        }
        synchronized (pending) {
          if (closed) {
            root.close();
            throw new InterruptedIOException("Parquet source closed");
          }
          var batch = new BatchIterator(new ArrowBatch(root), position);
          pending.add(batch);
          return batch;
        }
      }
    }

    private VectorSchemaRoot next() {
      try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          ArrowSchema exported = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        try {
          if (!NativeParquet.parquetDecoderNext(
              handle, array.memoryAddress(), exported.memoryAddress())) return null;
          VectorSchemaRoot root = VectorSchemaRoot.create(output, NativeAllocator.SHARED);
          try {
            Data.importIntoVectorSchemaRoot(
                NativeAllocator.SHARED,
                ArrowArray.wrap(array.memoryAddress()),
                root,
                NativeAllocator.DICTIONARIES);
            return root;
          } catch (Throwable failure) {
            root.close();
            throw failure;
          }
        } finally {
          if (array.snapshot().release != 0) array.release();
          if (exported.snapshot().release != 0) exported.release();
        }
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (handle != 0) {
          long closing = handle;
          handle = 0;
          NativeParquet.closeParquetDecoder(closing);
        }
      } finally {
        try {
          if (memory != null) {
            memory.close();
            memory = null;
          }
        } finally {
          input.close();
        }
      }
    }
  }

  private final class BatchIterator implements RecordIterator<ArrowBatch> {
    private ArrowBatch batch;
    private final long position;

    BatchIterator(ArrowBatch batch, long position) {
      this.batch = batch;
      this.position = position;
    }

    @Override
    public synchronized RecordAndPosition<ArrowBatch> next() {
      if (batch == null) return null;
      var record = new MutableRecordAndPosition<ArrowBatch>();
      record.set(batch, CheckpointedPosition.NO_OFFSET, position);
      if (rows != null) rows.inc(batch.rowCount());
      batch = null;
      pending.remove(this);
      return record;
    }

    @Override
    public synchronized void releaseBatch() {
      if (batch != null) {
        batch.root().close();
        batch = null;
      }
      pending.remove(this);
    }
  }

  @Override
  public void close() {
    List<BatchIterator> remaining;
    synchronized (pending) {
      closed = true;
      remaining = new ArrayList<>(pending);
    }
    for (var batch : remaining) batch.releaseBatch();
  }
}
