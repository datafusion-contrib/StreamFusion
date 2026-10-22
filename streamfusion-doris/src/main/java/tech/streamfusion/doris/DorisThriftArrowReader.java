package tech.streamfusion.doris;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.reader.DorisValueReader;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import tech.streamfusion.operator.NativeAllocator;

/** Keeps the stock Thrift scanner while decoding its Arrow IPC directly into the native budget. */
final class DorisThriftArrowReader extends DorisValueReader implements DorisBatchReader {
  private final Schema expected;
  private final org.apache.arrow.memory.BufferAllocator allocator;
  private final boolean ownsAllocator;
  private final Long limit;
  private long emitted;
  private ArrowStreamReader ipc;

  DorisThriftArrowReader(
      DorisSnapshotSplit split, DorisOptions options, DorisReadOptions reads, Schema expected) {
    this(split, options, reads, expected, NativeAllocator.SHARED, false);
  }

  DorisThriftArrowReader(
      DorisSnapshotSplit split,
      DorisOptions options,
      DorisReadOptions reads,
      Schema expected,
      org.apache.arrow.memory.BufferAllocator allocator,
      boolean ownsAllocator) {
    super(split.getPartitionDefinition(), options, reads);
    this.allocator = allocator;
    this.ownsAllocator = ownsAllocator;
    this.expected = expected;
    this.limit = reads.getRowLimit();
  }

  @Override
  protected boolean asyncThreadStarted() {
    // The FLIP-27 fetcher already runs off the mailbox thread; the stock worker materializes rows.
    return false;
  }

  private boolean openNext() throws IOException {
    if (ipc != null) {
      ipc.close();
      ipc = null;
    }
    if (eos.get()) return false;
    var request = new TScanNextBatchParams();
    request.setContextId(contextId);
    request.setOffset(offset);
    var response = client.getNext(request);
    eos.set(response.isEos());
    if (eos.get()) return false;
    ipc = new ArrowStreamReader(new ByteArrayInputStream(response.getRows()), allocator);
    return true;
  }

  @Override
  public Schema schema() throws IOException {
    clientLock.lock();
    try {
      return ipc != null || openNext() ? ipc.getVectorSchemaRoot().getSchema() : null;
    } finally {
      clientLock.unlock();
    }
  }

  @Override
  public VectorSchemaRoot readBatch() throws IOException {
    clientLock.lock();
    try {
      if (limit != null && emitted >= limit) return null;
      while (true) {
        if (ipc == null && !openNext()) return null;
        if (!ipc.loadNextBatch()) {
          if (!openNext()) return null;
          continue;
        }
        var source = ipc.getVectorSchemaRoot();
        offset += source.getRowCount();
        int rows = source.getRowCount();
        if (limit != null) rows = (int) Math.min(rows, limit - emitted);
        if (rows == 0) continue;
        ArrayList<Field> fields = new ArrayList<>();
        for (int i = 0; i < expected.getFields().size(); i++)
          fields.add(
              DorisArrowBuffers.layout(
                  source.getSchema().getFields().get(i), expected.getFields().get(i)));
        var result = VectorSchemaRoot.create(new Schema(fields), allocator);
        try {
          if (!fields.isEmpty()) {
            try (var batch = new VectorUnloader(source).getRecordBatch()) {
              new VectorLoader(result).load(batch);
            }
          }
          result.setRowCount(rows);
          emitted += rows;
          return result;
        } catch (Throwable failure) {
          result.close();
          throw failure;
        }
      }
    } finally {
      clientLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if (ipc != null) ipc.close();
      } finally {
        try {
          super.close();
        } finally {
          if (ownsAllocator) allocator.close();
        }
      }
    } catch (Exception failure) {
      throw new IOException("Failed to close Doris Thrift Arrow reader", failure);
    }
  }
}
