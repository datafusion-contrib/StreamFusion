package tech.streamfusion.doris;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.reader.DorisFlightValueReader;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import tech.streamfusion.operator.NativeAllocator;

/** Keeps the released client's allocator alive until the last exported buffer is released. */
final class DorisArrowReader extends DorisFlightValueReader implements DorisBatchReader {
  private final AtomicInteger references = new AtomicInteger(1);
  private boolean closed;
  private final Schema expected;
  private final boolean incremental;

  DorisArrowReader(
      DorisSourceSplit split, DorisOptions options, DorisReadOptions reads, Schema expected) {
    super(split, options, reads);
    this.expected = expected;
    incremental = split instanceof org.apache.doris.flink.source.split.DorisStreamSplit;
  }

  public org.apache.arrow.vector.types.pojo.Schema schema() throws IOException {
    return org.apache.arrow.vector.types.pojo.Schema.fromJSON(
        arrowReader.getVectorSchemaRoot().getSchema().toJson());
  }

  public VectorSchemaRoot readBatch() throws IOException {
    while (arrowReader.loadNextBatch()) {
      var root = arrowReader.getVectorSchemaRoot();
      if (root.getRowCount() != 0) {
        var borrowed =
            DorisArrowBuffers.borrow(
                root,
                expected,
                NativeAllocator.SHARED,
                references::incrementAndGet,
                this::release,
                incremental);
        if (!incremental) return borrowed;
        try {
          return DorisArrowChangelog.attach(root, borrowed);
        } catch (Throwable failure) {
          borrowed.close();
          throw failure;
        }
      }
    }
    return null;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      release();
    }
  }

  private void release() {
    if (references.decrementAndGet() == 0) {
      try {
        super.close();
      } catch (Exception failure) {
        throw new IllegalStateException("Failed to close Doris Flight reader", failure);
      }
    }
  }
}
