package tech.streamfusion.hudi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import tech.streamfusion.operator.ArrowBatch;

/** Fetched batches belong to the reader until the mailbox emitter claims them. */
final class HudiReadBatches implements AutoCloseable {
  private final Set<ArrowBatch> pending = new HashSet<>();
  private boolean closed;

  synchronized void add(ArrowBatch batch) throws IOException {
    if (closed) {
      batch.root().close();
      throw new IOException("Hudi source reader is closed");
    }
    pending.add(batch);
  }

  synchronized void claim(ArrowBatch batch) {
    if (!pending.remove(batch)) throw new IllegalStateException("Hudi batch was already released");
  }

  @Override
  public synchronized void close() throws Exception {
    closed = true;
    try {
      org.apache.flink.util.IOUtils.closeAll(
          pending.stream().map(ArrowBatch::root).collect(java.util.stream.Collectors.toList()));
    } finally {
      pending.clear();
    }
  }
}
