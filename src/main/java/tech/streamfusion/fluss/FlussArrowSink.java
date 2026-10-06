package tech.streamfusion.fluss;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.metadata.TablePath;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;

/** Append-only Arrow production through the existing Java RPC connection. */
public final class FlussArrowSink extends SinkAdapter<ArrowBatch> {
  private static final long serialVersionUID = 1L;
  private final Configuration config;
  private final TablePath path;
  private final RowType type;

  public FlussArrowSink(Configuration config, TablePath path, RowType type) {
    this.config = config;
    this.path = path;
    this.type = type;
  }

  @Override
  protected SinkWriter<ArrowBatch> createWriter(
      MailboxExecutor mailbox, SinkWriterMetricGroup metrics, int subtask) {
    try {
      return new Writer(
          new FlussArrowClient(
              config, path, ArrowConversion.toArrowSchema(type), NativeAllocator.SHARED, false));
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private static final class Writer implements SinkWriter<ArrowBatch> {
    private final FlussArrowClient client;

    Writer(FlussArrowClient client) {
      this.client = client;
    }

    @Override
    public void write(ArrowBatch batch, Context context) throws IOException {
      try (VectorSchemaRoot root = batch.root()) {
        client.appendAsync(root);
      }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
      client.flush();
    }

    @Override
    public void close() throws Exception {
      client.close();
    }
  }
}
