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
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.PartitionedArrowBatch;

/** Append writer for batches already split by native partition columns. */
public final class FlussPartitionedArrowSink extends SinkAdapter<PartitionedArrowBatch> {
  private static final long serialVersionUID = 1L;
  private final Configuration config;
  private final TablePath path;
  private final RowType type;

  public FlussPartitionedArrowSink(Configuration config, TablePath path, RowType type) {
    this.config = config;
    this.path = path;
    this.type = type;
  }

  @Override
  protected SinkWriter<PartitionedArrowBatch> createWriter(
      MailboxExecutor mailbox, SinkWriterMetricGroup metrics, int subtask) {
    try {
      return new Writer(
          new FlussArrowClient(
              config, path, ArrowConversion.toArrowSchema(type), NativeAllocator.SHARED, false));
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private record Writer(FlussArrowClient client) implements SinkWriter<PartitionedArrowBatch> {
    @Override
    public void write(PartitionedArrowBatch batch, Context context) throws IOException {
      try (VectorSchemaRoot root = batch.root()) {
        client.appendAsync(root, batch.bucketId());
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
