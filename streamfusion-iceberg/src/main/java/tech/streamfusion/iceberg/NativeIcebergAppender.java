package tech.streamfusion.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.ParquetUtil;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Keeps Java's accepted-row prefixes in Arrow until the native file encoder consumes them. */
final class NativeIcebergAppender implements FileAppender<RowData>, ArrowWriteContext.PendingWrite {
  private final ArrowWriteContext context;
  private final OutputFile file;
  private final String schema;
  private final IcebergParquetSettings settings;
  private final MetricsConfig metricsConfig;
  private int[] selected = new int[1024];
  private int count;
  private long handle;
  private long memoryOwner;
  private long heldBytes;
  private PositionOutputStream output;
  private Metrics metrics;
  private List<Long> splitOffsets;
  private long finalSize;
  private boolean closed;

  NativeIcebergAppender(
      ArrowWriteContext context,
      OutputFile file,
      String schema,
      IcebergParquetSettings settings,
      MetricsConfig metricsConfig) {
    this.context = context;
    this.file = file;
    this.schema = schema;
    this.settings = settings;
    this.metricsConfig = metricsConfig;
  }

  @Override
  public void add(RowData row) {
    if (closed) throw new IllegalStateException("Iceberg appender closed");
    if (count == selected.length) selected = Arrays.copyOf(selected, count * 2);
    selected[count++] = context.position(row);
    if (count == 1) context.pending(this);
    if (count >= 8192) {
      try {
        drain();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public void drain() throws IOException {
    if (count == 0) return;
    if (closed) throw new IllegalStateException("Iceberg appender closed with pending rows");
    var root = context.root();
    var allocator = root.getVector(0).getAllocator();
    if (handle == 0) {
      if (memoryOwner == 0) {
        memoryOwner = TaskOffHeapMemory.registerOwner("iceberg-parquet", file.location());
        account(1 << 20);
      }
      output = file.create();
      try (var exported = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, root.getSchema(), NativeAllocator.DICTIONARIES, exported);
        handle =
            NativeIceberg.createWriter(
                exported.memoryAddress(),
                schema,
                settings.json(),
                file.location(),
                output,
                new byte[1 << 20]);
      } catch (Throwable failure) {
        try {
          closeOutput();
        } catch (Throwable close) {
          failure.addSuppressed(close);
        }
        throw failure;
      }
    }
    account(NativeIceberg.write(handle, context.nativeBatch(handle), selected, count));
    count = 0;
  }

  @Override
  public long length() {
    if (closed) return finalSize;
    try {
      drain();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return handle == 0 ? 0 : NativeIceberg.estimatedBytes(handle);
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    try {
      drain();
      if (handle == 0) {
        metrics = new Metrics(0);
        splitOffsets = List.of();
        return;
      }
      JsonNode result = IcebergJson.mapper().readTree(NativeIceberg.finish(handle));
      output.flush();
      finalSize = output.getPos();
      closeOutput();
      if (result.get("size").asLong() != finalSize)
        throw new IOException("Iceberg native file size mismatch");
      Metrics footer = ParquetUtil.fileMetrics(file.toInputFile(), metricsConfig);
      if (result.get("row_count").asLong() != footer.recordCount())
        throw new IOException("Iceberg native row count mismatch");
      Map<Integer, Long> nanCounts = new HashMap<>();
      result
          .get("nan_counts")
          .fields()
          .forEachRemaining(
              entry -> {
                int id = Integer.parseInt(entry.getKey());
                if (metricsConfig.columnMode(id) != MetricsModes.None.get())
                  nanCounts.put(id, entry.getValue().asLong());
              });
      metrics =
          new Metrics(
              footer.recordCount(),
              footer.columnSizes(),
              footer.valueCounts(),
              footer.nullValueCounts(),
              nanCounts,
              footer.lowerBounds(),
              footer.upperBounds());
      var offsets = new java.util.ArrayList<Long>();
      result.get("split_offsets").forEach(offset -> offsets.add(offset.asLong()));
      splitOffsets = List.copyOf(offsets);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        closeOutput();
      } catch (Throwable close) {
        failure.addSuppressed(close);
      }
      throw failure;
    } finally {
      closed = true;
      try {
        if (handle != 0) {
          long current = handle;
          handle = 0;
          NativeIceberg.closeWriter(current);
        }
      } finally {
        releaseMemory();
      }
    }
  }

  private void account(long bytes) {
    if (bytes < 0) throw new IllegalStateException("Negative Iceberg encoder memory");
    long delta = bytes - heldBytes;
    if (delta > 0 && !TaskOffHeapMemory.tryReserve(memoryOwner, delta))
      throw TaskOffHeapMemory.exhausted("Iceberg Parquet encoder", delta);
    if (delta < 0) TaskOffHeapMemory.release(memoryOwner, -delta);
    heldBytes = bytes;
  }

  private void releaseMemory() {
    if (memoryOwner != 0) TaskOffHeapMemory.closeOwner(memoryOwner);
    memoryOwner = 0;
    heldBytes = 0;
  }

  private void closeOutput() throws IOException {
    PositionOutputStream current = output;
    output = null;
    if (current != null) current.close();
  }

  @Override
  public void abort() throws IOException {
    if (closed) return;
    closed = true;
    count = 0;
    metrics = new Metrics(0);
    splitOffsets = List.of();
    try {
      if (handle != 0) {
        long current = handle;
        handle = 0;
        NativeIceberg.closeWriter(current);
      }
    } finally {
      releaseMemory();
      closeOutput();
    }
  }

  @Override
  public Metrics metrics() {
    if (metrics == null)
      throw new IllegalStateException("Iceberg file metrics unavailable before close");
    return metrics;
  }

  @Override
  public List<Long> splitOffsets() {
    return splitOffsets;
  }
}
