package tech.streamfusion.hudi;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.StreamerUtil;
import tech.streamfusion.operator.ArrowBatch;

/** Uses Hudi's fetcher and split lifecycle while decoding file payloads as Arrow. */
final class HudiSplitReader implements SplitReaderFunction<ArrowBatch>, AutoCloseable {
  private final Configuration conf;
  private final RowType output;
  private final int rows;
  private final HudiReadBatches batches;
  private transient HudiParquetReader reader;
  private transient tech.streamfusion.planner.HudiCommitFilter filter;
  private long skip;

  HudiSplitReader(Configuration conf, RowType output, int rows, HudiReadBatches batches) {
    this.conf = conf;
    this.output = output;
    this.rows = rows;
    this.batches = batches;
  }

  @Override
  public void open(HoodieSourceSplit split) {
    closeCurrentSplit();
    if (!split.getBasePath().isPresent()
        || (split.getLogPaths().isPresent() && !split.getLogPaths().get().isEmpty()))
      throw new IllegalArgumentException("Hudi native COW reads require a base-file-only split");
    try {
      var meta = StreamerUtil.metaClientForReader(conf, HadoopConfigurations.getHadoopConf(conf));
      RowType physical = output;
      if (split.getInstantRange().isPresent()) {
        var fields = new java.util.ArrayList<>(output.getFields());
        fields.add(
            new RowType.RowField(
                "_hoodie_commit_time",
                new org.apache.flink.table.types.logical.VarCharType(Integer.MAX_VALUE)));
        physical = new RowType(fields);
        filter =
            new tech.streamfusion.planner.HudiCommitFilter(
                split.getInstantRange().get(), output.getFieldNames());
      }
      reader =
          new HudiParquetReader(
              meta.getStorage(), new StoragePath(split.getBasePath().get()), physical, rows);
      skip = split.getConsumed();
    } catch (Throwable failure) {
      try {
        closeCurrentSplit();
      } catch (Throwable closing) {
        failure.addSuppressed(closing);
      }
      if (failure instanceof IOException) throw new UncheckedIOException((IOException) failure);
      if (failure instanceof Error) throw (Error) failure;
      throw (RuntimeException) failure;
    }
  }

  @Override
  public BatchRecords<ArrowBatch> readBatch(
      HoodieSourceSplit split, int batchSize, BooleanSupplier wakeup) {
    try {
      while (!wakeup.getAsBoolean()) {
        VectorSchemaRoot root = reader.next();
        if (root == null) {
          if (skip != 0)
            throw new IOException("Invalid restored Hudi row offset " + split.getConsumed());
          return null;
        }
        if (filter != null) root = filter.apply(root);
        if (root.getRowCount() == 0) {
          root.close();
          continue;
        }
        if (skip >= root.getRowCount()) {
          skip -= root.getRowCount();
          root.close();
          continue;
        }
        if (skip > 0) {
          try (var original = root) {
            root = original.slice((int) skip, original.getRowCount() - (int) skip);
          }
          skip = 0;
        }
        var batch = new ArrowBatch(root);
        batches.add(batch);
        return BatchRecords.forRecords(split.splitId(), List.of(batch), split.getFileOffset(), 0);
      }
      return null;
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  @Override
  public void closeCurrentSplit() {
    try {
      if (reader != null) reader.close();
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    } finally {
      reader = null;
      if (filter != null) {
        filter.close();
        filter = null;
      }
    }
  }

  @Override
  public void close() {
    closeCurrentSplit();
  }
}
