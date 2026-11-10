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
        || (split.getLogPaths().isPresent() && !split.getLogPaths().get().isEmpty())
        || split.getInstantRange().isPresent())
      throw new IllegalArgumentException("Hudi native snapshot requires a base-file-only split");
    try {
      var meta = StreamerUtil.metaClientForReader(conf, HadoopConfigurations.getHadoopConf(conf));
      reader =
          new HudiParquetReader(
              meta.getStorage(), new StoragePath(split.getBasePath().get()), output, rows);
      skip = split.getConsumed();
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
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
    if (reader == null) return;
    try {
      reader.close();
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    } finally {
      reader = null;
    }
  }

  @Override
  public void close() {
    closeCurrentSplit();
  }
}
