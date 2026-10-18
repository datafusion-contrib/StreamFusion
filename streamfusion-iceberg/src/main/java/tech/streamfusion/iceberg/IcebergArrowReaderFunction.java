package tech.streamfusion.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import tech.streamfusion.operator.ArrowBatch;

/** Preserves Java split ordinals and logical row offsets while fetching Arrow batches. */
final class IcebergArrowReaderFunction implements ReaderFunction<ArrowBatch> {
  private final Schema schema;
  private final List<Integer> projection;
  private final FileIO io;
  private final int batchRows;
  private final List<org.apache.iceberg.expressions.Expression> filters;
  private final boolean caseSensitive;
  private final String nameMapping;
  private final org.apache.iceberg.flink.source.reader.RowDataReaderFunction stock;
  private transient IcebergReadCancellation cancellation;
  private transient org.apache.flink.metrics.Counter nativeTasks;
  private transient org.apache.flink.metrics.Counter stockTasks;

  void registerMetrics(org.apache.flink.metrics.MetricGroup group) {
    nativeTasks = group.counter("nativeParquetTasks");
    stockTasks = group.counter("stockFallbackTasks");
  }

  IcebergArrowReaderFunction(Schema schema, List<Integer> projection, FileIO io, int batchRows) {
    this(
        schema,
        projection,
        io,
        batchRows,
        List.of(),
        true,
        null,
        org.apache.iceberg.encryption.PlaintextEncryptionManager.instance(),
        new org.apache.flink.configuration.Configuration());
  }

  IcebergArrowReaderFunction(
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean caseSensitive,
      String nameMapping,
      org.apache.iceberg.encryption.EncryptionManager encryption,
      org.apache.flink.configuration.ReadableConfig configuration) {
    this.stock =
        new org.apache.iceberg.flink.source.reader.RowDataReaderFunction(
            configuration,
            schema,
            new Schema(projection.stream().map(schema::findField).toList()),
            nameMapping,
            caseSensitive,
            io,
            encryption,
            filters);
    this.filters = List.copyOf(filters);
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
    this.schema = schema;
    this.projection = List.copyOf(projection);
    this.io = io;
    this.batchRows = batchRows;
  }

  private synchronized IcebergReadCancellation cancellation() {
    if (cancellation == null) cancellation = new IcebergReadCancellation();
    return cancellation;
  }

  void cancelIo() throws IOException {
    cancellation().close();
  }

  private boolean nativeTask(FileScanTask task) {
    if (task.isDataTask()
        || task.file().format() != org.apache.iceberg.FileFormat.PARQUET
        || task.file().keyMetadata() != null) return false;
    if (task.residual().op() != org.apache.iceberg.expressions.Expression.Operation.TRUE
        && filters.isEmpty()) return false;
    for (var delete : task.deletes()) {
      if (delete.format() != org.apache.iceberg.FileFormat.PARQUET || delete.keyMetadata() != null)
        return false;
      if (delete.equalityFieldIds() != null) {
        for (int id : delete.equalityFieldIds()) {
          var field = schema.findField(id);
          if (field == null || field.type().typeId() == org.apache.iceberg.types.Type.TypeID.TIME)
            return false;
        }
      }
    }
    return true;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<ArrowBatch>>> apply(
      IcebergSourceSplit split) {
    List<FileScanTask> tasks = List.copyOf(split.task().files());
    if (split.fileOffset() < 0 || split.fileOffset() > tasks.size() || split.recordOffset() < 0) {
      throw new IllegalArgumentException("Invalid restored Iceberg split position");
    }
    return new CloseableIterator<>() {
      private int fileOffset = split.fileOffset();
      private long rowOffset = split.recordOffset();
      private IcebergTaskBatchReader reader;
      private VectorSchemaRoot pending;
      private boolean closed;

      @Override
      public boolean hasNext() {
        if (closed) return false;
        if (pending != null) return true;
        try {
          while (fileOffset < tasks.size()) {
            if (reader == null) {
              var task = tasks.get(fileOffset);
              if (nativeTask(task)) {
                if (nativeTasks != null) nativeTasks.inc();
                reader =
                    new IcebergNativeTaskReader(
                        task,
                        schema,
                        projection,
                        io,
                        batchRows,
                        rowOffset,
                        filters,
                        caseSensitive,
                        nameMapping,
                        cancellation());
              } else {
                if (stockTasks != null) stockTasks.inc();
                reader =
                    new IcebergStockTaskReader(
                        task,
                        new Schema(projection.stream().map(schema::findField).toList()),
                        stock,
                        batchRows,
                        rowOffset);
              }
            }
            pending = reader.next();
            if (pending != null) return true;
            reader.close();
            reader = null;
            fileOffset++;
            rowOffset = 0;
          }
          return false;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public RecordsWithSplitIds<RecordAndPosition<ArrowBatch>> next() {
        if (!hasNext()) throw new NoSuchElementException();
        var root = pending;
        pending = null;
        rowOffset = Math.addExact(rowOffset, root.getRowCount());
        var batch =
            new Batch(
                split.splitId(),
                new RecordAndPosition<>(new ArrowBatch(root), fileOffset, rowOffset),
                cancellation());
        try {
          cancellation().add(batch);
          return batch;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
          if (pending != null) {
            pending.close();
            pending = null;
          }
        } finally {
          if (reader != null) {
            reader.close();
            reader = null;
          }
        }
      }
    };
  }

  private static final class Batch
      implements RecordsWithSplitIds<RecordAndPosition<ArrowBatch>>, java.io.Closeable {
    private String split;
    private RecordAndPosition<ArrowBatch> record;
    private final IcebergReadCancellation cancellation;

    private Batch(
        String split,
        RecordAndPosition<ArrowBatch> record,
        IcebergReadCancellation cancellation) {
      this.split = split;
      this.record = record;
      this.cancellation = cancellation;
    }

    @Override
    public String nextSplit() {
      String result = split;
      split = null;
      return result;
    }

    @Override
    public synchronized RecordAndPosition<ArrowBatch> nextRecordFromSplit() {
      var result = record;
      record = null;
      cancellation.remove(this);
      return result;
    }

    @Override
    public Set<String> finishedSplits() {
      return Set.of();
    }

    @Override
    public synchronized void recycle() {
      if (record != null) {
        var root = record.record().root();
        record = null;
        try {
          root.close();
        } finally {
          cancellation.remove(this);
        }
      }
    }

    @Override
    public void close() {
      recycle();
    }
  }
}
