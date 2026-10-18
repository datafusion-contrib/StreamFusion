package tech.streamfusion.iceberg;

import java.io.IOException;
import java.io.Serializable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Host task writers traverse borrowed Arrow views; their normal rolling and lifecycle stay intact.
 */
final class IcebergTaskWriterFactory implements TaskWriterFactory<ArrowBatch> {
  interface Decorator extends Serializable {
    FileWriterFactory<RowData> decorate(
        FileWriterFactory<RowData> stock, ArrowWriteContext context);
  }

  private final RowDataTaskWriterFactory stock;
  private final org.apache.flink.streaming.api.operators.AbstractStreamOperator<?> owner;
  private final RowType rowType;
  private final Decorator decorator;
  private final boolean keyed;
  private transient ArrowWriteContext context;

  IcebergTaskWriterFactory(
      RowDataTaskWriterFactory stock,
      RowType rowType,
      Decorator decorator,
      org.apache.flink.streaming.api.operators.AbstractStreamOperator<?> owner) {
    this.owner = owner;
    this.stock = stock;
    this.rowType = rowType;
    this.decorator = decorator;
    try {
      var keys =
          (java.util.Collection<?>)
              IcebergSinkAdapter.field(RowDataTaskWriterFactory.class, "equalityFieldIds")
                  .get(stock);
      this.keyed = keys != null && !keys.isEmpty();
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot inspect the released Iceberg equality fields", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(int taskId, int attemptId) {
    NativeAllocator.initializeFor(owner);
    stock.initialize(taskId, attemptId);
    context = new ArrowWriteContext();
    var field = IcebergSinkAdapter.field(RowDataTaskWriterFactory.class, "fileWriterFactory");
    try {
      FileWriterFactory<RowData> original = (FileWriterFactory<RowData>) field.get(stock);
      field.set(stock, decorator.decorate(original, context));
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot adapt Iceberg data-file writer factory", e);
    }
  }

  @Override
  public TaskWriter<ArrowBatch> create() {
    if (context == null)
      throw new IllegalStateException("Iceberg writer factory is not initialized");
    TaskWriter<RowData> delegate = stock.create();
    return new TaskWriter<>() {
      @Override
      public void write(ArrowBatch batch) throws IOException {
        try (VectorSchemaRoot root = batch.root()) {
          var kinds =
              (org.apache.arrow.vector.TinyIntVector)
                  root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          if (kinds != null) {
            for (int i = 0; i < root.getRowCount(); i++) {
              if (kinds.isNull(i) || (!keyed && kinds.get(i) != RowKind.INSERT.toByteValue())) {
                throw new IllegalArgumentException(
                    "Append-only Iceberg adapter received an updating row");
              }
              RowKind.fromByteValue(kinds.get(i));
            }
          }
          var reader = ArrowConversion.createArrowReader(root, rowType);
          context.begin(root);
          try {
            for (int i = 0; i < root.getRowCount(); i++) {
              RowData row = reader.read(i);
              row.setRowKind(kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(i)));
              context.position(i, row);
              delegate.write(row);
            }
            context.drain();
          } catch (IOException | RuntimeException | Error failure) {
            context.fail(failure);
            throw failure;
          } finally {
            context.clear();
          }
        }
      }

      @Override
      public WriteResult complete() throws IOException {
        return delegate.complete();
      }

      @Override
      public void abort() throws IOException {
        delegate.abort();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }
}
