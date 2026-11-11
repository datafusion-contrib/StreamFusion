package tech.streamfusion.hudi;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriter;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.HoodieFlinkIOFactory;
import org.apache.hudi.util.HoodieSchemaConverter;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.ParquetCodec;

/** Decorates append data files; Hudi still owns handles, markers, write statuses and commits. */
public final class HudiAppendIOFactory extends HoodieFlinkIOFactory {
  public HudiAppendIOFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType type) {
    if (type != HoodieRecord.HoodieRecordType.FLINK) return super.getWriterFactory(type);
    return new HoodieRowDataFileWriterFactory(storage) {
      @Override
      public HoodieFileWriter newParquetFileWriter(
          String instant,
          StoragePath path,
          HoodieConfig config,
          HoodieSchema schema,
          TaskContextSupplier context)
          throws IOException {
        if (enableBloomFilter(((HoodieWriteConfig) config).populateMetaFields(), config))
          throw new IllegalArgumentException("Hudi bloom metadata is not admitted");
        return new Writer(
            storage,
            path,
            (HoodieWriteConfig) config,
            HoodieSchemaConverter.convertToRowType(schema));
      }
    };
  }

  static final class Batch {
    final VectorSchemaRoot data;
    final Cursor cursor;
    final VectorizedColumnBatch vectors;
    boolean finished;

    Batch(VectorSchemaRoot root, RowType type) {
      // This root borrows the input's payload vectors; the caller retains their sole close duty.
      data =
          new VectorSchemaRoot(
              ArrowConversion.toArrowSchema(type),
              root.getFieldVectors().subList(0, type.getFieldCount()),
              root.getRowCount());
      vectors =
          new VectorizedColumnBatch(
              ArrowConversion.createArrowReader(data, type).getColumnVectors());
      cursor = new Cursor(this, vectors);
    }
  }

  static final class Cursor extends org.apache.flink.table.data.utils.JoinedRowData {
    private static final RowData EMPTY = new org.apache.flink.table.data.GenericRowData(0);
    final Batch batch;
    final ColumnarRowData view;
    final int[] projection;
    int rowId;

    Cursor(Batch batch, VectorizedColumnBatch vectors) {
      this(batch, new ColumnarRowData(vectors));
    }

    private Cursor(Batch batch, ColumnarRowData view) {
      this(batch, view, null);
    }

    private Cursor(Batch batch, ColumnarRowData view, int[] projection) {
      super(
          EMPTY,
          projection == null
              ? view
              : org.apache.flink.table.data.utils.ProjectedRowData.from(projection)
                  .replaceRow(view));
      this.batch = batch;
      this.view = view;
      this.projection = projection;
    }

    void setRowId(int index) {
      rowId = index;
      view.setRowId(index);
    }

    Cursor project(int[] fields) {
      var mapped = fields;
      if (projection != null) {
        mapped = new int[fields.length];
        for (int i = 0; i < fields.length; i++) mapped[i] = projection[fields[i]];
      }
      var result = new Cursor(batch, new ColumnarRowData(batch.vectors), mapped);
      result.setRowId(rowId);
      result.setRowKind(getRowKind());
      return result;
    }
  }

  private static final class Writer implements HoodieRowDataFileWriter {
    private final OutputStream output;
    private final ColumnarFileCodec.Encoder encoder;
    private final TaskOffHeapMemory.Reservation memory;
    private final long targetSize;
    private Batch last;
    private boolean closed;

    Writer(HoodieStorage storage, StoragePath path, HoodieWriteConfig config, RowType type)
        throws IOException {
      if (config.populateMetaFields())
        throw new IllegalArgumentException("Native Hudi append requires metadata fields disabled");
      targetSize = config.getParquetMaxFileSize();
      memory =
          TaskOffHeapMemory.reserve(
              "hudi-parquet-writer",
              path.toString(),
              Math.addExact(Math.multiplyExact(2L, config.getParquetBlockSize()), 1 << 20));
      OutputStream opened = null;
      try {
        opened = storage.create(path, false);
        var options = new java.util.LinkedHashMap<String, String>();
        options.put("compression", config.getParquetCompressionCodec().toUpperCase(Locale.ROOT));
        options.put("block.size", Integer.toString(config.getParquetBlockSize()));
        options.put("page.size", Integer.toString(config.getParquetPageSize()));
        options.put(
            "enable.dictionary",
            Boolean.toString(
                config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED)));
        encoder =
            new ParquetCodec()
                .createEncoder(
                    ArrowConversion.toArrowSchema(type),
                    new int[0],
                    options.keySet().toArray(new String[0]),
                    options.values().toArray(new String[0]),
                    false,
                    opened);
        output = opened;
      } catch (Throwable failure) {
        if (opened != null)
          try {
            opened.close();
          } catch (Throwable closing) {
            failure.addSuppressed(closing);
          }
        memory.close();
        throw failure;
      }
    }

    @Override
    public boolean canWrite() {
      return !closed && ((last != null && !last.finished) || encoder.estimatedBytes() < targetSize);
    }

    @Override
    public void writeRow(String key, RowData row) throws IOException {
      if (closed) throw new IOException("Hudi Parquet writer is closed");
      if (!(row instanceof Cursor))
        throw new IOException("Hudi append expected a borrowed Arrow row view");
      Batch batch = ((Cursor) row).batch;
      if (batch == last) return;
      encoder.write(batch.data, new int[0], 0, batch.data.getRowCount());
      last = batch;
    }

    @Override
    public void writeRowWithMetaData(HoodieKey key, RowData row) throws IOException {
      throw new IOException("Metadata-bearing Hudi writes are not admitted");
    }

    @Override
    public void close() throws IOException {
      if (closed) return;
      closed = true;
      last = null;
      try (var reserved = memory;
          var stream = output;
          var writer = encoder) {
        writer.finish();
      }
    }
  }
}
