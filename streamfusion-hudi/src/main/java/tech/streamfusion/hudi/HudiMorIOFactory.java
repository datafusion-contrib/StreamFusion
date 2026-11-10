package tech.streamfusion.hudi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.format.HoodieFlinkIOFactory;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.ParquetCodec;

/** Only replaces Parquet log encoding; Hudi retains its log framing and append handle. */
public final class HudiMorIOFactory extends HoodieFlinkIOFactory {
  public HudiMorIOFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  public FileFormatUtils getFileFormatUtils(HoodieFileFormat format) {
    if (format != HoodieFileFormat.PARQUET) return super.getFileFormatUtils(format);
    return new ParquetUtils() {
      @Override
      public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(
          HoodieStorage storage,
          Iterator<HoodieRecord> records,
          HoodieRecord.HoodieRecordType type,
          HoodieSchema writerSchema,
          HoodieSchema readerSchema,
          String keyField,
          Map<String, String> parameters)
          throws IOException {
        if (type != HoodieRecord.HoodieRecordType.FLINK)
          return super.serializeRecordsToLogBlock(
              storage, records, type, writerSchema, readerSchema, keyField, parameters);
        var config = new HoodieConfig();
        parameters.forEach(config::setValue);
        config.setValue(
            HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(),
            Long.toString(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE));
        config.setValue(
            HoodieStorageConfig.PARQUET_PAGE_SIZE.key(),
            Integer.toString(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE));
        var output = new EncodedBytes();
        var writer = new LogWriter(output, config, writerSchema);
        // The released helper has no failure cleanup. Keep its control loop with scoped ownership.
        try (var owned = writer) {
          while (records.hasNext()) {
            var record = records.next();
            writer.write(record.getRecordKey(readerSchema, keyField), record, writerSchema);
          }
        }
        return Pair.of(output, writer.getFileFormatMetadata());
      }
    };
  }

  private static final class EncodedBytes extends ByteArrayOutputStream {
    ParquetMetadata footer() throws IOException {
      return readFooter(buf, count);
    }
  }

  private static final class LogWriter implements HoodieRowDataFileWriter {
    private static final Field PAYLOAD = payloadField();
    private final EncodedBytes output;
    private final ColumnarFileCodec.Encoder encoder;
    private final TaskOffHeapMemory.Reservation memory;
    private final org.apache.arrow.vector.types.pojo.Schema schema;
    private final List<VarCharVector> metadata = new ArrayList<>();
    private final List<Integer> selected = new ArrayList<>();
    private HudiAppendIOFactory.Batch batch;
    private RowData[] generatedMetadata;
    private ParquetMetadata footer;
    private boolean closed;

    LogWriter(EncodedBytes output, HoodieConfig config, HoodieSchema hudiSchema)
        throws IOException {
      this.output = output;
      schema = ArrowConversion.toArrowSchema(HoodieSchemaConverter.convertToRowType(hudiSchema));
      long block = config.getLongOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE);
      memory =
          TaskOffHeapMemory.reserve(
              "hudi-parquet-log",
              "MOR log block",
              Math.addExact(Math.multiplyExact(2, block), 1 << 20));
      try {
        encoder =
            new ParquetCodec()
                .createEncoder(
                    schema,
                    new int[0],
                    new String[] {"compression", "block.size", "page.size", "enable.dictionary"},
                    new String[] {
                      config
                          .getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)
                          .toUpperCase(Locale.ROOT),
                      Long.toString(block),
                      config.getStringOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
                      Boolean.toString(
                          config.getBooleanOrDefault(
                              HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED))
                    },
                    false,
                    output);
      } catch (Throwable failure) {
        memory.close();
        throw failure;
      }
    }

    @Override
    public boolean canWrite() {
      return !closed;
    }

    @Override
    public void writeRow(String key, RowData row) throws IOException {
      if (closed) throw new IOException("Hudi Parquet log writer is closed");
      var cursor = payload(row);
      if (batch != cursor.batch) {
        flushBatch();
        batch = cursor.batch;
        if (row.getArity() != batch.data.getFieldVectors().size() + 5
            || schema.getFields().size() != row.getArity())
          throw new IOException("Hudi MOR requires exactly five generated metadata fields");
        generatedMetadata = new RowData[batch.data.getRowCount()];
        try {
          for (int i = 0; i < 5; i++) {
            var vector =
                (VarCharVector) schema.getFields().get(i).createVector(NativeAllocator.SHARED);
            metadata.add(vector);
            vector.allocateNew();
          }
        } catch (Throwable failure) {
          closeMetadata();
          throw failure;
        }
      }
      if (generatedMetadata[cursor.rowId] != null)
        throw new IOException("Hudi MOR received the same payload position twice");
      generatedMetadata[cursor.rowId] = row;
      selected.add(cursor.rowId);
    }

    @Override
    public void writeRowWithMetaData(HoodieKey key, RowData row) throws IOException {
      throw new IOException("Hudi's append handle must generate log metadata before encoding");
    }

    private void flushBatch() throws IOException {
      if (batch == null) return;
      try {
        // Fill variable-width metadata in vector order; selection retains Hudi's record order.
        for (int row = 0; row < generatedMetadata.length; row++)
          for (int column = 0; column < 5; column++) {
            var values = generatedMetadata[row];
            if (values == null || values.isNullAt(column)) metadata.get(column).setNull(row);
            else metadata.get(column).setSafe(row, values.getString(column).toBytes());
          }
        for (var vector : metadata) vector.setValueCount(batch.data.getRowCount());
        List<FieldVector> vectors = new ArrayList<>(metadata);
        vectors.addAll(batch.data.getFieldVectors());
        // Payload vectors are borrowed; only the five generated metadata vectors are closed here.
        var borrowed = new VectorSchemaRoot(schema, vectors, batch.data.getRowCount());
        int[] rows = selected.stream().mapToInt(Integer::intValue).toArray();
        encoder.write(borrowed, rows, 0, rows.length);
      } finally {
        closeMetadata();
      }
    }

    private void closeMetadata() {
      batch = null;
      generatedMetadata = null;
      selected.clear();
      for (var vector : metadata) vector.close();
      metadata.clear();
    }

    @Override
    public void close() throws IOException {
      if (closed) return;
      closed = true;
      try (var reserved = memory;
          var writer = encoder) {
        flushBatch();
        writer.finish();
        footer = output.footer();
      } finally {
        closeMetadata();
      }
    }

    @Override
    public Object getFileFormatMetadata() {
      if (footer == null) throw new IllegalStateException("Hudi Parquet log is not finished");
      return footer;
    }

    private static Field payloadField() {
      try {
        var field = JoinedRowData.class.getDeclaredField("row2");
        field.setAccessible(true);
        return field;
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException("Unsupported Flink joined-row metadata wrapper", failure);
      }
    }

    private static HudiAppendIOFactory.Cursor payload(RowData row) throws IOException {
      try {
        Object payload = row.getClass() == JoinedRowData.class ? PAYLOAD.get(row) : null;
        if (!(payload instanceof HudiAppendIOFactory.Cursor))
          throw new IOException("Hudi MOR expected a stable borrowed Arrow payload view");
        return (HudiAppendIOFactory.Cursor) payload;
      } catch (IllegalAccessException failure) {
        throw new IOException("Cannot inspect Hudi's generated metadata wrapper", failure);
      }
    }
  }

  private static ParquetMetadata readFooter(byte[] bytes, int length) throws IOException {
    InputFile file =
        new InputFile() {
          @Override
          public long getLength() {
            return length;
          }

          @Override
          public SeekableInputStream newStream() {
            var input = new ByteArrayInputStream(bytes, 0, length);
            return new DelegatingSeekableInputStream(input) {
              @Override
              public long getPos() {
                return length - input.available();
              }

              @Override
              public void seek(long offset) throws IOException {
                if (offset < 0 || offset > length) throw new IOException("Invalid footer seek");
                input.reset();
                if (input.skip(offset) != offset) throw new IOException("Incomplete footer seek");
              }
            };
          }
        };
    try (var reader = ParquetFileReader.open(file)) {
      return reader.getFooter();
    }
  }
}
