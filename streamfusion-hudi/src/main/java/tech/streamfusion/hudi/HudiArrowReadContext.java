package tech.streamfusion.hudi;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.FlinkRecordContext;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Hudi merges stable Arrow row references and spills its own control records. */
final class HudiArrowReadContext extends FlinkRowDataReaderContext implements AutoCloseable {
  static final class Input {
    final VectorSchemaRoot root;
    final HudiAppendIOFactory.Batch batch;
    final long bytes;
    boolean released;
    boolean selected;

    Input(VectorSchemaRoot root, RowType type) {
      this.root = root;
      batch = new HudiAppendIOFactory.Batch(root, type);
      bytes = root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
    }
  }

  private static final class Position implements Serializable {
    final int batch;
    final int row;
    final byte kind;
    final int[] projection;

    Position(int batch, int row, byte kind, int[] projection) {
      this.batch = batch;
      this.row = row;
      this.kind = kind;
      this.projection = projection;
    }
  }

  final List<Input> inputs = new ArrayList<>();
  private final Map<HudiAppendIOFactory.Batch, Integer> locations = new IdentityHashMap<>();
  private final Set<HudiParquetReader> readers = new HashSet<>();
  private final List<Input> temporaryInputs = new ArrayList<>();
  private final TaskOffHeapMemory.Reservation memory;
  private final long budget;
  private final int batchRows;
  private final StoragePath basePath;
  private Input currentBase;
  private long retainedBytes;

  HudiArrowReadContext(
      HoodieTableMetaClient meta,
      Option<InstantRange> range,
      int batchRows,
      long budget,
      String basePath,
      HoodieSchema outputSchema) {
    super(
        meta.getStorageConf(),
        () -> InternalSchemaManager.DISABLED,
        List.of(),
        meta.getTableConfig(),
        range);
    this.batchRows = batchRows;
    this.budget = budget;
    this.basePath = basePath == null ? null : new StoragePath(basePath);
    recordContext =
        new FlinkRecordContext(meta.getTableConfig(), meta.getStorageConf()) {
          @Override
          public RowData toBinaryRow(HoodieSchema schema, RowData record) {
            if (!(record instanceof HudiAppendIOFactory.Cursor))
              throw new IllegalArgumentException("Hudi merge expected a stable Arrow row view");
            return record;
          }

          @Override
          public RowData seal(RowData record) {
            return record;
          }

          @Override
          public org.apache.hudi.org.apache.avro.generic.GenericRecord convertToAvroRecord(
              RowData record, HoodieSchema schema) {
            throw new UnsupportedOperationException(
                "Hudi Arrow payloads require native columnar processing");
          }

          @Override
          public RowData convertAvroRecord(
              org.apache.hudi.org.apache.avro.generic.IndexedRecord record) {
            throw new UnsupportedOperationException(
                "Hudi Arrow reads require Parquet log payloads");
          }

          @Override
          public RowData constructEngineRecord(HoodieSchema schema, Object[] values) {
            throw new UnsupportedOperationException(
                "Hudi Arrow reads require complete stored payloads");
          }

          @Override
          public RowData mergeWithEngineRecord(
              HoodieSchema schema, Map<Integer, Object> updates, BufferedRecord<RowData> base) {
            throw new UnsupportedOperationException(
                "Partial Hudi updates require the stock reader");
          }

          @Override
          public java.util.function.UnaryOperator<RowData> projectRecord(
              HoodieSchema from, HoodieSchema to, Map<String, String> renamed) {
            int[] fields =
                to.getFields().stream()
                    .mapToInt(
                        field ->
                            from.getField(renamed.getOrDefault(field.name(), field.name()))
                                .orElseThrow(
                                    () ->
                                        new IllegalArgumentException(
                                            "Hudi Arrow schema evolution is unavailable"))
                                .pos())
                    .toArray();
            var handler = HudiArrowReadContext.this.getSchemaHandler();
            boolean batchProjection =
                handler != null
                    && from.equals(handler.getRequiredSchema())
                    && to.equals(outputSchema)
                    && renamed.isEmpty();
            return record -> {
              if (record instanceof HudiAppendIOFactory.Cursor)
                // The final field projection is applied once to the gathered Arrow batch.
                return batchProjection
                    ? record
                    : ((HudiAppendIOFactory.Cursor) record).project(fields);
              if (record.getRowKind() != RowKind.DELETE
                  && record.getRowKind() != RowKind.UPDATE_BEFORE)
                throw new IllegalArgumentException(
                    "Hudi projection expected an Arrow payload or a generated delete key");
              return org.apache.flink.table.data.utils.ProjectedRowData.from(fields)
                  .replaceRow(record);
            };
          }
        };
    memory =
        TaskOffHeapMemory.reserve("hudi-merge", "Arrow file-group payloads and output", budget);
  }

  @Override
  public ClosableIterator<RowData> getFileRecordIterator(
      StoragePath path,
      long start,
      long length,
      HoodieSchema dataSchema,
      HoodieSchema requiredSchema,
      HoodieStorage storage)
      throws IOException {
    if (start != 0) throw new IOException("Hudi Arrow reads require a complete Parquet view");
    var type = HoodieSchemaConverter.convertToRowType(requiredSchema);
    var reader = new HudiParquetReader(storage, path, type, batchRows);
    boolean base = path.equals(basePath);
    readers.add(reader);
    return new ClosableIterator<>() {
      private Input current;
      private int row;

      @Override
      public boolean hasNext() {
        while (current == null || row == current.root.getRowCount()) {
          var root = reader.next();
          if (root == null) return false;
          if (root.getRowCount() == 0) {
            root.close();
            continue;
          }
          current = retain(root, type, !base);
          if (base) {
            currentBase = current;
            releaseFinishedInputs();
          }
          row = 0;
        }
        return true;
      }

      @Override
      public RowData next() {
        if (!hasNext()) throw new NoSuchElementException();
        var cursor =
            base
                ? current.batch.cursor
                : new HudiAppendIOFactory.Cursor(current.batch, current.batch.vectors);
        cursor.setRowId(row++);
        cursor.setRowKind(RowKind.INSERT);
        return cursor;
      }

      @Override
      public void close() {
        if (!readers.remove(reader)) return;
        try {
          reader.close();
        } catch (IOException failure) {
          throw new UncheckedIOException(failure);
        }
      }
    };
  }

  Input retain(VectorSchemaRoot root, RowType type) {
    return retain(root, type, false);
  }

  private Input retain(VectorSchemaRoot root, RowType type, boolean buffered) {
    try {
      long bytes = root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
      long total = Math.addExact(retainedBytes, bytes);
      if (Math.multiplyExact(total, 2) > budget)
        throw TaskOffHeapMemory.exhausted("Hudi file-group Arrow payloads and output", total * 2);
      var input = new Input(root, type);
      locations.put(input.batch, inputs.size());
      inputs.add(input);
      if (!buffered) temporaryInputs.add(input);
      retainedBytes = total;
      return input;
    } catch (Throwable failure) {
      root.close();
      throw failure;
    }
  }

  void selectionEmitted() {
    for (var input : temporaryInputs) input.selected = false;
    releaseFinishedInputs();
  }

  private void releaseFinishedInputs() {
    var iterator = temporaryInputs.iterator();
    while (iterator.hasNext()) {
      var input = iterator.next();
      if (input == currentBase || input.selected) continue;
      input.root.close();
      input.released = true;
      retainedBytes -= input.bytes;
      locations.remove(input.batch);
      iterator.remove();
    }
  }

  int location(HudiAppendIOFactory.Cursor cursor) {
    int location = locations.get(cursor.batch);
    inputs.get(location).selected = true;
    return location;
  }

  private BufferedRecord<Position> control(BufferedRecord<RowData> record) {
    Position position = null;
    if (record.getRecord() != null) {
      var cursor = (HudiAppendIOFactory.Cursor) record.getRecord();
      position =
          new Position(
              location(cursor), cursor.rowId, cursor.getRowKind().toByteValue(), cursor.projection);
    }
    return new BufferedRecord<>(
        record.getRecordKey(),
        record.getOrderingValue(),
        position,
        record.getSchemaId(),
        record.getHoodieOperation());
  }

  @Override
  public SizeEstimator<BufferedRecord<RowData>> getRecordSizeEstimator() {
    var estimator = new DefaultSizeEstimator<BufferedRecord<Position>>();
    return record -> estimator.sizeEstimate(control(record));
  }

  @Override
  public CustomSerializer<BufferedRecord<RowData>> getRecordSerializer() {
    var serializer = new DefaultSerializer<BufferedRecord<Position>>();
    return new CustomSerializer<>() {
      @Override
      public byte[] serialize(BufferedRecord<RowData> record) throws IOException {
        return serializer.serialize(control(record));
      }

      @Override
      public BufferedRecord<RowData> deserialize(byte[] bytes) {
        var record = serializer.deserialize(bytes);
        RowData payload = null;
        if (record.getRecord() != null) {
          var position = record.getRecord();
          var batch = inputs.get(position.batch).batch;
          var cursor = new HudiAppendIOFactory.Cursor(batch, batch.vectors);
          if (position.projection != null) cursor = cursor.project(position.projection);
          cursor.setRowId(position.row);
          cursor.setRowKind(RowKind.fromByteValue(position.kind));
          payload = cursor;
        }
        return new BufferedRecord<>(
            record.getRecordKey(),
            record.getOrderingValue(),
            payload,
            record.getSchemaId(),
            record.getHoodieOperation());
      }
    };
  }

  @Override
  public void close() throws Exception {
    try (var reserved = memory) {
      List<AutoCloseable> owned = new ArrayList<>(readers);
      for (var input : inputs) if (!input.released) owned.add(input.root);
      org.apache.flink.util.IOUtils.closeAll(owned);
    } finally {
      readers.clear();
      inputs.clear();
      locations.clear();
      temporaryInputs.clear();
    }
  }
}
