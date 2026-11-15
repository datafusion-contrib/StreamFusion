package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.FileIO;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** One Java-planned file task, decoded directly to Arrow by the pinned Iceberg reader. */
final class IcebergNativeTaskReader implements IcebergTaskBatchReader {
  private final IcebergTaskFiles files;
  private long handle;
  private final org.apache.arrow.vector.types.pojo.Schema outputType;
  private final IcebergReadCancellation cancellation;
  private long equalityMemoryOwner;
  private boolean equalityAccounted;

  IcebergNativeTaskReader(
      FileScanTask task,
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      long skip)
      throws IOException {
    this(task, schema, projection, io, batchRows, skip, List.of(), true, null);
  }

  IcebergNativeTaskReader(
      FileScanTask task,
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      long skip,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean caseSensitive,
      String nameMapping)
      throws IOException {
    this(task, schema, projection, io, batchRows, skip, filters, caseSensitive, nameMapping, null);
  }

  IcebergNativeTaskReader(
      FileScanTask task,
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      long skip,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean caseSensitive,
      String nameMapping,
      IcebergReadCancellation cancellation)
      throws IOException {
    this(
        task,
        schema,
        projection,
        io,
        batchRows,
        skip,
        filters,
        caseSensitive,
        nameMapping,
        cancellation,
        false);
  }

  IcebergNativeTaskReader(
      FileScanTask task,
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      long skip,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean caseSensitive,
      String nameMapping,
      IcebergReadCancellation cancellation,
      boolean rewrite)
      throws IOException {
    this(
        task,
        schema,
        projection,
        io,
        batchRows,
        skip,
        filters,
        caseSensitive,
        nameMapping,
        cancellation,
        rewrite,
        null);
  }

  IcebergNativeTaskReader(
      FileScanTask task,
      Schema schema,
      List<Integer> projection,
      FileIO io,
      int batchRows,
      long skip,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean caseSensitive,
      String nameMapping,
      IcebergReadCancellation cancellation,
      boolean rewrite,
      Schema requested)
      throws IOException {
    this.cancellation = cancellation;
    if (task.isDataTask()
        || task.file().format() != FileFormat.PARQUET
        || task.file().keyMetadata() != null) {
      throw new IllegalArgumentException(
          "Unverified Iceberg task format, encryption, or data task");
    }
    var mapper = IcebergJson.mapper();
    var descriptor = mapper.createObjectNode();
    descriptor.set("schema", mapper.readTree(SchemaParser.toJson(schema)));
    descriptor.put("path", task.file().location());
    descriptor.put("file_size", task.file().fileSizeInBytes());
    descriptor.put("start", task.start());
    descriptor.put("length", task.length());
    descriptor.set("project_ids", mapper.valueToTree(projection));
    descriptor.put("case_sensitive", caseSensitive);
    boolean equalitySets =
        rewrite || schema.columns().stream().anyMatch(f -> f.type().isNestedType());
    descriptor.put("columnar_equality_sets", equalitySets);
    descriptor.set("first_row_id", mapper.valueToTree(task.file().firstRowId()));
    descriptor.set("data_sequence_number", mapper.valueToTree(task.file().dataSequenceNumber()));
    descriptor.set(
        "metadata",
        IcebergTaskMetadata.translate(mapper, task, schema, filters, caseSensitive, nameMapping));
    var deletes = descriptor.putArray("deletes");
    Map<String, Long> lengths = new HashMap<>();
    lengths.put(task.file().location(), task.file().fileSizeInBytes());
    for (var delete : task.deletes()) {
      if ((delete.format() != FileFormat.PARQUET
              && !org.apache.iceberg.util.ContentFileUtil.isDV(delete))
          || delete.keyMetadata() != null) {
        throw new IllegalArgumentException(
            "Native delete reads require plaintext Parquet or Puffin DVs");
      }
      var entry = deletes.addObject();
      entry.put("file_path", delete.location());
      entry.put("file_size_in_bytes", delete.fileSizeInBytes());
      entry.put("partition_spec_id", delete.specId());
      entry.put("file_format", delete.format() == FileFormat.PUFFIN ? "Puffin" : "Parquet");
      entry.set("referenced_data_file", mapper.valueToTree(delete.referencedDataFile()));
      entry.set("content_offset", mapper.valueToTree(delete.contentOffset()));
      entry.set("content_size_in_bytes", mapper.valueToTree(delete.contentSizeInBytes()));
      entry.put("record_count", delete.recordCount());
      switch (delete.content()) {
        case POSITION_DELETES:
          entry.put("file_type", "PositionDeletes");
          break;
        case EQUALITY_DELETES:
          entry.put("file_type", "EqualityDeletes");
          break;
        default:
          throw new IllegalArgumentException("Unverified native delete type");
      }
      entry.set("equality_ids", mapper.valueToTree(delete.equalityFieldIds()));
      Long previous = lengths.put(delete.location(), delete.fileSizeInBytes());
      if (previous != null && previous.longValue() != delete.fileSizeInBytes()) {
        throw new IllegalArgumentException("Conflicting Java-planned file lengths");
      }
    }
    var readSchema = org.apache.iceberg.MetadataColumns.schemaWithRowLineage(schema);
    var selected = new Schema(projection.stream().map(readSchema::findField).toList());
    outputType = arrowSchema(requested == null ? selected : requested, rewrite);
    files = new IcebergTaskFiles(io, lengths);
    try (var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      if (equalitySets)
        equalityMemoryOwner = TaskOffHeapMemory.registerOwner("iceberg-read", "equality-sets");
      if (cancellation != null) cancellation.add(files);
      Data.exportSchema(
          NativeAllocator.SHARED, outputType, NativeAllocator.DICTIONARIES, outputSchema);
      try {
        handle =
            NativeIceberg.createReader(
                mapper.writeValueAsString(descriptor),
                files,
                batchRows,
                skip,
                outputSchema.memoryAddress());
      } finally {
        if (outputSchema.snapshot().release != 0) outputSchema.release();
      }
    } catch (Throwable failure) {
      try {
        files.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      if (cancellation != null) cancellation.remove(files);
      TaskOffHeapMemory.closeOwner(equalityMemoryOwner);
      throw failure;
    }
  }

  static org.apache.arrow.vector.types.pojo.Schema arrowSchema(Schema schema, boolean rewrite) {
    var arrow =
        tech.streamfusion.arrow.ArrowFileBatch.schema(
            tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
                org.apache.iceberg.flink.FlinkSchemaUtil.convert(schema)));
    var fields = new java.util.ArrayList<org.apache.arrow.vector.types.pojo.Field>();
    for (int i = 0; i < schema.columns().size(); i++)
      fields.add(icebergField(arrow.getFields().get(i), schema.columns().get(i), rewrite));
    return new org.apache.arrow.vector.types.pojo.Schema(fields, arrow.getCustomMetadata());
  }

  private static org.apache.arrow.vector.types.pojo.Field icebergField(
      org.apache.arrow.vector.types.pojo.Field field,
      org.apache.iceberg.types.Types.NestedField iceberg,
      boolean rewrite) {
    var type = field.getType();
    if (rewrite && type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Time)
      type =
          new org.apache.arrow.vector.types.pojo.ArrowType.Time(
              org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64);
    var metadata = new HashMap<>(field.getMetadata());
    metadata.put("PARQUET:field_id", Integer.toString(iceberg.fieldId()));
    var children = field.getChildren();
    if (iceberg.type().isMapType()) {
      var entry = children.get(0);
      var keyValue = iceberg.type().asMapType().fields();
      children =
          List.of(
              new org.apache.arrow.vector.types.pojo.Field(
                  entry.getName(),
                  entry.getFieldType(),
                  List.of(
                      icebergField(entry.getChildren().get(0), keyValue.get(0), rewrite),
                      icebergField(entry.getChildren().get(1), keyValue.get(1), rewrite))));
    } else if (iceberg.type().isNestedType()) {
      var nested = iceberg.type().asNestedType().fields();
      var converted = new java.util.ArrayList<org.apache.arrow.vector.types.pojo.Field>();
      for (int i = 0; i < nested.size(); i++)
        converted.add(icebergField(children.get(i), nested.get(i), rewrite));
      children = converted;
    }
    return new org.apache.arrow.vector.types.pojo.Field(
        field.getName(),
        new org.apache.arrow.vector.types.pojo.FieldType(
            field.isNullable(), type, field.getDictionary(), metadata),
        children);
  }

  public VectorSchemaRoot next() {
    if (handle == 0) throw new IllegalStateException("Iceberg native task reader is closed");
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
      try {
        boolean present = NativeIceberg.nextRead(handle, array.memoryAddress());
        if (equalityMemoryOwner != 0 && !equalityAccounted) {
          long retained = NativeIceberg.readRetainedBytes(handle);
          if (!TaskOffHeapMemory.tryReserve(equalityMemoryOwner, retained))
            throw new IllegalStateException(
                "Iceberg equality sets exceed the task off-heap budget");
          equalityAccounted = true;
        }
        if (!present) return null;
        var root = VectorSchemaRoot.create(outputType, NativeAllocator.SHARED);
        try {
          Data.importIntoVectorSchemaRoot(
              NativeAllocator.SHARED,
              ArrowArray.wrap(array.memoryAddress()),
              root,
              NativeAllocator.DICTIONARIES);
          return root;
        } catch (Throwable failure) {
          root.close();
          throw failure;
        }
      } finally {
        if (array.snapshot().release != 0) array.release();
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (handle != 0) {
        long current = handle;
        handle = 0;
        NativeIceberg.closeReader(current);
      }
    } finally {
      try {
        files.close();
      } finally {
        if (cancellation != null) cancellation.remove(files);
        TaskOffHeapMemory.closeOwner(equalityMemoryOwner);
        equalityMemoryOwner = 0;
      }
    }
  }
}
