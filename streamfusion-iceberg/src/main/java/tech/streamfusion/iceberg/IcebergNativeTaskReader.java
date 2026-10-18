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

/** One Java-planned file task, decoded directly to Arrow by the released Iceberg reader. */
final class IcebergNativeTaskReader implements IcebergTaskBatchReader {
  private final IcebergTaskFiles files;
  private long handle;
  private final org.apache.arrow.vector.types.pojo.Schema outputType;
  private final IcebergReadCancellation cancellation;

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
    descriptor.set(
        "metadata",
        IcebergTaskMetadata.translate(mapper, task, schema, filters, caseSensitive, nameMapping));
    var deletes = descriptor.putArray("deletes");
    Map<String, Long> lengths = new HashMap<>();
    lengths.put(task.file().location(), task.file().fileSizeInBytes());
    for (var delete : task.deletes()) {
      if (delete.format() != FileFormat.PARQUET || delete.keyMetadata() != null) {
        throw new IllegalArgumentException("Native delete reads require plaintext Parquet");
      }
      var entry = deletes.addObject();
      entry.put("file_path", delete.location());
      entry.put("file_size_in_bytes", delete.fileSizeInBytes());
      entry.put("partition_spec_id", delete.specId());
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
    var selected = new Schema(projection.stream().map(schema::findField).toList());
    outputType =
        tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
            org.apache.iceberg.flink.FlinkSchemaUtil.convert(selected));
    files = new IcebergTaskFiles(io, lengths);
    try (var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
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
      throw failure;
    }
  }

  public VectorSchemaRoot next() {
    if (handle == 0) throw new IllegalStateException("Iceberg native task reader is closed");
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
      try {
        if (!NativeIceberg.nextRead(handle, array.memoryAddress())) return null;
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
      }
    }
  }
}
