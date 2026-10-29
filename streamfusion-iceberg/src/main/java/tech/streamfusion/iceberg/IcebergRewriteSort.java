package tech.streamfusion.iceberg;

import java.util.ArrayList;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Arrow-only external sorting; Java retains the table's sort declaration and spill directory. */
final class IcebergRewriteSort implements AutoCloseable {
  private final Schema schema;
  private final TaskOffHeapMemory.Reservation reservation;
  private long handle;

  IcebergRewriteSort(
      Schema schema,
      org.apache.iceberg.Schema tableSchema,
      SortOrder order,
      long memoryBytes,
      String spillDirectory)
      throws java.io.IOException {
    this.schema = schema;
    var keys = new ArrayList<Object>();
    for (var field : order.fields()) {
      if (!field.transform().isIdentity())
        throw new IllegalArgumentException("Native rewrite sorting requires identity sort fields");
      var path = fieldPath(tableSchema.columns(), field.sourceId());
      if (path == null)
        throw new IllegalArgumentException("Missing sort field ID " + field.sourceId());
      String name = path.get(0);
      int index = -1;
      for (int i = 0; i < schema.getFields().size(); i++) {
        if (schema.getFields().get(i).getName().equals(name)) index = i;
      }
      if (index < 0) throw new IllegalArgumentException("Missing sort field " + name);
      keys.add(
          java.util.List.of(
              index,
              path.subList(1, path.size()),
              field.direction() == SortDirection.DESC,
              field.nullOrder() == NullOrder.NULLS_FIRST));
    }
    reservation = TaskOffHeapMemory.reserve("iceberg-rewrite-sort", "sort", memoryBytes);
    try (var exported = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(NativeAllocator.SHARED, schema, NativeAllocator.DICTIONARIES, exported);
        handle =
            NativeIceberg.createSort(
                exported.memoryAddress(),
                IcebergJson.mapper().writeValueAsString(keys),
                memoryBytes,
                spillDirectory);
      } finally {
        if (exported.snapshot().release != 0) exported.release();
      }
    } catch (Throwable failure) {
      reservation.close();
      throw failure;
    }
  }

  private static java.util.List<String> fieldPath(
      java.util.List<org.apache.iceberg.types.Types.NestedField> fields, int id) {
    for (var field : fields) {
      if (field.fieldId() == id) return java.util.List.of(field.name());
      if (field.type().isStructType()) {
        var child = fieldPath(field.type().asStructType().fields(), id);
        if (child != null) {
          var path = new ArrayList<String>();
          path.add(field.name());
          path.addAll(child);
          return path;
        }
      }
    }
    return null;
  }

  void push(VectorSchemaRoot batch) {
    try (batch;
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, batch, NativeAllocator.DICTIONARIES, array);
        NativeIceberg.pushSort(handle, array.memoryAddress());
      } finally {
        if (array.snapshot().release != 0) array.release();
      }
    }
  }

  void finish() {
    NativeIceberg.finishSort(handle);
  }

  long[] statistics() {
    return NativeIceberg.sortStatistics(handle);
  }

  VectorSchemaRoot next() {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
      try {
        if (!NativeIceberg.nextSort(handle, array.memoryAddress())) return null;
        var root = VectorSchemaRoot.create(schema, NativeAllocator.SHARED);
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
  public void close() {
    try {
      if (handle != 0) {
        long current = handle;
        handle = 0;
        NativeIceberg.closeSort(current);
      }
    } finally {
      reservation.close();
    }
  }
}
