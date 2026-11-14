package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;

/** Original Java UTF-16 caches beside logical UTF-8 columns; null lists mean binary-only values. */
public final class StringCacheColumns {
  private StringCacheColumns() {}

  public static final String METADATA_KEY = "streamfusion.java_string_cache";

  public static ListVector[] readers(org.apache.arrow.vector.VectorSchemaRoot root, int fields) {
    ListVector[] caches = null;
    for (FieldVector vector : root.getFieldVectors()) {
      var metadata = vector.getField().getMetadata();
      if (metadata != null && "v1".equals(metadata.get(METADATA_KEY))) {
        String name = vector.getName();
        int field = Integer.parseInt(name.substring("$string_java_".length(), name.length() - 1));
        if (field < 0 || field >= fields)
          throw new IllegalArgumentException("Invalid Java string-cache field index");
        if (caches == null) caches = new ListVector[fields];
        caches[field] = (ListVector) vector;
      }
    }
    return caches;
  }

  public static String name(int field) {
    return "$string_java_" + field + "$";
  }

  public static final class Writer implements AutoCloseable {
    private final List<Integer> fields = new ArrayList<>();
    private final List<ListVector> vectors = new ArrayList<>();
    private final List<Utf16Cache.Writer> writers = new ArrayList<>();
    private final BufferAllocator allocator;

    public Writer(RowType type, BufferAllocator allocator) {
      this.allocator = allocator;
      for (int field = 0; field < type.getFieldCount(); field++) {
        switch (type.getTypeAt(field).getTypeRoot()) {
          case CHAR:
          case VARCHAR:
            fields.add(field);
            vectors.add(null);
            writers.add(null);
            break;
          default:
            break;
        }
      }
    }

    private Utf16Cache.Writer allocate(int slot, int position) {
      var vector =
          new ListVector(
              name(fields.get(slot)),
              allocator,
              new FieldType(
                  true, ArrowType.List.INSTANCE, null, java.util.Map.of(METADATA_KEY, "v1")),
              null);
      vectors.set(slot, vector);
      vector.initializeChildrenFromFields(
          List.of(new Field("item", FieldType.notNullable(new ArrowType.Int(16, false)), null)));
      vector.allocateNew();
      for (int previous = 0; previous < position; previous++) vector.setNull(previous);
      var writer = new Utf16Cache.Writer(vector);
      writers.set(slot, writer);
      return writer;
    }

    public void write(RowData row, int position) {
      for (int slot = 0; slot < fields.size(); slot++) {
        int field = fields.get(slot);
        String cache =
            row.isNullAt(field) ? null : ((BinaryStringData) row.getString(field)).getJavaObject();
        var writer = writers.get(slot);
        if (writer == null && cache != null) writer = allocate(slot, position);
        if (writer != null) writer.write(position, cache);
      }
    }

    public List<FieldVector> finish(int rows) {
      var result = new ArrayList<FieldVector>();
      for (ListVector vector : vectors) {
        if (vector == null) continue;
        vector.setValueCount(rows);
        result.add(vector);
      }
      return result;
    }

    @Override
    public void close() {
      for (ListVector vector : vectors) if (vector != null) vector.close();
      vectors.clear();
      writers.clear();
    }
  }

  static BinaryStringData read(ListVector cache, int row, BinaryStringData binary) {
    if (cache == null || cache.isNull(row)) return null;
    return new BinaryStringData(
        binary.getSegments(),
        binary.getOffset(),
        binary.getSizeInBytes(),
        Utf16Cache.read(cache, row));
  }
}
