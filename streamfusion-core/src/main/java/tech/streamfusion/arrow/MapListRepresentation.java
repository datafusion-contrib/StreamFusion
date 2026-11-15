package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** IPC-safe map storage: List and Map share buffers, while List permits nullable keys. */
public final class MapListRepresentation {
  private static final String MARKER = "streamfusion.map-list";

  private MapListRepresentation() {}

  public static boolean requiredForCounts(RowType rowType, int[] columns, int[] kinds) {
    for (int i = 0; i < columns.length; i++) {
      if (kinds[i] != 3 || columns[i] < 0) continue;
      switch (rowType.getTypeAt(columns[i]).getTypeRoot()) {
        case ARRAY:
        case MAP:
        case MULTISET:
        case ROW:
          return containsMap(rowType);
        default:
          break;
      }
    }
    return false;
  }

  public static boolean containsMap(LogicalType type) {
    return type.getTypeRoot() == LogicalTypeRoot.MAP
        || type.getTypeRoot() == LogicalTypeRoot.MULTISET
        || type.getChildren().stream().anyMatch(MapListRepresentation::containsMap);
  }

  public static boolean isMapList(Field field) {
    return field.getType() instanceof ArrowType.List
        && ("1".equals(field.getMetadata().get(MARKER))
            || field.getChildren().size()==1
                && "1".equals(field.getChildren().get(0).getMetadata().get(MARKER)));
  }

  /** A borrowed export view; close the original root, never this view's shared vectors. */
  public static VectorSchemaRoot forExport(VectorSchemaRoot root) {
    boolean hasMap = false;
    for (Field field : root.getSchema().getFields()) hasMap |= containsMap(field);
    if (!hasMap) return root;
    return new VectorSchemaRoot(
        forIpc(root.getSchema()), root.getFieldVectors(), root.getRowCount());
  }

  private static boolean containsMap(Field field) {
    if (field.getType() instanceof ArrowType.Map) return true;
    for (Field child : field.getChildren()) if (containsMap(child)) return true;
    return false;
  }

  public static Schema forIpc(Schema schema) {
    List<Field> fields = new ArrayList<>();
    for (Field field : schema.getFields()) fields.add(forIpc(field));
    return fields.equals(schema.getFields())
        ? schema
        : new Schema(fields, schema.getCustomMetadata());
  }

  private static Field forIpc(Field field) {
    List<Field> children = new ArrayList<>();
    for (Field child : field.getChildren()) children.add(forIpc(child));
    FieldType type = field.getFieldType();
    if (field.getType() instanceof ArrowType.Map) {
      Field entry = children.get(0);
      var entries = new ArrayList<>(entry.getChildren());
      Field key = entries.get(0);
      entries.set(
          0,
          new Field(
              key.getName(),
              new FieldType(true, key.getType(), key.getDictionary(), key.getMetadata()),
              key.getChildren()));
      children.set(0, new Field(entry.getName(), entry.getFieldType(), entries));
      var metadata = new HashMap<>(field.getMetadata());
      metadata.put(MARKER, "1");
      type =
          new FieldType(
              field.isNullable(), ArrowType.List.INSTANCE, field.getDictionary(), metadata);
    }
    return new Field(field.getName(), type, children);
  }
}
