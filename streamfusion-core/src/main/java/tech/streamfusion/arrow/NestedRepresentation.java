package tech.streamfusion.arrow;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Physical children that preserve Flink value semantics without changing SQL field ordinals. */
public final class NestedRepresentation {
  private static final String STRING = "streamfusion.nested-string";
  private static final String BINARY_ROW_BYTES = "streamfusion.nested-row-binary";
  private static final String KIND = "streamfusion.nested-row-kind";
  public static final String ARRAY = "streamfusion.array-copy";
  public static final String JSON_DOCUMENT = "streamfusion.json-document";
  private static final String ELEMENT = "streamfusion.array-element";
  public static final int BINARY_ROW = 4;
  public static final int BINARY_ARRAY = 8;
  public static final int NESTED_ROW = 16;

  private NestedRepresentation() {}

  public static Field stringField(String name, boolean nullable) {
    return new Field(
        name,
        new FieldType(nullable, ArrowType.Struct.INSTANCE, null, Map.of(STRING, "1")),
        List.of(
            new Field(
                "utf8",
                new FieldType(true, ArrowType.Utf8.INSTANCE, null, Map.of(STRING, "1")),
                null),
            new Field(
                "java",
                FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(
                    new Field("unit", FieldType.notNullable(new ArrowType.Int(16, false)), null))),
            new Field("array_copy_class", FieldType.nullable(new ArrowType.Int(8, true)), null)));
  }

  public static boolean isString(Field field) {
    return field.getType() instanceof ArrowType.Struct
        && (field.getChildren().size() == 2 || field.getChildren().size() == 3)
        && "1".equals(field.getChildren().get(0).getMetadata().get(STRING))
        && field.getChildren().get(0).getType() instanceof ArrowType.Utf8
        && field.getChildren().get(0).getName().equals("utf8")
        && field.getChildren().get(1).getName().equals("java")
        && field.getChildren().get(1).getType() instanceof ArrowType.List;
  }

  public static boolean requiresView(
      org.apache.flink.table.data.columnar.vector.ColumnVector vector) {
    return vector instanceof NestedStringColumnVector
        || vector instanceof tech.streamfusion.arrow.vectors.ArrowRowColumnVector
            && ((tech.streamfusion.arrow.vectors.ArrowRowColumnVector) vector)
                .preservesRepresentation();
  }

  public static boolean binaryArray(
      org.apache.flink.table.data.columnar.vector.ColumnVector vector, int index) {
    return vector instanceof NestedStringColumnVector
        ? ((NestedStringColumnVector) vector).binaryArray(index)
        : vector instanceof tech.streamfusion.arrow.vectors.ArrowRowColumnVector
            && ((tech.streamfusion.arrow.vectors.ArrowRowColumnVector) vector).binaryArray(index);
  }

  public static Field elementField(Field value) {
    return new Field(
        "element",
        FieldType.notNullable(ArrowType.Struct.INSTANCE),
        List.of(
            value,
            new Field(
                "array_copy_class",
                new FieldType(true, new ArrowType.Int(8, true), null, Map.of(ELEMENT, "1")),
                null)));
  }

  public static boolean isElement(Field field) {
    return field.getType() instanceof ArrowType.Struct
        && field.getChildren().size() == 2
        && "value".equals(field.getChildren().get(0).getName())
        && "1".equals(field.getChildren().get(1).getMetadata().get(ELEMENT));
  }

  public static boolean contains(Field field) {
    return isString(field)
        || "1".equals(field.getMetadata().get(KIND))
        || "1".equals(field.getMetadata().get(ARRAY))
        || field.getChildren().stream().anyMatch(NestedRepresentation::contains);
  }

  public static Field kindField(List<Field> fields) {
    String name = "__sf_nested_row_kind";
    while (containsName(fields, name)) name += "_";
    return new Field(
        name, new FieldType(true, new ArrowType.Int(8, true), null, Map.of(KIND, "1")), null);
  }

  /** Adds optional owned binary payloads only where a ROW already carries its representation. */
  public static org.apache.arrow.vector.types.pojo.Schema withBinaryRows(
      org.apache.arrow.vector.types.pojo.Schema schema) {
    return new org.apache.arrow.vector.types.pojo.Schema(
        schema.getFields().stream()
            .map(NestedRepresentation::withBinaryRows)
            .collect(java.util.stream.Collectors.toList()),
        schema.getCustomMetadata());
  }

  private static Field withBinaryRows(Field field) {
    var children =
        field.getChildren().stream()
            .map(NestedRepresentation::withBinaryRows)
            .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
    if (!children.isEmpty()
        && "1".equals(children.get(children.size() - 1).getMetadata().get(KIND))
        && children.stream().noneMatch(NestedRepresentation::isBinaryRowBytes)) {
      String name = "__sf_binary_row";
      while (containsName(children, name)) name += "_";
      children.add(
          children.size() - 1,
          new Field(
              name,
              new FieldType(true, ArrowType.Binary.INSTANCE, null, Map.of(BINARY_ROW_BYTES, "1")),
              null));
    }
    return new Field(field.getName(), field.getFieldType(), children);
  }

  public static boolean isBinaryRowBytes(Field field) {
    return field.getType() instanceof ArrowType.Binary
        && "1".equals(field.getMetadata().get(BINARY_ROW_BYTES));
  }

  public static org.apache.arrow.vector.VarBinaryVector binaryRows(StructVector vector) {
    for (var child : vector.getChildrenFromFields())
      if (isBinaryRowBytes(child.getField()))
        return (org.apache.arrow.vector.VarBinaryVector) child;
    return null;
  }

  private static boolean containsName(List<Field> fields, String name) {
    return fields.stream().anyMatch(field -> field.getName().equals(name));
  }

  public static TinyIntVector kinds(StructVector vector) {
    if (vector.size() == 0) return null;
    var last = vector.getVectorById(vector.size() - 1);
    return "1".equals(last.getField().getMetadata().get(KIND)) && last instanceof TinyIntVector
        ? (TinyIntVector) last
        : null;
  }
}
