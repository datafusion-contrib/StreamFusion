package tech.streamfusion.arrow;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.annotation.Internal;

/** The Java ROW value's kind travels beside its SQL fields, outside their logical ordinals. */
@Internal
public final class NestedRowKinds {
  private static final String MARKER = "streamfusion.nested-row-kind";

  private NestedRowKinds() {}

  static Field field(List<Field> fields) {
    String name = "__sf_nested_row_kind";
    while (containsName(fields, name)) name += "_";
    return new Field(
        name, new FieldType(true, new ArrowType.Int(8, true), null, Map.of(MARKER, "1")), null);
  }

  private static boolean containsName(List<Field> fields, String name) {
    return fields.stream().anyMatch(field -> field.getName().equals(name));
  }

  private static boolean marked(Field field) {
    return "1".equals(field.getMetadata().get(MARKER))
        && field.getType().equals(new ArrowType.Int(8, true));
  }

  public static TinyIntVector vector(StructVector row) {
    if (row.size() == 0) return null;
    var last = row.getVectorById(row.size() - 1);
    return marked(last.getField()) ? (TinyIntVector) last : null;
  }

  static List<Field> dataFields(List<Field> fields) {
    return !fields.isEmpty() && marked(fields.get(fields.size() - 1))
        ? fields.subList(0, fields.size() - 1)
        : fields;
  }
}
