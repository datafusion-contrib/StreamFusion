package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Retained Arrow views for logical schemas that exclude internal ROW transport fields. */
public final class ArrowSchemaViews {
  private ArrowSchemaViews() {}

  public static List<Field> dataFields(List<Field> fields) {
    return NestedRowKinds.dataFields(fields).stream()
        .filter(field -> !NestedRepresentation.isBinaryRowBytes(field))
        .collect(Collectors.toList());
  }

  public static Field logicalField(Field field) {
    var children =
        dataFields(field.getChildren()).stream()
            .map(ArrowSchemaViews::logicalField)
            .collect(Collectors.toList());
    return children.equals(field.getChildren())
        ? field
        : new Field(field.getName(), field.getFieldType(), children);
  }

  /** The returned root owns buffer references and can outlive or close independently of input. */
  public static VectorSchemaRoot retain(
      List<FieldVector> input, Schema schema, int rows, BufferAllocator allocator) {
    if (input.size() != schema.getFields().size())
      throw new IllegalArgumentException("Arrow view field count differs from input");
    VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
    try {
      for (int i = 0; i < input.size(); i++) load(input.get(i), result.getVector(i));
      result.setRowCount(rows);
      return result;
    } catch (RuntimeException | Error failure) {
      result.close();
      throw failure;
    }
  }

  private static void load(FieldVector source, FieldVector target) {
    var from = dataChildren(source);
    var to = dataChildren(target);
    if (from.size() != to.size())
      throw new IllegalArgumentException("Arrow view nested field count differs from input");
    target.loadFieldBuffers(
        new ArrowFieldNode(source.getValueCount(), source.getNullCount()),
        source.getFieldBuffers());
    for (int i = 0; i < from.size(); i++) load(from.get(i), to.get(i));
    target.setValueCount(source.getValueCount());
  }

  public static List<FieldVector> dataChildren(FieldVector vector) {
    var children = vector.getChildrenFromFields();
    var fields =
        dataFields(children.stream().map(FieldVector::getField).collect(Collectors.toList()));
    var result = new ArrayList<FieldVector>(fields.size());
    for (FieldVector child : children) if (fields.contains(child.getField())) result.add(child);
    return result;
  }
}
