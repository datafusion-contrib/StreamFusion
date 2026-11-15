package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** A file sees SQL fields; internal nested RowKinds remain an execution-only sidecar. */
public final class ArrowFileBatch implements AutoCloseable {
  private final VectorSchemaRoot root;
  private final boolean owned;

  private ArrowFileBatch(VectorSchemaRoot root, boolean owned) {
    this.root = root;
    this.owned = owned;
  }

  public static Schema schema(Schema schema) {
    List<Field> fields =
        schema.getFields().stream().map(ArrowFileBatch::field).collect(Collectors.toList());
    return fields.equals(schema.getFields())
        ? schema
        : new Schema(fields, schema.getCustomMetadata());
  }

  private static Field field(Field original) {
    List<Field> children =
        original.getChildren().stream()
            .filter(
                child ->
                    !("1".equals(child.getMetadata().get("streamfusion.nested-row-kind"))
                        && child.getType().equals(new ArrowType.Int(8, true))))
            .map(ArrowFileBatch::field)
            .collect(Collectors.toList());
    return children.equals(original.getChildren())
        ? original
        : new Field(original.getName(), original.getFieldType(), children);
  }

  public static ArrowFileBatch of(VectorSchemaRoot input) {
    Schema logical = schema(input.getSchema());
    if (logical == input.getSchema()) return new ArrowFileBatch(input, false);
    List<FieldVector> vectors = new ArrayList<>();
    try {
      for (int i = 0; i < logical.getFields().size(); i++) {
        FieldVector source = input.getVector(i);
        FieldVector target = logical.getFields().get(i).createVector(source.getAllocator());
        vectors.add(target);
        retain(source, target);
      }
      return new ArrowFileBatch(new VectorSchemaRoot(logical, vectors, input.getRowCount()), true);
    } catch (Throwable failure) {
      for (FieldVector vector : vectors) {
        try {
          vector.close();
        } catch (Throwable close) {
          failure.addSuppressed(close);
        }
      }
      throw failure;
    }
  }

  private static void retain(FieldVector source, FieldVector target) {
    target.loadFieldBuffers(
        new ArrowFieldNode(source.getValueCount(), source.getNullCount()),
        source.getFieldBuffers());
    for (FieldVector child : target.getChildrenFromFields()) {
      FieldVector original =
          source.getChildrenFromFields().stream()
              .filter(candidate -> candidate.getName().equals(child.getName()))
              .findFirst()
              .orElseThrow();
      retain(original, child);
    }
  }

  public VectorSchemaRoot root() {
    return root;
  }

  @Override
  public void close() {
    if (owned) root.close();
  }
}
