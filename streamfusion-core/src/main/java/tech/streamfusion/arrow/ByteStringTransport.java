package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Lossless physical storage for logical strings with verified byte-capable consumers. */
public final class ByteStringTransport {
  public static final String METADATA_KEY = "streamfusion.logical_string";

  private ByteStringTransport() {}

  public static Schema inputSchema(Schema schema, int[] columns) {
    var fields = new ArrayList<>(schema.getFields());
    for (int column : columns) {
      fields.set(column, retag(fields.get(column), true));
    }
    return new Schema(fields, schema.getCustomMetadata());
  }

  public static boolean isString(Field field) {
    return field.getType() instanceof ArrowType.Binary
        && "1".equals(field.getMetadata().get(METADATA_KEY));
  }

  /** Schema for the same buffers after the indicated byte-backed arrays have been validated. */
  public static Schema utf8Schema(Schema schema, int[] columns) {
    var fields = new ArrayList<>(schema.getFields());
    for (int column : columns) {
      fields.set(column, retag(fields.get(column), false));
    }
    return new Schema(fields, schema.getCustomMetadata());
  }

  /** The exact evaluator can return arbitrary bytes at any string leaf of a row result. */
  public static Schema resultSchema(Schema schema) {
    return new Schema(
        schema.getFields().stream().map(ByteStringTransport::binaryStrings).collect(Collectors.toList()),
        schema.getCustomMetadata());
  }

  private static Field binaryStrings(Field field) {
    if (field.getType() instanceof ArrowType.Utf8) return retag(field, true);
    if (field.getChildren().isEmpty()) return field;
    return new Field(field.getName(), field.getFieldType(),
        field.getChildren().stream().map(ByteStringTransport::binaryStrings).collect(Collectors.toList()));
  }

  public static FieldVector bytes(FieldVector vector) {
    return NestedRepresentation.isString(vector.getField())
        ? ((StructVector) vector).getChild("utf8")
        : vector;
  }

  private static Field retag(Field field, boolean binary) {
    if (NestedRepresentation.isString(field)) {
      var children = new ArrayList<>(field.getChildren());
      children.set(0, retag(children.get(0), binary));
      return new Field(field.getName(), field.getFieldType(), children);
    }
    if (field.getDictionary() != null
        || !(binary
            ? field.getType() instanceof ArrowType.Utf8 || field.getType() instanceof ArrowType.Binary
            : isString(field))) {
      throw new IllegalArgumentException("Expected a plain or represented STRING field");
    }
    var metadata = new HashMap<>(field.getMetadata());
    if (binary) metadata.put(METADATA_KEY, "1");
    else metadata.remove(METADATA_KEY);
    return new Field(field.getName(),
        new FieldType(field.isNullable(),
            binary ? ArrowType.Binary.INSTANCE : ArrowType.Utf8.INSTANCE, null, metadata), null);
  }
}
