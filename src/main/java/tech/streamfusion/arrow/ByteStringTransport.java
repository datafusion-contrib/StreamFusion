package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.HashMap;
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
      Field original = fields.get(column);
      if (!(original.getType() instanceof ArrowType.Utf8) || original.getDictionary() != null) {
        throw new IllegalArgumentException("Byte string transport requires a plain string field");
      }
      var metadata = new HashMap<>(original.getMetadata());
      metadata.put(METADATA_KEY, "1");
      fields.set(
          column,
          new Field(
              original.getName(),
              new FieldType(original.isNullable(), ArrowType.Binary.INSTANCE, null, metadata),
              original.getChildren()));
    }
    return new Schema(fields, schema.getCustomMetadata());
  }

  public static boolean isString(Field field) {
    return field.getType() instanceof ArrowType.Binary
        && "1".equals(field.getMetadata().get(METADATA_KEY));
  }
}
