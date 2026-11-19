package tech.streamfusion;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

/** Uses released logical types for schemas that SQL DDL cannot declare, including intervals. */
public final class CanonicalTestSchemas {
  private CanonicalTestSchemas() {}

  public static Schema.Builder columns(String... columns) {
    if (columns.length % 2 != 0) throw new IllegalArgumentException("Expected name/type pairs");
    var schema = Schema.newBuilder();
    for (int index = 0; index < columns.length; index += 2)
      schema.column(
          columns[index],
          TypeConversions.fromLogicalToDataType(
              LogicalTypeParser.parse(
                  columns[index + 1], CanonicalTestSchemas.class.getClassLoader())));
    return schema;
  }
}
