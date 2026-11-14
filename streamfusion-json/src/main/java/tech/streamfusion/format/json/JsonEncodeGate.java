package tech.streamfusion.format.json;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

/** Exact supported serializer type boundary, independent of the carrying connector. */
final class JsonEncodeGate {
  private JsonEncodeGate() {}

  static String fallbackReason(RowType type) {
    if (type == null) return null;
    for (LogicalType column : type.getChildren()) {
      if (!supportsJsonType(column)) return "json type " + column.asSummaryString();
    }
    if (!jsonFieldNamesEscapeFreely(type))
      return "a field name needs a JSON control-character escape";
    return null;
  }

  private static boolean jsonFieldNamesEscapeFreely(LogicalType type) {
    if (type instanceof RowType) {
      RowType row = ((RowType) type);

      for (RowType.RowField field : row.getFields()) {
        if (field.getName().chars().anyMatch(c -> c < 0x20)
            || !jsonFieldNamesEscapeFreely(field.getType())) {
          return false;
        }
      }
      return true;
    }
    return type.getChildren().stream().allMatch(JsonEncodeGate::jsonFieldNamesEscapeFreely);
  }

  private static boolean supportsJsonType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case ROW:
      case ARRAY:
        return type.getChildren().stream().allMatch(JsonEncodeGate::supportsJsonType);
      case MAP:
      case MULTISET:
        // Flink's own converter rejects a non-string map key (a MULTISET's element is its key)
        // when the job starts; declining keeps that failure on Flink instead of accepting a
        // schema Flink itself cannot serialize.
        return type.getChildren().get(0).is(LogicalTypeFamily.CHARACTER_STRING)
            && type.getChildren().stream().allMatch(JsonEncodeGate::supportsJsonType);
      default:
        return false;
    }
  }
}
