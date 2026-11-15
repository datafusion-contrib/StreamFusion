package tech.streamfusion.format.csv;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;

/** Exact supported serializer type boundary, independent of the carrying connector. */
final class CsvEncodeGate {
  private CsvEncodeGate() {}

  static String fallbackReason(RowType type) {
    if (type == null) return null;
    for (LogicalType column : type.getChildren()) {
      if (!supportsCsvType(column, false)) return "csv type " + column.asSummaryString();
    }
    return null;
  }

  private static boolean supportsCsvType(LogicalType type, boolean nested) {
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
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case TIME_WITHOUT_TIME_ZONE:
        // Flink's converter prints whatever milliseconds the value carries even under TIME(0),
        // but the Arrow boundary stores a TIME(0) column at second granularity — out-of-contract
        // millisecond data would silently truncate, so only millisecond-preserving precisions run.
        return ((TimeType) type).getPrecision() >= 1;
      case ROW:
      case ARRAY:
        return !nested
            && type.getChildren().stream().allMatch(child -> supportsCsvType(child, true));
      default:
        return false;
    }
  }
}
