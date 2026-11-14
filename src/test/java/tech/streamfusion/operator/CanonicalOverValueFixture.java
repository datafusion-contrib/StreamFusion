package tech.streamfusion.operator;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

final class CanonicalOverValueFixture {
  private CanonicalOverValueFixture() {}

  static java.util.List<RowData> physicalRows(java.util.List<RowData> rows, RowType type) {
    var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type);
    return rows.stream()
        .map(row -> (RowData) serializer.toBinaryRow(row).copy())
        .collect(java.util.stream.Collectors.toList());
  }

  static int nativeCode(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return 5;
      case SMALLINT:
        return 4;
      case INTEGER:
        return 2;
      case BIGINT:
        return 0;
      case FLOAT:
        return 6;
      case DOUBLE:
        return 1;
      case CHAR:
      case VARCHAR:
        return 3;
      case DATE:
        return 8;
      case TIME_WITHOUT_TIME_ZONE:
        return 9;
      case BOOLEAN:
        return 10;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 1000
            + org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type);
      case DECIMAL:
        var decimal = (DecimalType) type;
        return NativeWindowOperatorCore.decimalCode(decimal.getPrecision(), decimal.getScale());
      default:
        throw new AssertionError(type);
    }
  }

  static Object number(LogicalType type, Long value) {
    if (value == null) return null;
    switch (type.getTypeRoot()) {
      case TINYINT:
        return value.byteValue();
      case SMALLINT:
        return value.shortValue();
      case INTEGER:
        return value.intValue();
      case BIGINT:
        return value;
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value.doubleValue();
      case CHAR:
      case VARCHAR:
        return StringData.fromString(value % 2 == 0 ? "λ-雪-" + value : "😀-" + value);
      case DATE:
        return value.intValue();
      case TIME_WITHOUT_TIME_ZONE:
        return (int) Math.floorMod(value, 86400000L);
      case BOOLEAN:
        return value % 2 != 0;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromEpochMillis(value, 123456);
      case DECIMAL:
        var decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(
            BigDecimal.valueOf(value), decimal.getPrecision(), decimal.getScale());
      default:
        throw new AssertionError(type);
    }
  }
}
