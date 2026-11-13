package tech.streamfusion.operator;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

final class CanonicalOverValueFixture {
  private CanonicalOverValueFixture() {}

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
      case DECIMAL:
        var decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(BigDecimal.valueOf(value),
            decimal.getPrecision(), decimal.getScale());
      default:
        throw new AssertionError(type);
    }
  }
}
