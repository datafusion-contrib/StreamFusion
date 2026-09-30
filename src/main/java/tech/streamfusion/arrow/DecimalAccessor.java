package tech.streamfusion.arrow;

import java.math.BigDecimal;
import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.table.data.DecimalData;

/** Carries already-evaluated Flink decimals without applying a second precision check. */
public final class DecimalAccessor {
  private DecimalAccessor() {}

  public static DecimalData fromInternalValue(BigDecimal value, int precision, int scale) {
    if (value == null) return null;
    if (DecimalData.isCompact(precision)) {
      return DecimalData.fromUnscaledLong(value.unscaledValue().longValueExact(), precision, scale);
    }
    return DecimalData.fromBigDecimal(
        value.scale() == scale ? value : new BigDecimal(value.unscaledValue(), scale),
        precision, scale);
  }

  public static void set(DecimalVector vector, int row, DecimalData value) {
    if (value != null && value.scale() != vector.getScale()) {
      value =
          DecimalData.fromBigDecimal(
              value.toBigDecimal(), vector.getPrecision(), vector.getScale());
    }
    if (value == null) {
      vector.setNull(row);
    } else if (value.isCompact()) {
      // Flink's compact text parser may retain a rounding carry beyond the declared precision.
      vector.setSafe(row, value.toUnscaledLong());
    } else {
      vector.setSafe(row, value.toBigDecimal());
    }
  }
}
