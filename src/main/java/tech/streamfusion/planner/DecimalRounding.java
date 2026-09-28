package tech.streamfusion.planner;

import java.math.RoundingMode;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;

/** Rounds within Flink's decimal metadata contract without shifting the decimal point twice. */
public final class DecimalRounding {
  private DecimalRounding() {}

  public static DecimalData round(DecimalData value, int position, boolean truncate) {
    if (position >= value.scale()) return value;
    if (position < -38) {
      return truncate ? SqlFunctionUtils.struncate(value, position) : SqlFunctionUtils.sround(value, position);
    }
    int scale = Math.max(position, 0);
    int precision = position < 0
        ? Math.min(38, 1 + value.precision() - value.scale())
        : 1 + value.precision() - value.scale() + position;
    var rounded = value.toBigDecimal().setScale(position,
        truncate ? RoundingMode.DOWN : RoundingMode.HALF_UP);
    return DecimalData.fromBigDecimal(rounded, precision, scale);
  }

  public static StringData text(RowData row, int valueField, int scaleField,
      int precision, int scale, boolean truncate) {
    if (row.isNullAt(valueField) || row.isNullAt(scaleField)) return null;
    DecimalData value = round(row.getDecimal(valueField, precision, scale), row.getInt(scaleField), truncate);
    return value == null ? null : StringData.fromString(value.toString());
  }
}
