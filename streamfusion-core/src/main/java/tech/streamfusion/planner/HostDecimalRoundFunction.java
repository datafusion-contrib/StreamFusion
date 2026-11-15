package tech.streamfusion.planner;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.functions.ScalarFunction;

/** Preserves BigDecimal's scale/range failures for extreme negative ROUND positions. */
public final class HostDecimalRoundFunction extends ScalarFunction {
  private static final long serialVersionUID = 1L;
  private final int precision;
  private final int scale;
  private final int position;

  public HostDecimalRoundFunction(int precision, int scale, int position) {
    this.precision = precision;
    this.scale = scale;
    this.position = position;
  }

  public BigDecimal eval(BigDecimal input) {
    if (input == null) {
      return null;
    }
    DecimalData result =
        DecimalDataUtils.sround(DecimalData.fromBigDecimal(input, precision, scale), position);
    return result == null ? null : result.toBigDecimal();
  }
}
