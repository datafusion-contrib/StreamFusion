package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkExactNumericBuiltinsSqlHarnessTest {
  @Test
  void exactUnaryFunctionsPreserveTypesNullsAndOverflow() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT -i, -n, -d, ABS(i), ABS(n), ABS(d), SIGN(i), SIGN(n), SIGN(d), "
            + "FLOOR(i), FLOOR(n), CEIL(i), CEIL(n), TRUNCATE(i), TRUNCATE(n, -1) FROM src");
  }

  @Test
  void decimalConsumersAndFilteredBatchesRetainExactValues() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT ABS(-d), ROUND(-d, 2), MOD(-d, 3), TRUNCATE(i, digits), "
            + "TRUNCATE(n, digits) FROM src WHERE i IS NOT NULL");
    BuiltinFunctionParity.assertParity(
        this::environment, "SELECT ABS(-d), SIGN(n) FROM src WHERE i = 99");
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("i", INT()), FIELD("n", BIGINT()), FIELD("d", DECIMAL(38, 9)),
            FIELD("digits", INT())),
        List.of(
            Row.of(-3, -4L, new BigDecimal("-12.345000000"), -1),
            Row.of(Integer.MIN_VALUE, Long.MIN_VALUE,
                new BigDecimal("-99999999999999999999999999999.999999999"), 0),
            Row.of(Integer.MAX_VALUE, Long.MAX_VALUE, new BigDecimal("0.000000001"), 2),
            Row.of(0, 0L, BigDecimal.ZERO.setScale(9), -2),
            Row.of(null, null, null, null)));
  }
}
