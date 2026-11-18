package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkMixedDecimalArithmeticSqlHarnessTest {
  @Test
  void implicitIntegerConversionPreservesTheReleasedDivisionFailure() {
    var runs = NativeFailureParity.run(
        () -> environment(List.of(Row.of(new BigDecimal("0.1"), 10L))),
        "SELECT a / n FROM src");
    if (tech.streamfusion.compat.FlinkCompat.DECIMAL_ARITHMETIC_CASTS_INTEGERS_TO_RESULT) {
      runs.assertNullDereferenceFailure("toBigDecimal",
          NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    } else {
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    }
    assertTrue(runs.nativeRun().plan().contains("jvmFunctions=[]"), runs.nativeRun()::plan);
  }

  @Test
  void nullOperandsSkipImplicitConversionAndValidIntegersRemainExact() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(List.of(
            Row.of(null, 10L), Row.of(new BigDecimal("0.1"), null),
            Row.of(new BigDecimal("0.1"), 9L), Row.of(new BigDecimal("-0.1"), -9L))),
        "SELECT a / n, a + n, n - a, a * n, MOD(a,n), MOD(n,a) FROM src");
  }

  @Test
  void unselectedAndEmptyBatchesSkipTheOverflowingImplicitConversion() throws Exception {
    var rows = List.of(Row.of(new BigDecimal("0.1"), 10L));
    BuiltinFunctionParity.assertParity(() -> environment(rows),
        "SELECT IF(n < 10, a / n, a) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(rows),
        "SELECT a / n FROM src WHERE n = 99");
  }

  private static TableEnvironment environment(List<Row> rows) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("a", DECIMAL(38, 37)), FIELD("n", BIGINT())), rows);
  }
}
