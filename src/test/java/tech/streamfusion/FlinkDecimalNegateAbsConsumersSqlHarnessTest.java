package tech.streamfusion;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkDecimalNegateAbsConsumersSqlHarnessTest {
  @Test
  void overflowFlagsKeepFinalDefaultsAndPredicateNulls() throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      BuiltinFunctionParity.assertParity(() -> environment(nullable),
          "SELECT CAST(-ABS(d*d) AS STRING),"
              + "-ABS(d*d)>0,-ABS(d*d)<0,-ABS(d*d) IS NULL,"
              + "CASE WHEN -ABS(d*d)<0 THEN 'negative' ELSE 'other' END FROM src");
      BuiltinFunctionParity.assertParity(() -> environment(nullable),
          "SELECT CAST(-ABS(d*d) AS STRING) FROM src WHERE -ABS(d*d)<0");
      BuiltinFunctionParity.assertParity(() -> environment(nullable),
          "SELECT -ABS(d*d) FROM src WHERE -ABS(d*d)<0 OR d=0");
    }
  }

  @Test
  void verifiedArithmeticTryCastPreservesNulls() throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      String sql = "SELECT TRY_CAST(-ABS(d*d) AS CHAR(48)) FROM src";
      BuiltinFunctionParity.assertParity(() -> environment(nullable), sql);
    }
  }

  @Test
  void lazyAndEmptyArithmeticKeepTheGeneratedFailureBoundary() throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(true),
        "SELECT CASE WHEN d IS NOT NULL THEN -ABS(d*d) ELSE -ABS(1/(d-d)) END "
            + "FROM src WHERE d IS NOT NULL");
    BuiltinFunctionParity.assertParity(() -> environment(false),
        "SELECT -ABS(d/(d-d)),CAST(-ABS(d*d) AS STRING) FROM src WHERE d=123");
  }

  private static TableEnvironment environment(boolean nullable) {
    var decimal = DataTypes.DECIMAL(38,18);
    var type = DataTypes.ROW(DataTypes.FIELD("d", nullable ? decimal : decimal.notNull()));
    return BuiltinFunctionParity.environment(type,
        List.of(Row.of(new BigDecimal("99999999999999999999.999999999999999999")),
            Row.of(new BigDecimal("-99999999999999999999.999999999999999999")),
            Row.of(new BigDecimal("-2.000000000000000001")), Row.of(BigDecimal.ZERO)));
  }
}
