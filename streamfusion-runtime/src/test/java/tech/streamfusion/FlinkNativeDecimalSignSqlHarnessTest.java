package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNativeDecimalSignSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "1,0", "1,1", "18,0", "18,3", "18,18", "19,0", "19,3", "19,19", "38,0", "38,18", "38,38"
  })
  void boundariesNullsAndConsumersKeepReleasedTypesAndValues(int precision, int scale)
      throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(precision, scale, true),
        "SELECT id, SIGN(d), SIGN(SIGN(d)), CAST(SIGN(d) AS STRING), SIGN(d)<0, SIGN(d) IS NULL,"
            + " COALESCE(SIGN(d),d), CASE WHEN id<2 THEN SIGN(d) ELSE d END, IF(id<2,SIGN(d),d),"
            + " SIGN(CAST(d AS DECIMAL(38,"
            + scale
            + "))) FROM src");
    String nonnullQuery = "SELECT SIGN(d) FROM src WHERE MOD(id,2)=0";
    if (precision == scale && precision > 18) {
      NativeFailureParity.run(() -> environment(precision, scale, false), nonnullQuery)
          .assertFailure(
              RuntimeException.class,
              "NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    } else {
      BuiltinFunctionParity.assertParity(() -> environment(precision, scale, false), nonnullQuery);
    }
  }

  @ParameterizedTest
  @CsvSource({"1,0", "18,3", "19,3", "19,19", "38,18", "38,38"})
  void consumedNativeBatchesNeedNoJvmEvaluator(int precision, int scale) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      DataType type = inputType(precision, scale, true);
      List<Row> input = rows(precision, scale, 5003, true);
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env, input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      String sql =
          "SELECT SIGN(d), SIGN(SIGN(d)), SIGN(CAST(d AS DECIMAL(38," + scale + "))) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("SIGN") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var results = table.executeSql(sql).collect()) {
        while (results.hasNext()) {
          BigDecimal value = (BigDecimal) input.get(count).getField(1);
          BigDecimal expected =
              value == null || precision == scale && value.signum() != 0
                  ? null
                  : BigDecimal.valueOf(value.signum()).setScale(scale);
          BigDecimal widened =
              value == null || scale == 38 && value.signum() != 0
                  ? null
                  : BigDecimal.valueOf(value.signum()).setScale(scale);
          assertEquals(Row.of(expected, expected, widened), results.next());
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void unverifiedProducersKeepShortCircuitingAndEmptyBatchSemantics() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(18, 3, true),
        "SELECT SIGN(d+CAST(1 AS DECIMAL(18,3))), SIGN(-d), "
            + "SIGN(CAST(d AS DECIMAL(10,2))) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(19, 3, true), "SELECT SIGN(d/(id-id)) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment(38, 18, true),
        "SELECT CASE WHEN id<0 THEN SIGN(d/(id-id)) ELSE SIGN(d) END FROM src");
    NativeFailureParity.run(() -> environment(19, 3, true), "SELECT SIGN(d/(id-id)) FROM src")
        .assertFailure(
            ArithmeticException.class,
            "zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(int precision, int scale, boolean nullable) {
    return BuiltinFunctionParity.environment(
        inputType(precision, scale, nullable), rows(precision, scale, 31, nullable));
  }

  private static DataType inputType(int precision, int scale, boolean nullable) {
    DataType decimal = DECIMAL(precision, scale);
    return ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()));
  }

  private static List<Row> rows(int precision, int scale, int count, boolean nullable) {
    BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {
      max.negate(),
      max.negate().add(BigInteger.ONE),
      BigInteger.ONE.negate(),
      BigInteger.ZERO,
      BigInteger.ONE,
      max,
      null
    };
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      BigInteger value = values[i % (nullable ? values.length : values.length - 1)];
      rows.add(Row.of(i, value == null ? null : new BigDecimal(value, scale)));
    }
    return rows;
  }
}
