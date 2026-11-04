package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalBooleanTryCastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void directDecimalTruthPreservesPrecisionScaleNullsAndConsumers(int precision, int scale)
      throws Exception {
    String truth = "TRY_CAST(d AS BOOLEAN)";
    String ordinary = scale < precision ? "CAST(d AS BOOLEAN)," : "";
    String sql = "SELECT id," + truth + "," + ordinary + "NOT " + truth + ","
        + truth + " IS NULL," + truth + " IS TRUE,CASE WHEN MOD(id,3)=0 THEN "
        + truth + " ELSE CAST(NULL AS BOOLEAN) END FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        verify(precision, scale, nullable, legacy, sql, true);
        verify(precision, scale, nullable, legacy, sql + " WHERE id<0", true);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 18, 19, 38})
  void ordinaryCastWithNoIntegralDigitsRetainsReleasedPlanningFailure(int precision)
      throws Exception {
    for (boolean legacy : new boolean[] {false, true}) {
      java.util.function.Supplier<TableEnvironment> factory = () -> {
        var table = BuiltinFunctionParity.environment(ROW(FIELD("d", DECIMAL(precision, precision))),
            List.of(Row.of(BigDecimal.ZERO.setScale(precision))));
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
        return table;
      };
      NativeFailureParity.run(factory, "SELECT CAST(d AS BOOLEAN) FROM src")
          .assertFailure(IllegalArgumentException.class, "Cannot convert 0",
              NativeFailureParity.Phase.PLANNING, NativeFailureParity.Route.UNPLANNED);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filtersAndUnverifiedConsumersKeepTheirContracts(boolean legacy) throws Exception {
    verify(18, 3, true, legacy,
        "SELECT id,TRY_CAST(TRY_CAST(d AS BOOLEAN) AS CHAR(8)) FROM src", true);
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = BuiltinFunctionParity.environment(ROW(FIELD("id", INT().notNull()),
          FIELD("d", DECIMAL(18, 3))),
          List.of(Row.of(0, new BigDecimal("2.001")), Row.of(1, BigDecimal.ZERO), Row.of(2, null)));
      table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
      return table;
    };
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,TRY_CAST(d AS BOOLEAN) FROM src WHERE TRY_CAST(d AS BOOLEAN)");
    for (String sql : List.of("SELECT TRY_CAST(d+d AS BOOLEAN) FROM src",
        "SELECT TRY_CAST(d/(id-id) AS BOOLEAN) FROM src WHERE id<0",
        "SELECT CASE WHEN id>=0 THEN TRY_CAST(d AS BOOLEAN) "
            + "ELSE TRY_CAST(d/(id-id) AS BOOLEAN) END FROM src")) {
      NativeParity.assertFallbackReasonContains(factory, sql, "unsupported TRY_CAST DECIMAL");
    }
    NativeFailureParity.run(factory, "SELECT TRY_CAST(d/(id-id) AS BOOLEAN) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
  }

  static void verify(int precision, int scale, boolean nullable, boolean legacy,
      String sql, boolean pure) throws Exception {
    var decimal = DECIMAL(precision, scale);
    var type = ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()));
    BigInteger maximum = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {maximum.negate(), maximum.negate().add(BigInteger.ONE),
        BigInteger.ONE.negate(), BigInteger.ZERO, BigInteger.ONE, maximum};
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
        table.createTemporaryView("src", env.fromSequence(0, 5002).map(index ->
            (RowData) GenericRowData.of(index.intValue(), nullable && index % 37 == 0 ? null
                : DecimalData.fromBigDecimal(new BigDecimal(
                    values[index.intValue() % values.length], scale), precision, scale)))
            .returns(InternalTypeInfo.of((RowType) type.getLogicalType())));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        if (nativeRun) assertEquals(expectedTypes, types);
        else expectedTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var row = output.next();
            var fields = new ArrayList<Object>();
            for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
            assertNull(actual.put((Integer) row.getField(0), fields));
          }
        }
        assertEquals(sql.contains("WHERE id<0") ? 0 : 5003, actual.size());
        if (nativeRun) {
          assertEquals(expected, actual, sql);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
