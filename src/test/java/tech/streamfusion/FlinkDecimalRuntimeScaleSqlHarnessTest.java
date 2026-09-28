package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalRuntimeScaleSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"ROUND", "TRUNCATE"})
  void extremeNegativeScaleRetainsErrorsAndBooleanShortCircuiting(String function) throws Exception {
    var source = (java.util.function.Supplier<TableEnvironment>) () ->
        BuiltinFunctionParity.environment(ROW(FIELD("d", DECIMAL(20,3)), FIELD("s", INT())),
            List.of(Row.of(new BigDecimal("1.234"), Integer.MIN_VALUE)));
    NativeFailureParity.run(source, "SELECT CAST(" + function + "(d,s) AS STRING) FROM src")
        .assertFailure(ArithmeticException.class, "Underflow", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(source,
        "SELECT s < 0 OR " + function + "(d,s) > 0, s > 0 AND " + function + "(d,s) > 0 FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ROUND", "TRUNCATE"})
  void singleStringConsumerRetainsWideValuesAndPerRowMetadata(String function) throws Exception {
    for (int precision : new int[] {12, 18, 19, 20, 38}) {
      BuiltinFunctionParity.assertParity(() -> environment(precision),
          "SELECT CAST(" + function + "(d,s) AS STRING) FROM src");
    }
    List<Row> rows = new ArrayList<>();
    for (int position : new int[] {-38, -3, -1, 0, 2, 3, 38, Integer.MAX_VALUE}) {
      for (String value : new String[] {"99999999999999999999999999999999999.999",
          "-99999999999999999999999999999999999.999", "0.005", "-0.005", "0.000", null}) {
        rows.add(Row.of(value == null ? null : new BigDecimal(value), position));
      }
    }
    rows.add(Row.of(null, Integer.MIN_VALUE));
    rows.add(Row.of(new BigDecimal("1.234"), null));
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(38,3)), FIELD("s", INT())), rows),
        "SELECT CAST(" + function + "(d,s) AS STRING) FROM src");
    var max = new BigDecimal("99999999999999999999999999999999999999");
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(38,0)), FIELD("s", INT())),
        List.of(Row.of(max, -1), Row.of(max.negate(), -1), Row.of(max, -38), Row.of(max, 0))),
        "SELECT CAST(" + function + "(d,s) AS STRING) FROM src");
  }

  @Test
  void largePrecision38ValuesAndNullsKeepTheirScaleInStringConsumers() throws Exception {
    List<Row> rows = new ArrayList<>();
    for (Integer scale : new Integer[] {-3, 0, 2, 3, 9, null}) {
      for (String value : new String[] {"99999999999999999999999999999999999.999",
          "-99999999999999999999999999999999999.999", "0.005", null}) {
        rows.add(Row.of(value == null ? null : new BigDecimal(value), scale));
      }
    }
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(38,3)), FIELD("s", INT())), rows),
        "SELECT CAST(ROUND(d,s) AS STRING), CAST(TRUNCATE(d,s) AS STRING) FROM src");
  }

  @Test
  void multipleBatchesRetainIndependentValuesAndScaleNulls() throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) {
      rows.add(Row.of(i % 7 == 0 ? null : BigDecimal.valueOf(i - 2501, 3),
          i % 11 == 0 ? null : i % 8 - 3));
    }
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(20,3)), FIELD("s", INT())), rows),
        "SELECT CAST(ROUND(d,s) AS STRING), CAST(TRUNCATE(d,s) AS STRING) FROM src");
  }

  @Test
  void jsonFusionCannotBypassTheUnnormalizedDecimalBoundary() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(20,3)), FIELD("s", INT()), FIELD("id", INT())),
        List.of(Row.of(new BigDecimal("123.456"), 2, 0))),
        "SELECT CAST(ROUND(d,s) AS STRING), ARRAY[TRUNCATE(d,s)], JSON_OBJECT('id' VALUE id) FROM src",
        "verified scalar consumer");
  }

  @ParameterizedTest
  @ValueSource(ints = {12, 18, 19, 20, 38})
  void scalarConsumersPreserveRuntimeScaleAndPrecision(int precision) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(precision),
        "SELECT id, CAST(ROUND(d,s) AS STRING), CAST(TRUNCATE(d,s) AS STRING), "
            + "CAST(ROUND(d,s) AS BIGINT), CAST(TRUNCATE(d,s) AS DECIMAL(38,4)), "
            + "ROUND(d,s) + CAST(1 AS DECIMAL(20,3)), ROUND(d,s) = TRUNCATE(d,s) FROM src");
  }

  @Test
  void nestedAndConditionalConsumersPreserveTheInternalDecimal() throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CAST(COALESCE(ROUND(d,s),d) AS STRING), "
            + "CAST(ROUND(TRUNCATE(d,s),s-1) AS STRING), "
            + "CAST(FLOOR(ROUND(d,s)) AS STRING), IF(id > 0,ROUND(d,s),TRUNCATE(d,s)), "
            + "CAST(CASE WHEN id > 0 THEN ROUND(d,s) ELSE TRUNCATE(d,s) END AS STRING) FROM src");
  }

  @Test
  void directOutputKeepsTheReleasedSerializerContractThroughFallback() throws Exception {
    var source = (java.util.function.Supplier<TableEnvironment>) () ->
        BuiltinFunctionParity.environment(ROW(FIELD("d", DECIMAL(20,3)), FIELD("s", INT())),
            List.of(Row.of(new BigDecimal("123.456"), 2)));
    String sql = "SELECT ROUND(d,s), TRUNCATE(d,s) FROM src";
    try (var rows = source.get().executeSql(sql).collect()) {
      assertEquals(Row.of(new BigDecimal("12.346"), new BigDecimal("12.345")), rows.next());
    }
    NativeParity.assertFallbackReasonContains(source, sql, "runtime scale");
  }

  @Test
  void invalidDirectPrecisionKeepsTheHostFailureThroughFallback() {
    NativeFailureParity.run(() -> BuiltinFunctionParity.environment(
            ROW(FIELD("d", DECIMAL(20,3)), FIELD("s", INT())),
            List.of(Row.of(new BigDecimal("123.456"), 1))),
        "SELECT ROUND(d,s), TRUNCATE(d,s) FROM src")
        .assertFailure(AssertionError.class, "null", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
  }

  @Test
  void unselectedAndFilteredScaleFailuresAreSkipped() throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CASE WHEN id >= 0 THEN CAST(ROUND(d,s) AS STRING) "
            + "ELSE CAST(TRUNCATE(d,1/(id-id)) AS STRING) END FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CAST(ROUND(d,1/(id-id)) AS STRING) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ROUND", "TRUNCATE"})
  void selectedScaleFailuresPropagate(String function) {
    NativeFailureParity.run(() -> environment(20),
        "SELECT CAST(" + function + "(d,1/(id-id)) AS STRING) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(int precision) {
    List<Row> rows = new ArrayList<>();
    Integer[] scales = {-38, -3, -1, 0, 1, 2, 3, 6, 38, Integer.MAX_VALUE, null};
    String[] values = {"123.456", "-123.456", "999.995", "0.005", "-0.005", "0.000", null};
    int id = 0;
    for (Integer scale : scales) {
      for (String value : values) rows.add(Row.of(id++, value == null ? null : new BigDecimal(value), scale));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("d", DECIMAL(precision,3)), FIELD("s", INT())), rows);
  }
}
