package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyFloatingDecimalSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"f", "d"})
  void legacyNumericCastsRunNativelyByDefault(String column) {
    var result = NativeFailureParity.run(() -> {
      var table = environment(true, false);
      return table;
    }, "SELECT CAST(" + column + " AS DECIMAL(10,1)) FROM src");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(result.nativeRun().fallbackReasons().isEmpty(), result::toString);
  }

  @org.junit.jupiter.api.Test
  void legacyMixedProjectionKeepsEveryCast() {
    assertNativeSuccess(NativeFailureParity.run(() -> environment(true, false),
        "SELECT CAST(s AS DECIMAL(10,1)), CAST(id AS DECIMAL(10,1)),"
            + " CAST(CAST(id AS BIGINT) AS DECIMAL(10,1)), CAST(f AS DECIMAL(10,1)),"
            + " CAST(d AS DECIMAL(10,1)), CAST(e AS DECIMAL(32,5)),"
            + " CAST(CAST(e AS DECIMAL(32,5)) AS VARCHAR),"
            + " CAST(CAST(e AS DECIMAL(32,5)) AS INT),"
            + " CAST(CAST(e AS DECIMAL(32,5)) AS BIGINT),"
            + " CAST(CAST(e AS DECIMAL(32,5)) AS FLOAT),"
            + " CAST(CAST(e AS DECIMAL(32,5)) AS DOUBLE), CAST(s AS DECIMAL(38,19)) FROM src"));
  }

  @ParameterizedTest
  @CsvSource({"10,1", "15,4", "38,18", "38,19"})
  void runtimeBatchesMatchTheHostAcrossRoundingBoundaries(int precision, int scale) {
    var random = new java.util.Random(376);
    var input = new ArrayList<Row>();
    for (int i = 0; i < 4096; i++) {
      double value = i < 2048 ? random.nextInt(99999) / 10.0
          : Double.longBitsToDouble(random.nextLong());
      if (!Double.isFinite(value)) value = 0;
      float f = (float) value;
      if (!Float.isFinite(f)) f = Math.copySign(Float.MAX_VALUE, f);
      input.add(Row.of(i, i % 7 == 0 ? null : f, i % 7 == 0 ? null : value,
          "1", new java.math.BigDecimal("1.000000")));
    }
    for (String column : List.of("f", "d")) {
      assertNativeSuccess(NativeFailureParity.run(() -> environment(true, input),
          "SELECT id, CAST(" + column + " AS DECIMAL(" + precision + "," + scale + ")) FROM src"));
    }
  }

  @org.junit.jupiter.api.Test
  void composedCastOperandsRetainTheirGeneratedEvaluation() {
    assertNativeSuccess(NativeFailureParity.run(() -> environment(true, false),
        "SELECT CAST(f * CAST(10 AS FLOAT) AS DECIMAL(10,1)) FROM src WHERE id<10"));
    assertNativeSuccess(NativeFailureParity.run(() -> environment(true, false),
        "SELECT CAST(d + CAST(1 AS DOUBLE) AS DECIMAL(15,4)) FROM src WHERE id<10"));
    NativeFailureParity.run(() -> environment(true, false),
        "SELECT CAST(f * CAST(10 AS FLOAT) AS DECIMAL(10,1)) FROM src WHERE id=12")
        .assertFailure(NumberFormatException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void competingFailuresDoNotRequireWholeCalcJvmEvaluation(boolean legacy) {
    for (String projection :
        List.of("1/id, CAST(f AS DECIMAL(10,1))", "CAST(f AS DECIMAL(10,1)), 1/id")) {
      NativeFailureParity.run(() -> environment(legacy, true), "SELECT " + projection + " FROM src")
          .assertCompetingFailures(
              cause ->
                  cause instanceof NumberFormatException
                      || NativeFailureParity.isDivisionByZero(cause),
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @CsvSource({"f,false", "f,true", "d,false", "d,true"})
  void finiteValuesKeepRoundingOverflowAndNulls(String column, boolean legacy) {
    var result = NativeFailureParity.run(() -> environment(legacy, false),
        "SELECT id, CAST(" + column + " AS DECIMAL(10,1)), CAST(" + column
            + " AS DECIMAL(15,4)), CAST(" + column + " AS DECIMAL(38,19)) FROM src");
    assertNativeSuccess(result);
    assertEquals(new java.math.BigDecimal(column.equals("f") ? "1.5" : "1.6"),
        result.nativeRun().rows().get(0).get(2));
  }

  @ParameterizedTest
  @CsvSource({"f,false", "f,true", "d,false", "d,true"})
  void nonFiniteInputsKeepTheHostsFailureAndSelection(String column, boolean legacy) {
    for (String predicate : List.of(" WHERE id=0", " WHERE id=1", " WHERE id=2")) {
      NativeFailureParity.run(() -> environment(legacy, true),
          "SELECT CAST(" + column + " AS DECIMAL(10,1)) FROM src" + predicate)
          .assertFailure(NumberFormatException.class, NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
    for (String sql : List.of(
        "SELECT CAST(" + column + " AS DECIMAL(10,1)) FROM src WHERE id=99",
        "SELECT CASE WHEN id=99 THEN CAST(" + column + " AS DECIMAL(10,1)) ELSE NULL END FROM src")) {
      assertNativeSuccess(NativeFailureParity.run(() -> environment(legacy, true), sql));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void legacyCastsKeepMixedProjectionsAcrossRecovery(String backend) {
    var type = rowType();
    try (var recovery = new PortableSqlRecovery(backend, rows(false), ExternalTypeInfo.<Row>of(type),
        Schema.newBuilder().fromRowDataType(type).build(), false, 5, 10).withExecutionMetrics()) {
      java.util.function.Function<TableEnvironment, TableEnvironment> configure = table -> {
        table.getConfig().set("table.exec.legacy-cast-behaviour", "ENABLED");
        return table;
      };
      var result = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()),
          "SELECT id, CAST(f AS DECIMAL(10,1)), CAST(d AS DECIMAL(15,4)),"
              + " CAST(s AS DECIMAL(38,19)), CAST(id AS DECIMAL(10,1)),"
              + " CAST(CAST(id AS BIGINT) AS DECIMAL(10,1)) FROM recovery_input");
      assertNativeSuccess(result);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
    }
  }

  private static void assertNativeSuccess(NativeFailureParity.Comparison result) {
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertTrue(result.nativeRun().plan().contains("NativeCalc"), result::toString);
    assertTrue(result.nativeRun().fallbackReasons().isEmpty(), result::toString);
  }

  private static org.apache.flink.table.types.DataType rowType() {
    return ROW(FIELD("id", INT()), FIELD("f", FLOAT()), FIELD("d", DOUBLE()), FIELD("s", STRING()),
        FIELD("e", DECIMAL(10,6)));
  }

  private static List<Row> rows(boolean nonFinite) {
    List<Double> values = nonFinite
        ? Arrays.asList(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        : Arrays.asList(1.55, 1.45, 1.05, -1.55, -1.45, 0.0, -0.0, 0.1,
            Math.nextDown(1.55), Math.nextUp(1.55), 999999999.95, -999999999.95,
            Double.MAX_VALUE, Double.MIN_VALUE, (double) Float.MIN_VALUE, null);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      Double value = values.get(i);
      // Keep finite FLOAT overflow separate from DOUBLE overflow and the non-finite failure tests.
      Float f = value == null ? null
          : !nonFinite && Math.abs(value) > Float.MAX_VALUE
              ? Math.copySign(Float.MAX_VALUE, value.floatValue()) : value.floatValue();
      rows.add(Row.of(i, f, value, "1.0123456789012345678", new java.math.BigDecimal("3.523974")));
    }
    return rows;
  }

  private static TableEnvironment environment(boolean legacy, boolean nonFinite) {
    return environment(legacy, rows(nonFinite));
  }

  private static TableEnvironment environment(boolean legacy, List<Row> rows) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var type = rowType();
    table.createTemporaryView("src", table.fromDataStream(fromData(env, rows,
        ExternalTypeInfo.<Row>of(type)), Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }
}
