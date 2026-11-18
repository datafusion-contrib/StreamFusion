package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyCharDoubleSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void legacyNumericCastsRunNativelyByDefault() {
    var result = NativeFailureParity.run(() -> {
      var table = input(true, 8, false);
      return table;
    }, "SELECT CAST(c AS DOUBLE) FROM src");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(result.nativeRun().fallbackReasons().isEmpty(), result::toString);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8, 32})
  void legacyCharactersKeepNumericParsingAndNullOnFailure(int length) {
    compare(() -> input(true, length, true), "SELECT id, CAST(c AS DOUBLE) FROM src");
    compare(() -> input(true, length, true), "SELECT id FROM src WHERE CAST(c AS DOUBLE)>0");
  }

  @org.junit.jupiter.api.Test
  void decimalTextBatchesMatchTheHostsCorrectlyRoundedParser() {
    var random = new java.util.Random(377);
    var input = new ArrayList<Row>();
    for (int i = 0; i < 4096; i++) {
      double value = Double.longBitsToDouble(random.nextLong());
      if (!Double.isFinite(value)) value = 0;
      input.add(Row.of(i, i % 7 == 0 ? null : " \t" + Double.toString(value) + " "));
    }
    var type = ROW(FIELD("id", INT()), FIELD("c", CHAR(32)));
    compare(() -> table(true, type, input), "SELECT id, CAST(c AS DOUBLE) FROM src");
  }

  @org.junit.jupiter.api.Test
  void decimalTextKeepsHalfwaySubnormalAndOverflowRounding() {
    String[] values = {"9007199254740993", "9007199254740995", "2.2250738585072011e-308",
        "2.2250738585072012e-308", "2.4703282292062327e-324", "2.4703282292062328e-324",
        "1.7976931348623157e308", "1.7976931348623159e308",
        "1.00000000000000011102230246251565404236316680908203125",
        "1.00000000000000011102230246251565404236316680908203126",
        ".5", "1.", "+1e-400", "-1e-400", "\u0000-0.0\u001f"};
    var input = new ArrayList<Row>();
    for (int i = 0; i < values.length; i++) input.add(Row.of(i, values[i]));
    var type = ROW(FIELD("id", INT()), FIELD("c", CHAR(128)));
    compare(() -> table(true, type, input), "SELECT id, CAST(c AS DOUBLE) FROM src");
  }

  @org.junit.jupiter.api.Test
  void composedCharacterOperandsRetainTheirGeneratedEvaluation() {
    var type = ROW(FIELD("id", INT()), FIELD("c", CHAR(8)));
    compare(() -> table(true, type, List.of(Row.of(0, "2.5"), Row.of(1, "bad"), Row.of(2, null))),
        "SELECT CAST(CAST(CONCAT('1', c) AS CHAR(8)) AS DOUBLE) FROM src");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void validValuesAndSuppressedFailuresRetainBothCastModes(boolean legacy) {
    compare(() -> input(legacy, 8, false), "SELECT id, CAST(c AS DOUBLE) FROM src");
    compare(() -> input(legacy, 8, true), "SELECT CAST(c AS DOUBLE) FROM src WHERE id=99");
    compare(() -> input(legacy, 8, true),
        "SELECT CASE WHEN id=99 THEN CAST(c AS DOUBLE) ELSE NULL END FROM src");
  }

  @org.junit.jupiter.api.Test
  void defaultMalformedCharacterCastStillFails() {
    NativeFailureParity.run(() -> input(false, 8, true),
        "SELECT CAST(c AS DOUBLE) FROM src WHERE id=1")
        .assertFailure(NumberFormatException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @org.junit.jupiter.api.Test
  void legacyConversionDoesNotSwallowOperandFailures() {
    NativeFailureParity.run(() -> input(true, 8, false),
        "SELECT CAST(CAST(1/id AS CHAR(8)) AS DOUBLE) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @org.junit.jupiter.api.Test
  void roundProjectionKeepsItsSortKeyAndAllDecimalWidths() {
    String decimals = "ROUND(CAST(a AS DECIMAL(7,3))), ROUND(CAST(b AS DECIMAL(7,3)),2),"
        + " ROUND(CAST(c AS DECIMAL(7,3)),3), ROUND(CAST(d AS DECIMAL(7,3)),6),"
        + " ROUND(CAST(e AS DECIMAL(38,3))), ROUND(CAST(f AS DECIMAL(38,3)),2),"
        + " ROUND(CAST(g AS DECIMAL(38,3)),3), ROUND(CAST(h AS DECIMAL(38,3)),6)";
    compare(FlinkLegacyCharDoubleSqlHarnessTest::roundInput,
        "SELECT CAST(a AS DOUBLE), " + decimals + " FROM src");
    compare(FlinkLegacyCharDoubleSqlHarnessTest::roundInput, "SELECT " + decimals + " FROM src");
    compare(() -> {
      var type = ROW(FIELD("a", DECIMAL(7,3)), FIELD("b", DECIMAL(7,3)),
          FIELD("c", DECIMAL(7,3)), FIELD("d", DECIMAL(7,3)), FIELD("e", DECIMAL(38,3)),
          FIELD("f", DECIMAL(38,3)), FIELD("g", DECIMAL(38,3)), FIELD("h", DECIMAL(38,3)));
      return table(true, type, List.of(Row.of(new java.math.BigDecimal("1234.567"),
          new java.math.BigDecimal("2345.678"), new java.math.BigDecimal("3456.789"),
          new java.math.BigDecimal("4567.890"), new java.math.BigDecimal("112233445.512"),
          new java.math.BigDecimal("223344556.623"), new java.math.BigDecimal("334455667.734"),
          new java.math.BigDecimal("11112222333344445555666677778888999.912"))));
    }, "SELECT ROUND(a), ROUND(b,2), ROUND(c,3), ROUND(d,6),"
        + " ROUND(e), ROUND(f,2), ROUND(g,3), ROUND(h,6) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void legacyCharacterCastSurvivesRepeatedRecovery(String backend) {
    var type = ROW(FIELD("id", INT()), FIELD("c", CHAR(8)));
    try (var recovery = new PortableSqlRecovery(backend, rows(true), ExternalTypeInfo.<Row>of(type),
        Schema.newBuilder().fromRowDataType(type).build(), false, 4, 8).withExecutionMetrics()) {
      java.util.function.Function<TableEnvironment, TableEnvironment> configure = table -> {
        table.getConfig().set("table.exec.legacy-cast-behaviour", "ENABLED");
        return table;
      };
      var result = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()),
          "SELECT id, CAST(c AS DOUBLE) FROM recovery_input");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
    }
  }

  private static void compare(java.util.function.Supplier<TableEnvironment> source, String sql) {
    var result = NativeFailureParity.run(source, sql);
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertTrue(result.nativeRun().plan().contains("NativeCalc")
        || result.nativeRun().plan().contains("NativeFilter"), result::toString);
    assertTrue(result.nativeRun().fallbackReasons().isEmpty(), result::toString);
  }

  private static List<Row> rows(boolean malformed) {
    String[] values = {"1234.567", malformed ? "bad" : "9876.543", "  +1.25 ", "-1.2e-3", "-0.0",
        "NaN", "Infinity", "-Infinity", "1e999", "1e-999", malformed ? "" : "0",
        malformed ? "    " : "1", "0x1.0p2", "1       ", null};
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < values.length; i++) rows.add(Row.of(i, values[i]));
    return rows;
  }

  private static TableEnvironment input(boolean legacy, int length, boolean malformed) {
    var type = ROW(FIELD("id", INT()), FIELD("c", CHAR(length)));
    return table(legacy, type, rows(malformed));
  }

  private static TableEnvironment roundInput() {
    var type = ROW(FIELD("a", CHAR(8)), FIELD("b", CHAR(8)), FIELD("c", CHAR(8)), FIELD("d", CHAR(8)),
        FIELD("e", CHAR(13)), FIELD("f", CHAR(13)), FIELD("g", CHAR(13)), FIELD("h", CHAR(39)));
    return table(true, type, List.of(
        Row.of("1234.567", "2345.678", "3456.789", "4567.890", "112233445.512", "223344556.623",
            "334455667.734", "11112222333344445555666677778888999.912"),
        Row.of("9876.543", "8765.432", "7654.321", "6543.210", "554433221.154", "665544332.265",
            "776655443.376", "99998888777766665555444433332222111.121")));
  }

  private static TableEnvironment table(boolean legacy, org.apache.flink.table.types.DataType type,
      List<Row> rows) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.createTemporaryView("src", table.fromDataStream(fromData(env, rows,
        ExternalTypeInfo.<Row>of(type)), Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }
}
