package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class FlinkLegacyBasicCastSqlHarnessTest {
  @Test
  void legacyCharacterLengthsKeepUnicodeAndComposedPredicates() throws Exception {
    for (String sql : Arrays.asList("SELECT id, CAST(s AS CHAR(3)) FROM src",
        "SELECT id, CAST(s AS CHAR(1)), CAST(s AS VARCHAR(1)) FROM src",
        "SELECT id FROM src WHERE CAST(s AS CHAR(1)) LIKE '%x%'",
        "SELECT id, CASE WHEN CAST(s AS CHAR(1))='x' THEN 1 ELSE 0 END FROM src")) {
      NativeParity.assertParity(() -> stringRows(true, false,
          "", "x", "longx", "\u4e2d\u6587", "\ud83d\ude00x", "e\u0301", "\ud800x", null), sql);
    }
  }

  @Test
  void legacyNotNullDecimalResultsReachDropEnforcement() throws Exception {
    NativeParity.assertParity(() -> {
      var table = stringRows(true, true, "1.25", "bad", "99999999999999999999999999999", "2.50");
      table.getConfig().set("table.exec.sink.not-null-enforcer", "DROP");
      return table;
    }, "SELECT CAST(s AS DECIMAL(10,2)) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void legacyConversionsStayIdenticalAcrossRepeatedRecovery(String backend) {
    var type = ROW(FIELD("id", INT()), FIELD("s", STRING()));
    var rows = Arrays.asList(Row.of(1, "1.25"), Row.of(2, "bad"), Row.of(3, (Object) null),
        Row.of(4, "-2.555"), Row.of(5, "99999999999999999999999999999"),
        Row.of(6, "0"), Row.of(7, "12.34"), Row.of(8, ""), Row.of(9, "7.50"));
    try (var recovery = new PortableSqlRecovery(backend, rows, ExternalTypeInfo.<Row>of(type),
        Schema.newBuilder().fromRowDataType(type).build(), false, 3, 6).withExecutionMetrics()) {
      java.util.function.Function<org.apache.flink.table.api.TableEnvironment,
          org.apache.flink.table.api.TableEnvironment> configure = table -> {
        table.getConfig().set("table.exec.legacy-cast-behaviour", "ENABLED");
        return table;
      };
      var runs = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()),
          "SELECT id, CAST(s AS DECIMAL(10,2)), CAST(s AS CHAR(1)) FROM recovery_input");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      org.junit.jupiter.api.Assertions.assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
    }
  }

  private static StreamTableEnvironment stringRows(boolean legacy, boolean notNull, String... values) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var type = ROW(FIELD("id", INT()), FIELD("s", notNull ? STRING().notNull() : STRING()));
    java.util.List<Row> rows = new java.util.ArrayList<>();
    for (int i = 0; i < values.length; i++) rows.add(Row.of(i, values[i]));
    table.createTemporaryView("src", table.fromDataStream(fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void basicCastsRetainTheConfiguredHostSemantics(boolean legacy) throws Exception {
    for (String expression : Arrays.asList(
        "CAST(s AS DECIMAL(10,2))", "CAST(b AS VARCHAR)", "CAST(s AS CHAR(3))",
        "CAST(c AS CHAR(3))", "CAST(d AS VARCHAR)", "CAST(d AS CHAR(3))")) {
      NativeParity.assertParity(() -> environment(legacy, false),
          "SELECT id, " + expression + " FROM src");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"DECIMAL(10,2)", "DECIMAL(38,6)"})
  void legacyMalformedAndOverflowDecimalInputsRemainNull(String decimal) throws Exception {
    NativeParity.assertParity(() -> stringRows(true, false,
        "1.25e3", "-1.005", "1.005", ".5", "+0.00", "  -1.235  ",
        "-99999999999999999999999999999999999999999", "NaN", "0x1", null),
        "SELECT id, CAST(s AS " + decimal + ") FROM src");
    NativeParity.assertParity(() -> environment(true, true),
        "SELECT id, CAST(s AS " + decimal + ") FROM src");
    NativeParity.assertParity(() -> environment(true, true),
        "SELECT id, CASE WHEN id=1 THEN CAST(s AS " + decimal + ") ELSE NULL END FROM src");
    NativeParity.assertParity(() -> environment(true, true),
        "SELECT id, CAST(s AS " + decimal + ") FROM src WHERE id=99");
  }

  private static StreamTableEnvironment environment(boolean legacy, boolean malformed) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var type = ROW(FIELD("id", INT().notNull()), FIELD("s", STRING()), FIELD("b", BOOLEAN()),
        FIELD("c", CHAR(2)), FIELD("d", DECIMAL(7,2)));
    var rows = Arrays.asList(
        Row.of(1, "1.25", true, "ab", new BigDecimal("1.25")),
        Row.of(2, malformed ? "bad" : "2.50", false, "cd", new BigDecimal("-2.50")),
        Row.of(3, null, null, null, null),
        Row.of(4, malformed ? "99999999999999999999999999999999999999999" : "0", true, "",
            new BigDecimal("0.00")),
        Row.of(5, malformed ? "  -1.235 " : "-1.235", false, "x", new BigDecimal("99999.99")));
    table.createTemporaryView("src", table.fromDataStream(fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }
}
