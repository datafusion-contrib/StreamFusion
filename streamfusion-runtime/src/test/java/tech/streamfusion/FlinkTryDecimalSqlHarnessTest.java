package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkTryDecimalSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> parsingAndConsumersMatchHost() {
    return Stream.of(false, true)
        .flatMap(
            legacy ->
                Stream.of(
                        "SELECT id, TRY_CAST(s AS DECIMAL(5,2)), TRY_CAST(s AS DECIMAL(38,18)),"
                            + " TRY_CAST(s AS DECIMAL(38,38)) FROM src",
                        "SELECT id FROM src WHERE TRY_CAST(s AS DECIMAL(5,2)) IS NULL",
                        "SELECT id, COALESCE(TRY_CAST(s AS DECIMAL(5,2)), 7) FROM src",
                        "SELECT id, CASE WHEN TRY_CAST(s AS DECIMAL(5,2)) > 0 THEN 1 ELSE 0 END"
                            + " FROM src",
                        "SELECT id, TRY_CAST(TRY_CAST(s AS DECIMAL(10,3)) AS DECIMAL(5,2)) FROM"
                            + " src",
                        "SELECT TRY_CAST(s AS DECIMAL(5,2)), COUNT(*) FROM src GROUP BY TRY_CAST(s"
                            + " AS DECIMAL(5,2))",
                        "SELECT SUM(TRY_CAST(s AS DECIMAL(5,2))), COUNT(TRY_CAST(s AS"
                            + " DECIMAL(5,2))) FROM src",
                        "SELECT id, s IS NULL OR TRY_CAST(s AS DECIMAL(5,2)) > 0 FROM src",
                        "SELECT id, TRY_CAST(CAST(s AS CHAR(48)) AS DECIMAL(10,3)) FROM src")
                    .map(
                        sql ->
                            DynamicTest.dynamicTest(
                                legacy + ": " + sql,
                                () ->
                                    NativeParity.assertChangelogParity(
                                        () -> strings(legacy), sql))));
  }

  @TestFactory
  Stream<DynamicTest> exactRescalingMatchesHost() {
    return Stream.of("DECIMAL(5,2)", "DECIMAL(38,0)", "DECIMAL(38,38)", "DECIMAL(38,18)")
        .map(
            type ->
                DynamicTest.dynamicTest(
                    type,
                    () ->
                        NativeParity.assertParity(
                            FlinkTryDecimalSqlHarnessTest::decimals,
                            "SELECT id, TRY_CAST(d AS "
                                + type
                                + "), TRY_CAST(d AS "
                                + type
                                + ") IS NULL FROM src")));
  }

  @Test
  void failingChildrenAreNotSwallowedByTryCast() {
    NativeFailureParity.run(
            () -> strings(false),
            "SELECT TRY_CAST(CAST(1 / (id - id) AS STRING) AS DECIMAL(5,2)) FROM src")
        .assertFailure(
            ArithmeticException.class,
            "/ by zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void ordinaryMalformedCastStillFails() {
    NativeFailureParity.run(() -> strings(false), "SELECT CAST(s AS DECIMAL(5,2)) FROM src")
        .assertFailure(
            NumberFormatException.class,
            "Overflow",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment strings(boolean legacy) {
    String[] values = {
      "1.25",
      "999.995",
      "-999.995",
      "bad",
      "1.2.3",
      "",
      "   ",
      " +1.255 ",
      "-0.005",
      "+1e2",
      "1E-30",
      ".5",
      "5.",
      "NaN",
      "Infinity",
      "99999999999999999999999999999999999999",
      "1e1000",
      "1e-1000",
      null
    };
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    Row[] rows = new Row[2051];
    for (int i = 0; i < rows.length; i++) rows[i] = Row.of(i, values[i % values.length]);
    table.createTemporaryView(
        "src",
        fromData(env, Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING), rows));
    return table;
  }

  private static TableEnvironment decimals() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "d"}, Types.INT, Types.BIG_DEC),
            Row.of(1, new BigDecimal("999.995")),
            Row.of(2, new BigDecimal("-1.255")),
            Row.of(3, new BigDecimal("99999999999999999999.999999999999999999")),
            Row.of(4, new BigDecimal("0.000000000000000001")),
            Row.of(5, null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("d", DataTypes.DECIMAL(38, 18))
            .build());
    return table;
  }
}
