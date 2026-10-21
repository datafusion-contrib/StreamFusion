package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRetractingDecimalSumPartialsSqlHarnessTest {
  private static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT_TYPE =
      Types.ROW_NAMED(
          new String[] {"k", "raw_value", "selected"}, Types.INT, Types.STRING, Types.BOOLEAN);
  private static final Schema SCHEMA =
      Schema.newBuilder()
          .column("k", DataTypes.INT())
          .column("raw_value", DataTypes.STRING())
          .column("selected", DataTypes.BOOLEAN())
          .build();

  static Stream<Arguments> fixtures() {
    return Stream.of("DECIMAL(12,2)", "DECIMAL(20,3)", "DECIMAL(38,0)", "DECIMAL(38,38)")
        .flatMap(type -> Stream.of(1, 2, 4).map(bundle -> Arguments.of(type, bundle)));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void partialsPreserveResetAfterOverflowAndCountZeroAdjustments(String type, int bundle) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, rows(type).toArray(Row[]::new)),
                        SCHEMA,
                        ChangelogMode.all()));
                return configure(table, bundle);
              },
              query(type, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
    }
  }

  static Stream<Arguments> floatingFixtures() {
    return Stream.of("FLOAT", "DOUBLE")
        .flatMap(type -> Stream.of(1, 2, 4).map(bundle -> Arguments.of(type, bundle)));
  }

  @ParameterizedTest
  @MethodSource("floatingFixtures")
  void floatingPartialsPreserveNegativeZeroAndTheTwoLevelRoundingOrder(String type, int bundle) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, floatingRows(type).toArray(Row[]::new)),
                        SCHEMA,
                        ChangelogMode.all()));
                return configure(table, bundle);
              },
              floatingQuery(type, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
    }
  }

  static Stream<Arguments> floatingRecoveries() {
    return Stream.of("FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(backend -> Arguments.of(type, backend)));
  }

  @ParameterizedTest
  @MethodSource("floatingRecoveries")
  void floatingRoundingOrderSurvivesTwoRestores(String type, String backend) throws Exception {
    try (var recovery =
        new PortableSqlRecovery(backend, floatingRows(type), INPUT_TYPE, SCHEMA, true, 8, 16)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), 2),
              () -> configure(recovery.get(), 2),
              floatingQuery(type, "recovery_input"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      assertWork(recovery.nativeExecution());
    }
  }

  private static String floatingQuery(String type, String source) {
    return query(type, source).replace("COUNT(*)", "COUNT(*), AVG(v)");
  }

  private static List<Row> floatingRows(String type) {
    String large = type.equals("FLOAT") ? "16777216" : "9007199254740992";
    var values =
        java.util.Arrays.asList(
            "-0.0", null, null, null, "1", "0", "0", "0", large, "0", "-" + large, "0");
    var rows = new java.util.ArrayList<Row>();
    for (String value : values) rows.add(Row.ofKind(RowKind.INSERT, null, value, true));
    for (int i = values.size() - 1; i >= 0; i--)
      rows.add(Row.ofKind(RowKind.DELETE, null, values.get(i), true));
    for (String value : List.of("NaN", "Infinity", "-0.0", "+0.0")) {
      rows.add(Row.ofKind(RowKind.INSERT, null, value, true));
      rows.add(Row.ofKind(RowKind.DELETE, null, value, true));
    }
    return rows;
  }

  static Stream<Arguments> recoveries() {
    return Stream.of("DECIMAL(20,3)", "DECIMAL(38,0)")
        .flatMap(
            type ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(backend -> Arguments.of(type, backend)));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void partialOverflowAndSignedCountsSurviveTwoRestores(String type, String backend)
      throws Exception {
    try (var recovery =
        new PortableSqlRecovery(backend, rows(type), INPUT_TYPE, SCHEMA, true, 8, 24)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), 2),
              () -> configure(recovery.get(), 2),
              query(type, "recovery_input"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      assertWork(recovery.nativeExecution());
    }
  }

  private static TableEnvironment configure(TableEnvironment table, int bundle) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    return table;
  }

  private static String query(String type, String source) {
    return "SELECT k, SUM(v), SUM(v) FILTER (WHERE selected), COUNT(v), COUNT(*)"
        + " FROM (SELECT k, selected, CAST(raw_value AS "
        + type
        + ") AS v FROM "
        + source
        + ") typed GROUP BY k";
  }

  private static List<Row> rows(String type) {
    var parts = type.substring(8, type.length() - 1).split(",");
    int precision = Integer.parseInt(parts[0]), scale = Integer.parseInt(parts[1]);
    var max =
        new java.math.BigDecimal(
            java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE), scale);
    var unit = new java.math.BigDecimal(java.math.BigInteger.ONE, scale);
    String a = max.toPlainString(),
        b = unit.toPlainString(),
        c = unit.negate().toPlainString(),
        d = unit.multiply(java.math.BigDecimal.valueOf(2)).toPlainString(),
        e = max.negate().toPlainString();
    var rows = new java.util.ArrayList<Row>();
    for (Integer key : java.util.Arrays.asList(null, 1)) {
      rows.addAll(
          List.of(
              Row.ofKind(RowKind.INSERT, key, a, true), Row.ofKind(RowKind.INSERT, key, b, true),
              Row.ofKind(RowKind.INSERT, key, c, false), Row.ofKind(RowKind.DELETE, key, b, true),
              Row.ofKind(RowKind.INSERT, key, null, true),
                  Row.ofKind(RowKind.UPDATE_BEFORE, key, a, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, d, false),
                  Row.ofKind(RowKind.DELETE, key, c, false),
              Row.ofKind(RowKind.DELETE, key, null, true),
                  Row.ofKind(RowKind.DELETE, key, d, false),
              Row.ofKind(RowKind.INSERT, key, e, true), Row.ofKind(RowKind.INSERT, key, c, true),
              Row.ofKind(RowKind.INSERT, key, b, false), Row.ofKind(RowKind.DELETE, key, c, true),
              Row.ofKind(RowKind.DELETE, key, e, true), Row.ofKind(RowKind.DELETE, key, b, false)));
    }
    return rows;
  }

  private static void assertWork(List<Map<String, Object>> operators) {
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarLocalGroupAggregate");
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarGroupAggregate");
  }
}
