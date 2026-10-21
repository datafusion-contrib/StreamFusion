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

class FlinkRetractingExtremaPartialsSqlHarnessTest {
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
    return Stream.of(
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL(20,3)",
            "STRING",
            "DATE",
            "TIME(0)",
            "TIMESTAMP(9)",
            "BOOLEAN")
        .filter(type -> type.equals(System.getProperty("extrema.type", type)))
        .flatMap(
            type ->
                Stream.of(1, 2, 4)
                    .filter(bundle -> bundle == Integer.getInteger("extrema.bundle", bundle))
                    .map(bundle -> Arguments.of(type, bundle)));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void signedMapsRetainReplacementExtremaAndCompleteChangelogs(String type, int bundle) {
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

  static Stream<Arguments> monotonicFixtures() {
    return Stream.of(
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL(20,3)",
            "STRING",
            "DATE",
            "TIME(0)",
            "TIMESTAMP(9)",
            "BOOLEAN")
        .flatMap(type -> Stream.of("MIN", "MAX").map(inner -> Arguments.of(type, inner)));
  }

  @ParameterizedTest
  @MethodSource("monotonicFixtures")
  void monotonicScalarsCanSharePartialsWithRetractingMaps(String type, String inner) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromDataStream(
                        fromData(
                            env,
                            INPUT_TYPE,
                            Row.of(1, value(type, 3), true),
                            Row.of(1, value(type, 1), true),
                            Row.of(1, value(type, 5), true),
                            Row.of(2, value(type, 2), true),
                            Row.of(2, value(type, 8), true),
                            Row.of(2, value(type, -4), true)),
                        SCHEMA));
                return configure(table, 1);
              },
              "SELECT MIN(mx), MAX(mx), COUNT(mx) FROM (SELECT k, "
                  + inner
                  + "(CAST(raw_value AS "
                  + type
                  + ")) AS mx FROM src GROUP BY k) updates");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
    }
  }

  @org.junit.jupiter.api.Test
  void positiveRetentionKeepsStockMapEntryExpiry() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, rows("DOUBLE").toArray(Row[]::new)),
                        SCHEMA,
                        ChangelogMode.all()));
                configure(table, 2).getConfig().set("table.exec.state.ttl", "1 h");
                return table;
              },
              query("DOUBLE", "src"));
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    }
  }

  static Stream<Arguments> recoveries() {
    return Stream.of("DOUBLE", "TIMESTAMP(9)")
        .flatMap(
            type ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(backend -> Arguments.of(type, backend)));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void mapsRetainReplacementValuesAcrossTwoRestores(String type, String backend) throws Exception {
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
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    return table;
  }

  private static String query(String type, String source) {
    return "SELECT k, MIN(v), MAX(v), MIN(v) FILTER (WHERE selected),"
        + " MAX(v) FILTER (WHERE selected), COUNT(*) FROM (SELECT k, selected, CAST(raw_value AS "
        + type
        + ") AS v FROM "
        + source
        + ") typed GROUP BY k";
  }

  private static List<Row> rows(String type) {
    var rows = new java.util.ArrayList<Row>();
    for (Integer key : java.util.Arrays.asList(null, 1)) {
      Object a = value(type, 3),
          b = value(type, 1),
          c = value(type, 2),
          d = value(type, 5),
          e = value(type, -4),
          f = value(type, 8);
      rows.addAll(
          List.of(
              Row.ofKind(RowKind.INSERT, key, a, true), Row.ofKind(RowKind.INSERT, key, b, false),
              Row.ofKind(RowKind.INSERT, key, null, null), Row.ofKind(RowKind.INSERT, key, c, true),
              Row.ofKind(RowKind.DELETE, key, b, false),
                  Row.ofKind(RowKind.UPDATE_BEFORE, key, a, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, d, false),
                  Row.ofKind(RowKind.DELETE, key, c, true),
              Row.ofKind(RowKind.DELETE, key, null, null),
                  Row.ofKind(RowKind.DELETE, key, d, false),
              Row.ofKind(RowKind.INSERT, key, e, true),
                  Row.ofKind(RowKind.INSERT, key, null, false),
              Row.ofKind(RowKind.DELETE, key, e, true), Row.ofKind(RowKind.INSERT, key, f, true),
              Row.ofKind(RowKind.DELETE, key, null, false),
                  Row.ofKind(RowKind.DELETE, key, f, true)));
    }
    return rows;
  }

  private static String value(String type, int value) {
    switch (type) {
      case "DATE":
        return java.time.LocalDate.of(1970, 1, 1).plusDays(value).toString();
      case "TIME(0)":
        return java.time.LocalTime.ofSecondOfDay((value + 10) * 997L).toString();
      case "TIMESTAMP(9)":
        return java.time.LocalDateTime.of(1970, 1, 1, 0, 0)
            .plusSeconds(value)
            .plusNanos(Math.abs(value) + 1)
            .toString()
            .replace('T', ' ');
      case "BOOLEAN":
        return Boolean.toString(value % 2 == 0);
      case "STRING":
        return "v" + value;
      default:
        return Integer.toString(value);
    }
  }

  private static void assertWork(List<Map<String, Object>> operators) {
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarLocalGroupAggregate");
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarGroupAggregate");
  }
}
