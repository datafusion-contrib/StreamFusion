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

class FlinkRetractingSingleValueSqlHarnessTest {
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
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL(20,3)",
            "STRING",
            "DATE",
            "TIME(0)",
            "TIMESTAMP(9)",
            "BOOLEAN")
        .flatMap(
            type ->
                Stream.of(1, 3)
                    .flatMap(
                        bundle ->
                            Stream.of(false, true)
                                .map(filter -> Arguments.of(type, bundle, filter))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void signedValueAndCountPartialsRetainCompleteChangelogs(
      String type, int bundle, boolean filtered) {
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
                configure(table, bundle);
                return table;
              },
              query(type, filtered, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 3})
  void globalSingletonKeepsItsEmptyResultThroughDeletion(int bundle) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                Row[] input =
                    rows("DOUBLE").stream()
                        .filter(row -> row.getField(0) == null)
                        .toArray(Row[]::new);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, input), SCHEMA, ChangelogMode.all()));
                return configure(table, bundle);
              },
              "SELECT SINGLE_VALUE(CAST(raw_value AS DOUBLE)), COUNT(*) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
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
  void updatingSingletonsSurviveTwoRestores(String type, String backend) throws Exception {
    try (var recovery =
        new PortableSqlRecovery(backend, rows(type), INPUT_TYPE, SCHEMA, true, 9, 27)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), 3),
              () -> configure(recovery.get(), 3),
              query(type, true, "recovery_input"));
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

  private static String query(String type, boolean filter, String source) {
    String value = filter ? "CASE WHEN selected THEN raw_value ELSE NULL END" : "raw_value";
    return "SELECT k, SINGLE_VALUE(CAST("
        + value
        + " AS "
        + type
        + ")), COUNT(*) FROM "
        + source
        + " GROUP BY k";
  }

  private static List<Row> rows(String type) {
    var examples = interleavedRows(type);
    Object a = examples.get(0).getField(1),
        b = examples.get(3).getField(1),
        c = examples.get(10).getField(1);
    var rows = new java.util.ArrayList<Row>();
    // Each 15-row key cycle aligns the logical bundles, keeping their global trigger cadence fixed.
    for (Integer key : java.util.Arrays.asList(null, 1, 2)) {
      rows.addAll(
          List.of(
              Row.ofKind(RowKind.INSERT, key, a, true),
              Row.ofKind(RowKind.UPDATE_BEFORE, key, a, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, b, false),
              Row.ofKind(RowKind.UPDATE_BEFORE, key, b, false),
              Row.ofKind(RowKind.UPDATE_AFTER, key, null, true),
              Row.ofKind(RowKind.DELETE, key, null, true),
              Row.ofKind(RowKind.INSERT, key, null, true),
              Row.ofKind(RowKind.UPDATE_BEFORE, key, null, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, c, true),
              Row.ofKind(RowKind.UPDATE_BEFORE, key, c, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, b, true),
              Row.ofKind(RowKind.DELETE, key, b, true),
              Row.ofKind(RowKind.INSERT, key, null, null),
              Row.ofKind(RowKind.UPDATE_BEFORE, key, null, null),
              Row.ofKind(RowKind.UPDATE_AFTER, key, a, null)));
    }
    return rows;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DOUBLE", "TIMESTAMP(9)"})
  void interleavedKeysKeepTheMaterializedRelation(String type) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeParity.assertChangelogParity(
          () -> {
            var env = metrics.environment(new Configuration(), 1, 0);
            var table = StreamTableEnvironment.create(env);
            table.createTemporaryView(
                "src",
                table.fromChangelogStream(
                    fromData(env, INPUT_TYPE, interleavedRows(type).toArray(Row[]::new)),
                    SCHEMA,
                    ChangelogMode.all()));
            return configure(table, 3);
          },
          query(type, true, "src"));
      assertWork(metrics.nativeOperators());
    }
  }

  private static List<Row> interleavedRows(String type) {
    String a = "1.25", b = "-2.5", c = "0";
    switch (type) {
      case "BIGINT":
        a = "7";
        b = "-2";
        c = "0";
        break;
      case "DATE":
        a = "1969-12-31";
        b = "1970-01-01";
        c = "9999-12-31";
        break;
      case "TIME(0)":
        a = "00:00:00";
        b = "23:59:59";
        c = "12:30:45";
        break;
      case "TIMESTAMP(9)":
        a = "1969-12-31 23:59:59.999999999";
        b = "1970-01-01 00:00:00.000000001";
        c = "2024-11-03 01:59:59.999999999";
        break;
      case "BOOLEAN":
        a = "true";
        b = "false";
        c = "true";
        break;
      case "DECIMAL(20,3)":
        a = "99999999999999999.999";
        b = "-0.001";
        c = "7.000";
        break;
      default:
        break;
    }
    return List.of(
        Row.ofKind(RowKind.INSERT, null, a, true), Row.ofKind(RowKind.INSERT, 1, a, false),
        Row.ofKind(RowKind.UPDATE_BEFORE, null, a, true),
            Row.ofKind(RowKind.UPDATE_AFTER, null, b, false),
        Row.ofKind(RowKind.UPDATE_BEFORE, 1, a, false),
            Row.ofKind(RowKind.UPDATE_AFTER, 1, b, true),
        Row.ofKind(RowKind.DELETE, null, b, false), Row.ofKind(RowKind.DELETE, 1, b, true),
        Row.ofKind(RowKind.INSERT, null, null, true),
            Row.ofKind(RowKind.UPDATE_BEFORE, null, null, true),
        Row.ofKind(RowKind.UPDATE_AFTER, null, c, true), Row.ofKind(RowKind.INSERT, 2, a, true),
        Row.ofKind(RowKind.UPDATE_BEFORE, 2, a, true),
            Row.ofKind(RowKind.UPDATE_AFTER, 2, null, false),
        Row.ofKind(RowKind.DELETE, 2, null, false),
            Row.ofKind(RowKind.UPDATE_BEFORE, null, c, true),
        Row.ofKind(RowKind.UPDATE_AFTER, null, b, true), Row.ofKind(RowKind.INSERT, 3, null, null));
  }

  private static void assertWork(List<Map<String, Object>> operators) {
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarLocalGroupAggregate");
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarGroupAggregate");
  }
}
