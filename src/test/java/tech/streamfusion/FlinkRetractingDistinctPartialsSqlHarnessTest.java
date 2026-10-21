package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.*;
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

class FlinkRetractingDistinctPartialsSqlHarnessTest {
  private static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT_TYPE =
      Types.ROW_NAMED(
          new String[] {"k", "raw_value", "keep_row"}, Types.INT, Types.STRING, Types.BOOLEAN);
  private static final Schema SCHEMA =
      Schema.newBuilder()
          .column("k", DataTypes.INT())
          .column("raw_value", DataTypes.STRING())
          .column("keep_row", DataTypes.BOOLEAN())
          .build();

  static Stream<Arguments> profiles() {
    return Stream.of(
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "DECIMAL(12,2)",
            "STRING",
            "CHAR(4)",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "DATE",
            "TIME(3)",
            "TIMESTAMP(9)",
            "TIMESTAMP_LTZ",
            "DECIMAL(20,3)",
            "DECIMAL(38,0)",
            "DECIMAL(38,37)")
        .filter(
            type ->
                System.getProperty("distinct.retract.type", "").isEmpty()
                    || System.getProperty("distinct.retract.type").equals(type))
        .flatMap(type -> Stream.of(1, 2, 4).map(bundle -> Arguments.of(type, bundle)));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void signedBundlesPreserveSharedMembershipAndFilteredCounts(String type, int bundle) {
    compare(type, bundle, true);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"BIGINT", "STRING", "DOUBLE"})
  void insertedRecordCountersPrecedeTheNativeViews(String type) {
    for (int bundle : new int[] {1, 3}) compare(type, bundle, false);
  }

  private static void compare(String type, int bundle, boolean explicitCount) {
    compare(type, bundle, explicitCount, rows(type));
  }

  static void compare(String type, int bundle, boolean explicitCount, List<Row> input) {
    compare(type, bundle, explicitCount, input, false);
  }

  static void compare(
      String type,
      int bundle,
      boolean explicitCount,
      List<Row> input,
      boolean hostOverflowFailure) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, input.toArray(Row[]::new)),
                        SCHEMA,
                        ChangelogMode.all()));
                return configure(table, bundle);
              },
              explicitCount ? query(type, "src") : query(type, "src").replace(", COUNT(*)", ""));
      if (hostOverflowFailure) {
        runs.assertFailure(
            NullPointerException.class,
            "toBigDecimal",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
        return;
      }
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators());
    }
  }

  static Stream<Arguments> recoveries() {
    return Stream.of(
            "BIGINT", "STRING", "TIMESTAMP(9)", "DOUBLE", "DECIMAL(38,0)", "DECIMAL(38,37)")
        .flatMap(
            type ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(backend -> Arguments.of(type, backend)));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void signedMembershipSurvivesTwoRestores(String type, String backend) throws Exception {
    var input = new ArrayList<Row>();
    for (int i = 0; i < 8; i++) input.addAll(rows(type));
    input.add(Row.of(0, values(type)[2], true));
    try (var recovery =
        new PortableSqlRecovery(backend, input, INPUT_TYPE, SCHEMA, true, 44, 112)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), 4),
              () -> configure(recovery.get(), 4),
              query(type, "recovery_input"));
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()));
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      assertWork(recovery.nativeExecution());
      SharedFlinkCluster.assertNativeMemoryReleased("signed-distinct-partials");
    }
  }

  private static TableEnvironment configure(TableEnvironment table, int bundle) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    return table;
  }

  private static String query(String type, String source) {
    String expression =
        type.equals("TIMESTAMP_LTZ")
            ? "TO_TIMESTAMP_LTZ(CAST(raw_value AS BIGINT),3)"
            : "CAST(raw_value AS " + type + ")";
    boolean numeric =
        type.startsWith("DECIMAL(")
            || List.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE").contains(type);
    return "SELECT k, COUNT(DISTINCT v), COUNT(DISTINCT v) FILTER (WHERE keep_row), "
        + "COUNT(DISTINCT v) FILTER (WHERE NOT keep_row), COUNT(*)"
        + (numeric
            ? ", SUM(DISTINCT v), AVG(DISTINCT v), SUM(DISTINCT v) FILTER (WHERE keep_row),"
                + " AVG(DISTINCT v) FILTER (WHERE NOT keep_row)"
            : "")
        + " FROM (SELECT k, "
        + expression
        + " AS v, keep_row FROM "
        + source
        + ") GROUP BY k";
  }

  private static String[] values(String type) {
    if (type.equals("DECIMAL(38,0)"))
      return new String[] {"9" + "0".repeat(37), "8" + "0".repeat(37), "-9" + "0".repeat(37)};
    if (type.equals("DECIMAL(38,37)"))
      return new String[] {"9." + "9".repeat(37), "8." + "8".repeat(37), "-9." + "9".repeat(37)};
    if (type.equals("DECIMAL(20,3)"))
      return new String[] {"99999999999999999.999", "-99999999999999999.998", "0.001"};
    if (type.equals("BOOLEAN")) return new String[] {"true", "false", "true"};
    if (type.equals("DATE")) return new String[] {"2024-02-29", "1969-12-31", "2000-01-01"};
    if (type.startsWith("TIME("))
      return new String[] {"00:00:00.007", "23:59:59.999", "12:34:56.789"};
    if (type.startsWith("TIMESTAMP("))
      return new String[] {
        "1969-12-31 23:59:59.999999999",
        "2024-01-01 00:00:00.000000001",
        "2024-01-01 00:00:00.000000002"
      };
    if (type.equals("FLOAT") || type.equals("DOUBLE")) return new String[] {"-0.0", "0.0", "NaN"};
    return new String[] {"7", "-4", "13"};
  }

  private static List<Row> rows(String type) {
    String[] v = values(type);
    return List.of(
        row(RowKind.INSERT, null, false),
        row(RowKind.INSERT, v[0], true),
        row(RowKind.INSERT, v[1], false),
        row(RowKind.INSERT, v[0], true),
        row(RowKind.INSERT, v[1], true),
        row(RowKind.UPDATE_BEFORE, v[0], true),
        row(RowKind.UPDATE_AFTER, v[2], true),
        row(RowKind.DELETE, v[0], true),
        row(RowKind.DELETE, v[1], false),
        row(RowKind.DELETE, v[1], true),
        row(RowKind.DELETE, v[2], true),
        row(RowKind.DELETE, null, false),
        row(RowKind.INSERT, v[1], false),
        row(RowKind.INSERT, v[0], null),
        row(RowKind.DELETE, v[0], null),
        row(RowKind.INSERT, null, true),
        row(RowKind.DELETE, v[1], false),
        row(RowKind.DELETE, null, true));
  }

  private static Row row(RowKind kind, String value, Boolean keep) {
    return Row.ofKind(kind, 0, value, keep);
  }

  private static void assertWork(List<Map<String, Object>> work) {
    RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarLocalGroupAggregate");
    RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarGroupAggregate");
  }
}
