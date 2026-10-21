package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkSharedDistinctRetentionSqlHarnessTest {
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
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true).map(mini -> Arguments.of(type, append, mini))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void retainedTypedMembershipUsesTheNativeSinglePhasePath(
      String type, boolean append, boolean mini) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                var rows = input(type, append, false);
                var source =
                    fromData(
                        env,
                        FlinkRetractingDistinctPartialsSqlHarnessTest.INPUT_TYPE,
                        rows.toArray(Row[]::new));
                var schema = FlinkRetractingDistinctPartialsSqlHarnessTest.SCHEMA;
                table.createTemporaryView(
                    "src",
                    append
                        ? table.fromDataStream(source, schema)
                        : table.fromChangelogStream(source, schema, ChangelogMode.all()));
                return configure(table, mini);
              },
              FlinkRetractingDistinctPartialsSqlHarnessTest.query(type, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertFalse(runs.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  static Stream<Arguments> recoveries() {
    return Stream.of("BIGINT", "STRING", "DOUBLE", "TIMESTAMP(9)", "DECIMAL(12,2)", "DECIMAL(38,0)")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(
                                    "hashmap",
                                    "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                                .map(backend -> Arguments.of(type, append, backend))));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void typedMembershipAndCachedResultsSurviveTwoRestores(
      String type, boolean append, String backend) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 24; i++) rows.addAll(input(type, append, true));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                FlinkRetractingDistinctPartialsSqlHarnessTest.INPUT_TYPE,
                FlinkRetractingDistinctPartialsSqlHarnessTest.SCHEMA,
                !append,
                44,
                112)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), true),
              () -> configure(recovery.get(), true),
              FlinkRetractingDistinctPartialsSqlHarnessTest.query(type, "recovery_input"));
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()),
          runs::toString);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("shared-distinct-retention");
    }
  }

  private static List<Row> input(String type, boolean append, boolean safeArithmetic) {
    var rows = new ArrayList<Row>();
    var replacements = new LinkedHashMap<String, String>();
    for (Row source : FlinkRetractingDistinctPartialsSqlHarnessTest.rows(type)) {
      if (append && source.getKind() != RowKind.INSERT) continue;
      var row = Row.copy(source);
      Object value = row.getField(1);
      if (safeArithmetic
          && value != null
          && (type.startsWith("DECIMAL(") || type.equals("DOUBLE"))) {
        String key = value.toString();
        String replacement = replacements.get(key);
        if (replacement == null) {
          replacement = List.of("7", "-4", "13").get(replacements.size());
          replacements.put(key, replacement);
        }
        row.setField(1, replacement);
      }
      rows.add(row);
    }
    return rows;
  }

  private static TableEnvironment configure(TableEnvironment table, boolean mini) {
    table.getConfig().set("table.exec.state.ttl", "1 h");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "4");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    return table;
  }
}
