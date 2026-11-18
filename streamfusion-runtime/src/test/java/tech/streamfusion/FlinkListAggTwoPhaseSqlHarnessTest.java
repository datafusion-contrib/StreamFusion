package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkListAggTwoPhaseSqlHarnessTest {
  @Test
  void separatorsNullsAndCompositionPreservePrefixesAcrossBundles() {
    for (String separator : new String[] {"", ",", "::", "é🚀", "'"}) {
      check(
          "SELECT json_key, LISTAGG(v, '"
              + separator.replace("'", "''")
              + "'), COUNT(*) FROM src GROUP BY json_key",
          row(null),
          row(""),
          row("a"),
          row("a"),
          row("b"),
          row(null),
          row("é🚀\0"));
    }
    check(
        "SELECT LISTAGG(v), LISTAGG(v, '/'), SUM(n), COUNT(*) FROM src",
        row("a"),
        row("b"),
        row("a"),
        row(null));
    check("SELECT LISTAGG(v) FROM src", row(null), row(null), row(null), row(null));
    check("SELECT LISTAGG(v) FROM src");
    check(
        "SELECT LISTAGG(CAST(v AS CHAR(4)), '::') FROM src",
        row("a"),
        row("abcdef"),
        row("é🚀"),
        row("a"));
  }

  @Test
  void filteredStringsAndTrailingDistinctViewsKeepTheirOffsets() {
    var ignored = row("ignored");
    ignored.setField(4, false);
    check(
        "SELECT json_key, LISTAGG(v, '::') FILTER (WHERE keep_row), COUNT(*) FROM src GROUP BY"
            + " json_key",
        row("a"),
        ignored,
        row("b"),
        row("a"),
        row(null),
        row("c"));
    check(
        "SELECT LISTAGG(v, '/'), COUNT(DISTINCT v), COUNT(*) FROM src",
        row("a"),
        row("a"),
        row("b"),
        row(null),
        row("c"));
    check(
        "SELECT COUNT(DISTINCT v), LISTAGG(v, '/'), AVG(n), COUNT(*) FROM src",
        row("a"),
        row("a"),
        row("b"),
        row(null),
        row("c"));
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,3",
    "hashmap,11",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,3",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,11"
  })
  void localDrainAndGlobalPrefixesSurviveTwoRestores(String backend, int batchSize)
      throws Exception {
    var input = new java.util.ArrayList<Row>();
    for (int i = 0; i < 96; i++) input.add(Row.of(i % 3, i % 7 == 0 ? null : "é🚀" + i % 5));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                    new String[] {"k", "v"},
                    org.apache.flink.api.common.typeinfo.Types.INT,
                    org.apache.flink.api.common.typeinfo.Types.STRING),
                org.apache.flink.table.api.Schema.newBuilder()
                    .column("k", org.apache.flink.table.api.DataTypes.INT())
                    .column("v", org.apache.flink.table.api.DataTypes.STRING())
                    .build(),
                false,
                24,
                72)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> config =
          table -> configure(table, batchSize);
      var comparison =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              "SELECT k, LISTAGG(v, '::'), COUNT(*) FROM recovery_input GROUP BY k");
      assertNull(comparison.host().failure(), comparison::toString);
      assertNull(comparison.nativeRun().failure(), comparison::toString);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      // Checkpoint drains change intermediate bundle boundaries; ordered final strings remain
      // exact.
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()),
          comparison::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
      assertNativePhases(comparison);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("two-phase-LISTAGG");
    }
  }

  private static org.apache.flink.table.api.TableEnvironment configure(
      org.apache.flink.table.api.TableEnvironment table, int size) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(size));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  private static void check(String sql, Row... input) {
    var comparison =
        NativeFailureParity.run(
            () -> configure(FlinkStringAggregateHostContractTest.environment(false, input), 3),
            sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertNativePhases(comparison);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  private static void assertNativePhases(NativeFailureParity.Comparison comparison) {
    assertTrue(comparison.host().plan().contains("LocalGroupAggregate"), comparison.toString());
    assertTrue(comparison.host().plan().contains("GlobalGroupAggregate"), comparison.toString());
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"),
        comparison.toString());
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
        comparison.toString());
  }

  private static Row row(String value) {
    return Row.of(value, ",", "key", 1L, true);
  }
}
