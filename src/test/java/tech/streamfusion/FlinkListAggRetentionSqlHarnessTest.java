package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.UnaryOperator;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkListAggRetentionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"0,false", "0,true", "1,false", "1,true", "2,false", "2,true"})
  void wholeAccumulatorRetentionAdmitsBothSeparatorsAndPhaseLayouts(int context, boolean literal) {
    var rows = new Row[] {row(""), row("a"), row(null), row("a"), row("é🚀"), row("b"), row(null)};
    var comparison =
        NativeFailureParity.run(
            () -> configure(FlinkStringAggregateHostContractTest.environment(false, rows), context),
            "SELECT json_key, LISTAGG(v"
                + (literal ? ", '::'" : "")
                + ") FROM src GROUP BY json_key");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertPhases(comparison, context);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  @Test
  void numericCompositionAndFiltersKeepTheWholeGroupLifetime() {
    var ignored = row("ignored");
    ignored.setField(4, false);
    for (int context = 0; context < 3; context++) {
      int mode = context;
      var comparison =
          NativeFailureParity.run(
              () ->
                  configure(
                      FlinkStringAggregateHostContractTest.environment(
                          false, row("a"), ignored, row(null), row("b"), row("a")),
                      mode),
              "SELECT LISTAGG(v, '::') FILTER (WHERE keep_row), COUNT(*), SUM(n), MIN(n) FROM src");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertPhases(comparison, mode);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,true", "true,false"})
  void independentDistinctStateRetainsItsGuardAndOrdinaryRetractionExecutesNatively(
      boolean updating, boolean distinct) {
    var removed = row("a");
    removed.setKind(RowKind.DELETE);
    Row[] rows =
        updating
            ? new Row[] {row("a"), row("b"), removed}
            : new Row[] {row("a"), row("a"), row("b")};
    var comparison =
        NativeFailureParity.run(
            () -> configure(FlinkStringAggregateHostContractTest.environment(updating, rows), 0),
            "SELECT LISTAGG(" + (distinct ? "DISTINCT " : "") + "v, '::') FROM src");
    comparison.assertSuccess(
        distinct ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,0",
    "hashmap,1",
    "hashmap,2",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,0",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,1",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,2"
  })
  void wholePrefixRetentionSurvivesTwoRestores(String backend, int context) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 96; i++) rows.add(Row.of(i % 3, i % 7 == 0 ? null : "é🚀" + i % 5));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
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
      UnaryOperator<TableEnvironment> config = table -> configure(table, context);
      var comparison =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              "SELECT k, LISTAGG(v, '::'), COUNT(*) FROM recovery_input GROUP BY k");
      assertNull(comparison.host().failure(), comparison::toString);
      assertNull(comparison.nativeRun().failure(), comparison::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()),
          comparison::toString);
      assertPhases(comparison, context);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      if (context == 2)
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("LISTAGG-whole-retention");
    }
  }

  private static TableEnvironment configure(TableEnvironment table, int context) {
    table.getConfig().set("table.exec.state.ttl", "1 h");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", context == 2 ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(context > 0));
    table.getConfig().set("table.exec.mini-batch.size", "11");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  private static void assertPhases(NativeFailureParity.Comparison comparison, int context) {
    assertEquals(
        context == 2,
        comparison.host().plan().contains("LocalGroupAggregate"),
        comparison::toString);
    assertEquals(
        context == 2,
        comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"),
        comparison::toString);
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
        comparison::toString);
  }

  private static Row row(String value) {
    return Row.of(value, ",", "key", 1L, true);
  }
}
