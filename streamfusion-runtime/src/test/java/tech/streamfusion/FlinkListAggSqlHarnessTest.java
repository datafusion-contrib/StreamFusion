package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkListAggSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void literalSeparatorsNullsAndDuplicatesKeepTheOrderedChangelog(boolean updating) {
    for (String separator : List.of("", ",", "::", "é🚀", "'")) {
      String literal = "'" + separator.replace("'", "''") + "'";
      check(
          updating,
          "SELECT LISTAGG(v, " + literal + ") FROM src",
          row(""),
          row("a"),
          row(null),
          row("a"),
          row("\0\n🚀"));
    }
  }

  @Test
  void firstDuplicateRetractionAndFilteredRowsPreserveArrivalOrder() {
    var removed = row("a");
    removed.setKind(RowKind.DELETE);
    var ignored = row("not selected");
    ignored.setField(4, false);
    check(
        true,
        "SELECT LISTAGG(v, '/') FILTER (WHERE keep_row) FROM src",
        row("a"),
        ignored,
        row("b"),
        row("a"),
        row(null),
        removed);
  }

  @Test
  void groupedAndDefaultSeparatorFormsComposeWithNumericAggregates() {
    var other = row("other");
    other.setField(2, "other");
    check(
        false,
        "SELECT json_key, LISTAGG(v), COUNT(*), SUM(n) FROM src GROUP BY json_key",
        row("a"),
        other,
        row("b"),
        row(null),
        row("a"));
  }

  @Test
  void allNullAndEmptyInputRetainTheReleasedTypesAndRowCount() {
    check(false, "SELECT LISTAGG(v, ',') FROM src", row(null), row(null));
    check(false, "SELECT LISTAGG(v, ',') FROM src");
  }

  @Test
  void characterInputsRetainCastPaddingAndTruncation() {
    check(
        false,
        "SELECT LISTAGG(CAST(v AS CHAR(4)), '::') FROM src",
        row("a"),
        row("abcdef"),
        row("é🚀"),
        row(""),
        row(null));
  }

  @Test
  void singlePhaseMiniBatchRetainsTheMaterializedChangelog() throws Exception {
    Row removed = row("a");
    removed.setKind(RowKind.DELETE);
    java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> environment =
        () -> {
          var table =
              FlinkStringAggregateHostContractTest.environment(
                  true, row("a"), row("b"), row("a"), row(null), removed, row("last"));
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.size", "3");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          return table;
        };
    String sql = "SELECT LISTAGG(v, '::') FROM src";
    assertTrue(
        tech.streamfusion.planner.NativePlanner.explain(environment.get(), sql)
            .contains("NativeColumnarGroupAggregate"));
    NativeParity.assertChangelogParity(environment, sql);
  }

  @Test
  void numericSinglePhaseMiniBatchKeepsItsTtlEmissionMode() throws Exception {
    java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> environment =
        () -> {
          var table =
              FlinkStringAggregateHostContractTest.environment(
                  false,
                  Row.of("a", ",", "key", 1L, true),
                  Row.of("b", ",", "key", 0L, true),
                  Row.of("c", ",", "key", 0L, true),
                  Row.of("d", ",", "key", 0L, true));
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.size", "2");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.state.ttl", "1 h");
          return table;
        };
    var comparison = NativeFailureParity.run(environment, "SELECT SUM(n) FROM src");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"));
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void twoRestoresRetainPrefixesAndRetractableArrivalOrder(String backend, boolean updating)
      throws Exception {
    var input = new java.util.ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      int original = updating && i >= 64 ? i - 64 : i;
      input.add(
          Row.ofKind(
              updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
              original % 3,
              original % 7 == 0 ? null : "é🚀" + original % 5));
    }
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
                updating,
                24,
                72)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
          table -> {
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "false");
            table.getConfig().set("streamfusion.transpose.batchRows", "13");
            return table;
          };
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT k, LISTAGG(v, '::') FROM recovery_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
          comparison.nativeRun().plan());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("LISTAGG-recovery");
    }
  }

  private static void check(boolean updating, String sql, Row... input) {
    var comparison =
        NativeFailureParity.run(
            () -> FlinkStringAggregateHostContractTest.environment(updating, input), sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
        comparison.nativeRun().plan());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  private static Row row(String value) {
    return Row.of(value, ",", "key", 1L, true);
  }
}
