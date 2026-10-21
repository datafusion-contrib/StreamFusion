package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkListAggDistinctSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void retainedSharedAndSeparateArgumentViewsCompose(boolean updating, boolean miniBatch) {
    var input = new ArrayList<Row>();
    for (String value : new String[] {null, "a", "a", "b", "", "é🚀", "a"}) input.add(row(value));
    var ignored = row("b");
    ignored.setField(4, false);
    input.add(ignored);
    if (updating) {
      var removed = row("a");
      removed.setKind(RowKind.DELETE);
      input.add(removed);
    }
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table =
                  FlinkStringAggregateHostContractTest.environment(
                      updating, input.toArray(Row[]::new));
              table.getConfig().set("table.exec.state.ttl", "1 h");
              table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
              table.getConfig().set("table.exec.mini-batch.size", "3");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              return table;
            },
            "SELECT json_key, LISTAGG(DISTINCT v), LISTAGG(DISTINCT v, '::'), "
                + "LISTAGG(DISTINCT v, '::') FILTER (WHERE keep_row), "
                + "COUNT(DISTINCT v) FILTER (WHERE keep_row), COUNT(*) FROM src GROUP BY json_key");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,false,false",
    "ONE_PHASE,false,true",
    "ONE_PHASE,true,false",
    "ONE_PHASE,true,true",
    "TWO_PHASE,false,false",
    "TWO_PHASE,false,true",
    "TWO_PHASE,true,false",
    "TWO_PHASE,true,true"
  })
  void releasedPhasePlansKeepVerifiedNativeAndFallbackRoutes(
      String phase, boolean updating, boolean distinct) {
    var rows = new ArrayList<Row>();
    for (String value : new String[] {null, "a", "a", "b", "", "é🚀", "\0", "\ud800", "?"})
      rows.add(row(value));
    if (updating) {
      var first = row("a");
      first.setKind(RowKind.DELETE);
      rows.add(first);
      var last = row("a");
      last.setKind(RowKind.DELETE);
      rows.add(last);
      rows.add(row("a"));
    }
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table =
                  FlinkStringAggregateHostContractTest.environment(
                      updating, rows.toArray(Row[]::new));
              table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
              table
                  .getConfig()
                  .set(
                      "table.exec.mini-batch.enabled",
                      phase.equals("TWO_PHASE") ? "true" : "false");
              table.getConfig().set("table.exec.mini-batch.size", "3");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              return table;
            },
            "SELECT json_key, LISTAGG("
                + (distinct ? "DISTINCT " : "")
                + "v, '::'), COUNT(*) FROM src GROUP BY json_key");
    comparison.assertSuccess(
        (phase.equals("ONE_PHASE") || !distinct)
            ? NativeFailureParity.Route.NATIVE
            : NativeFailureParity.Route.FALLBACK);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(
        phase.equals("TWO_PHASE"), comparison.host().plan().contains("LocalGroupAggregate"));
    assertEquals(
        phase.equals("TWO_PHASE"), comparison.host().plan().contains("GlobalGroupAggregate"));
    if (phase.equals("ONE_PHASE"))
      assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    System.out.println(
        "[listagg-context] phase="
            + phase
            + " updating="
            + updating
            + " distinct="
            + distinct
            + " host_local="
            + comparison.host().plan().contains("LocalGroupAggregate")
            + " host_global="
            + comparison.host().plan().contains("GlobalGroupAggregate")
            + " native="
            + comparison.nativeRun().route());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void separatorsFiltersCastsAndEmptyGroups(boolean updating) {
    for (String separator : new String[] {"", ",", "::", "é🚀", "'"}) {
      check(
          updating,
          "SELECT LISTAGG(DISTINCT v, '" + separator.replace("'", "''") + "') FROM src",
          row(""),
          row("a"),
          row("a"),
          row(null),
          row("é🚀\0"));
    }
    check(
        updating,
        "SELECT LISTAGG(DISTINCT CAST(v AS CHAR(4))), LISTAGG(DISTINCT v, '::'), COUNT(*) FROM src",
        row("a"),
        row("abcdef"),
        row("é🚀"),
        row("a"),
        row(null));
    check(updating, "SELECT LISTAGG(DISTINCT v) FROM src", row(null), row(null));
    check(updating, "SELECT LISTAGG(DISTINCT v) FROM src");
    var ignored = row("ignored");
    ignored.setField(4, false);
    var removed = row("a");
    removed.setKind(RowKind.DELETE);
    var input = new ArrayList<Row>();
    input.addAll(java.util.List.of(row("a"), row("b"), row("a"), ignored));
    if (updating) {
      input.add(removed);
      input.add(removed);
      input.add(row("a"));
    }
    check(
        updating,
        "SELECT json_key, LISTAGG(DISTINCT v, '/') FILTER (WHERE keep_row), COUNT(*) FROM src GROUP"
            + " BY json_key",
        input.toArray(Row[]::new));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void singlePhaseMiniBatchesRetainDistinctOrder(boolean updating) {
    var removed = row("a");
    removed.setKind(RowKind.DELETE);
    var rows = new ArrayList<Row>(java.util.List.of(row("a"), row("b"), row("a"), row(null)));
    if (updating) {
      rows.add(removed);
      rows.add(removed);
      rows.add(row("a"));
    }
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table =
                  FlinkStringAggregateHostContractTest.environment(
                      updating, rows.toArray(Row[]::new));
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.size", "3");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              table.getConfig().set("streamfusion.transpose.batchRows", "2");
              return table;
            },
            "SELECT json_key, LISTAGG(DISTINCT v, '::') FROM src GROUP BY json_key");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void twoRestoresRetainDistinctMultiplicitiesAndOrdering(String backend, boolean updating)
      throws Exception {
    var input = new ArrayList<Row>();
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
              "SELECT k, LISTAGG(DISTINCT v, '::') FROM recovery_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("LISTAGG-DISTINCT-recovery");
    }
  }

  private static void check(boolean updating, String sql, Row... rows) {
    var comparison =
        NativeFailureParity.run(
            () -> FlinkStringAggregateHostContractTest.environment(updating, rows), sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  private static Row row(String value) {
    return Row.of(value, ",", "key", 1L, true);
  }
}
