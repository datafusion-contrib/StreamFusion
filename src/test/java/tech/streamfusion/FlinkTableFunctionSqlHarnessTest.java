package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.SAME_THREAD)
class FlinkTableFunctionSqlHarnessTest {
  @Test
  void innerAndLeftKeepNullEmptyAndPushedPredicateSemantics() {
    for (String join :
        new String[] {
          ", LATERAL TABLE(test_split(s.text_value)) AS t(v, pos)",
          " LEFT JOIN LATERAL TABLE(test_split(s.text_value)) AS t(v, pos) ON TRUE",
          ", LATERAL TABLE(test_split(s.text_value)) AS t(v, pos) WHERE t.pos > 0"
        }) {
      var comparison =
          NativeFailureParity.run(
              () -> PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of()),
              "SELECT s.id, t.v, t.pos FROM test_input s" + join);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          comparison.nativeRun().plan().contains("NativeTableFunction"),
          comparison.nativeRun().plan());
    }
  }

  @Test
  void repeatedAndEmptyProjectionsRetainFanout() {
    for (String projection : new String[] {"t.v, t.pos, t.v", "COUNT(*)"}) {
      var comparison =
          NativeFailureParity.run(
              () -> PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of()),
              "SELECT "
                  + projection
                  + " FROM test_input s, "
                  + "LATERAL TABLE(test_split(s.text_value)) AS t(v, pos)");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mutableRowsAreConsumedBeforeReuseAcrossManyBatches(boolean objectReuse) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(false, objectReuse, 5003, ""),
            "SELECT s.id, t.v, t.pos FROM src s, LATERAL TABLE(expand_rows(s.id)) AS t(v, pos)");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(comparison.nativeRun().rows().size() > 5003);
  }

  @Test
  void aSingleInvocationCanFillManyOwnedOutputBatches() {
    var comparison =
        NativeFailureParity.run(
            () -> environment(false, true, 2, "wide"),
            "SELECT s.id, t.v, t.pos FROM src s, LATERAL TABLE(expand_rows(s.id)) AS t(v, pos)");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(8193, comparison.nativeRun().rows().size());
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  @Test
  void allInputRowKindsPropagateThroughLeftPadding() {
    var comparison =
        NativeFailureParity.run(
            () -> environment(true, true, 16, ""),
            "SELECT s.id, t.v, t.pos FROM src s LEFT JOIN "
                + "LATERAL TABLE(expand_rows(s.id)) AS t(v, pos) ON TRUE");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    for (String kind : new String[] {"+I", "-U", "+U", "-D"}) {
      assertTrue(
          comparison.nativeRun().rows().stream().anyMatch(row -> kind.equals(row.get(0))), kind);
    }
  }

  @Test
  void openEvaluationAndCloseFailuresKeepTheirOriginalCauses() {
    for (String phase : new String[] {"open", "eval", "close"}) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(false, false, 16, phase),
              "SELECT s.id, t.v, t.pos FROM src s, LATERAL TABLE(expand_rows(s.id)) AS t(v, pos)");
      if (comparison.host().failure() == null) {
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        continue;
      }
      assertNotNull(comparison.nativeRun().failure(), phase);
      assertEquals(
          comparison.host().rootCause().getClass(),
          comparison.nativeRun().rootCause().getClass(),
          phase);
      assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage(),
          phase);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void checkpointedFanoutFeedsRestoredGroupedState(String backend, boolean updating)
      throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      int original = updating && i >= 64 ? i - 64 : i;
      rows.add(
          Row.ofKind(
              updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
              original % 3,
              original % 13 == 0 ? null : (long) original));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "id"}, Types.INT, Types.LONG),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("id", DataTypes.BIGINT())
                    .build(),
                updating,
                24,
                72)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> register(recovery.uninterrupted()),
              () -> register(recovery.get()),
              "SELECT s.k, SUM(t.pos), COUNT(*) FROM recovery_input s LEFT JOIN "
                  + "LATERAL TABLE(expand_rows(s.id)) AS t(v, pos) ON TRUE GROUP BY s.k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("table-function-recovery");
    }
  }

  private static TableEnvironment register(TableEnvironment table) {
    table.createTemporarySystemFunction("expand_rows", new MutableRows(""));
    return table;
  }

  @Test
  void nestedFunctionOutputsRetainTypesAndNativeApplication() {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment(false, false, 16, "");
              table.createTemporarySystemFunction("nested_rows", new NestedRows());
              return table;
            },
            "SELECT s.id, t.v FROM src s, LATERAL TABLE(nested_rows(s.id)) AS t(v)");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(comparison.nativeRun().plan().contains("NativeTableFunction"));
  }

  @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("ROW<v ARRAY<INT>>"))
  public static class NestedRows extends TableFunction<Row> {
    public void eval(Long value) {
      if (value != null) collect(Row.of((Object) new Integer[] {value.intValue(), null}));
    }
  }

  @Test
  void numericArgumentsResultsAndGroupedConsumersRetainTheirTypes() {
    for (String projection : new String[] {"s.id, t.n, t.pos", "SUM(t.n), COUNT(*)"}) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = environment(false, true, 64, "");
                table.createTemporarySystemFunction(
                    "numeric_rows", new TableFunctionBenchmark.NumericRows());
                return table;
              },
              "SELECT "
                  + projection
                  + " FROM src s, "
                  + "LATERAL TABLE(numeric_rows(CAST(MOD(s.id, 7) AS INT), s.id)) AS t(n, pos)");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @Test
  void parallelFanoutBetweenKeyedAggregatesRetainsFinalChangelog() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          var table = environment(false, true, 128, "", 3);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          table.createTemporarySystemFunction(
              "numeric_rows", new TableFunctionBenchmark.NumericRows());
          return table;
        },
        "SELECT t.pos, SUM(t.n), COUNT(*) FROM "
            + "(SELECT CAST(MOD(id, 7) AS INT) AS k, SUM(id) AS n FROM src "
            + "GROUP BY CAST(MOD(id, 7) AS INT)) s, "
            + "LATERAL TABLE(numeric_rows(s.k, s.n)) AS t(n, pos) GROUP BY t.pos");
  }

  @Test
  void operatorSwitchKeepsTheWholeFunctionQueryOnFlink() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(false, false, 16, "");
          table.getConfig().set("streamfusion.operator.tableFunction.enabled", "false");
          return table;
        },
        "SELECT s.id, t.v, t.pos FROM src s, LATERAL TABLE(expand_rows(s.id)) AS t(v, pos)",
        "tableFunction: disabled by config");
  }

  private static TableEnvironment environment(
      boolean updating, boolean objectReuse, int count, String fail) {
    return environment(updating, objectReuse, count, fail, 1);
  }

  private static TableEnvironment environment(
      boolean updating, boolean objectReuse, int count, String fail, int parallelism) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    if (objectReuse) env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    var rows = new Row[count];
    for (int i = 0; i < count; i++) {
      rows[i] =
          Row.ofKind(
              updating ? RowKind.values()[i % 4] : RowKind.INSERT, i % 13 == 0 ? null : (long) i);
    }
    var source =
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env, Types.ROW_NAMED(new String[] {"id"}, Types.LONG), rows);
    if (updating) table.createTemporaryView("src", table.fromChangelogStream(source));
    else table.createTemporaryView("src", source);
    table.createTemporarySystemFunction("expand_rows", new MutableRows(fail));
    return table;
  }

  @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("ROW<v STRING, pos INT>"))
  public static class MutableRows extends TableFunction<Row> {
    private final String fail;
    private transient Row reused;
    private transient boolean opened;

    public MutableRows(String fail) {
      this.fail = fail;
    }

    @Override
    public void open(FunctionContext context) {
      if (fail.equals("open")) throw new IllegalStateException("table-open");
      if (opened) throw new IllegalStateException("opened twice");
      opened = true;
      reused = Row.of(null, 0);
    }

    public void eval(Long value) {
      if (!opened) throw new IllegalStateException("evaluated outside lifecycle");
      if (value == null) return;
      if (fail.equals("eval") && value == 4) throw new IllegalStateException("table-eval");
      int emissions = fail.equals("wide") ? 8193 : (int) (value % 4);
      for (int i = 0; i < emissions; i++) {
        reused.setField(0, i == 1 ? null : "é🚀-" + value);
        reused.setField(1, i);
        collect(reused);
      }
      reused.setField(0, "mutated after collect");
      reused.setField(1, -1);
    }

    @Override
    public void close() {
      opened = false;
      if (fail.equals("close")) throw new IllegalStateException("table-close");
    }
  }
}
