package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "false")
class FlinkLegacyJvmAggregateSqlHarnessTest {
  static final String QUERY =
      "SELECT k,legacy_avg(DISTINCT v,w),legacy_avg(v,w),COUNT(*) FROM src GROUP BY k";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyDistinctAndFilteredCallsRetainPojoStateAndLifecycle(boolean updating) {
    for (boolean ttl : new boolean[] {false, true}) {
      for (String calls :
          List.of(
              "legacy_avg(DISTINCT v,w),legacy_avg(v,w),COUNT(*)",
              "legacy_avg(DISTINCT v,w) FILTER (WHERE w>0),legacy_avg(v,w) FILTER (WHERE v IS NOT"
                  + " NULL)")) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var runs =
              NativeFailureParity.run(
                  () -> environment(updating, ttl, metrics),
                  "SELECT k," + calls + " FROM src GROUP BY k");
          runs.assertSuccess(NativeFailureParity.Route.NATIVE);
          assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyPojoStateAndDistinctMembershipSurviveRepeatedRecovery(boolean ttl) throws Exception {
    var rows = rows(true);
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  rowType(),
                  Schema.newBuilder()
                      .column("k", org.apache.flink.table.api.DataTypes.INT())
                      .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
                      .column("w", org.apache.flink.table.api.DataTypes.INT())
                      .build(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), ttl),
                () -> register(recovery.get(), ttl),
                QUERY.replace("FROM src", "FROM recovery_input"));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
        SharedFlinkCluster.assertNativeMemoryReleased("legacy-jvm-aggregate-recovery");
      }
    }
  }

  private static TableEnvironment environment(
      boolean updating, boolean ttl, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            fromData(env, rows(updating), rowType()),
            Schema.newBuilder()
                .column("k", org.apache.flink.table.api.DataTypes.INT())
                .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
                .column("w", org.apache.flink.table.api.DataTypes.INT())
                .build(),
            ChangelogMode.all()));
    return register(table, ttl);
  }

  static TableEnvironment benchmarkEnvironment(int rows, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    var input =
        env.fromSequence(0, Math.max(0, rows - 1))
            .filter(i -> i < rows)
            .map(i -> Row.of((int) (i % 64), i % 8 == 0 ? null : (long) (i % 7 - 3), (int) (i % 3)))
            .returns(rowType());
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            input,
            Schema.newBuilder()
                .column("k", org.apache.flink.table.api.DataTypes.INT())
                .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
                .column("w", org.apache.flink.table.api.DataTypes.INT())
                .build()));
    return register(table, false);
  }

  private static TableEnvironment register(TableEnvironment table, boolean ttl) {
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    planner
        .getFlinkContext()
        .getFunctionCatalog()
        .registerTemporarySystemFunction(
            "legacy_avg",
            new AggregateFunctionDefinition(
                "legacy_avg",
                new WeightedAverage(),
                Types.LONG,
                TypeInformation.of(Accumulator.class)),
            false);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", ttl ? "1 h" : "0 s");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    return table;
  }

  private static TypeInformation<Row> rowType() {
    return Types.ROW_NAMED(new String[] {"k", "v", "w"}, Types.INT, Types.LONG, Types.INT);
  }

  private static List<Row> rows(boolean updating) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(
          Row.of(
              i % 5 == 0 ? null : i % 4,
              i % 8 == 0 ? null : (long) (i % 7 - 3),
              i % 7 == 0 ? null : i % 3));
    if (updating)
      for (int i = 0; i < 16; i++) {
        var old = rows.get(i);
        rows.add(
            Row.ofKind(RowKind.UPDATE_BEFORE, old.getField(0), old.getField(1), old.getField(2)));
        rows.add(Row.ofKind(RowKind.UPDATE_AFTER, old.getField(0), 7L, 2));
        rows.add(Row.ofKind(RowKind.DELETE, old.getField(0), 7L, 2));
      }
    return rows;
  }

  public static class Accumulator {
    public long sum;
    public int weight;
  }

  public static class WeightedAverage extends AggregateFunction<Long, Accumulator> {
    private transient boolean opened;

    @Override
    public Accumulator createAccumulator() {
      return new Accumulator();
    }

    @Override
    public Long getValue(Accumulator accumulator) {
      if (!opened) throw new IllegalStateException("legacy aggregate is not open");
      return accumulator.weight == 0 ? null : accumulator.sum / accumulator.weight;
    }

    public void accumulate(Accumulator accumulator, Long value, Integer weight) {
      if (!opened) throw new IllegalStateException("legacy aggregate is not open");
      if (value != null && weight != null) {
        accumulator.sum += value * weight;
        accumulator.weight += weight;
      }
    }

    public void retract(Accumulator accumulator, Long value, Integer weight) {
      if (!opened) throw new IllegalStateException("legacy aggregate is not open");
      if (value != null && weight != null) {
        accumulator.sum -= value * weight;
        accumulator.weight -= weight;
      }
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("legacy aggregate opened twice");
      opened = true;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("legacy aggregate closed twice");
      opened = false;
    }
  }
}
