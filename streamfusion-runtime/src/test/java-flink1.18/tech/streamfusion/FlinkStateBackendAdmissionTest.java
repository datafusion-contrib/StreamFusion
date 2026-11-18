package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkStateBackendAdmissionTest {
  private static final String GROUP = "SELECT k, SUM(v) FROM t GROUP BY k";

  @Test
  void environmentSettingsRocksBackendSurvivesAggregatePipelineCreation() {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var configuration = new Configuration();
              configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
              configuration.setString("parallelism.default", "1");
              try {
                var table = NativePlannerTestEnvironment.create(configuration);
                table.executeSql(
                    "CREATE TEMPORARY VIEW t AS SELECT * FROM "
                        + "(VALUES (1, 'a', 'first'), (1, 'b', 'second'), (2, 'c', 'third')) "
                        + "AS v(k, json_key, val)");
                return table;
              } catch (java.io.IOException failure) {
                throw new java.io.UncheckedIOException(failure);
              }
            },
            "SELECT k, LISTAGG(val, ','), JSON_ARRAYAGG(val), "
                + "JSON_OBJECTAGG(json_key VALUE val) FROM t GROUP BY k");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
        comparison.nativeRun().plan());
  }

  @Test
  void explicitStockRocksBackendAcceleratesKeyedSql() throws Exception {
    NativeParity.assertChangelogParity(() -> environment(true, false), GROUP);
  }

  @Test
  void configuredStockRocksBackendAcceleratesKeyedSql() throws Exception {
    NativeParity.assertChangelogParity(() -> environment(false, true), GROUP);
  }

  @Test
  void legacyStockRocksBackendAcceleratesKeyedSql() throws Exception {
    NativeParity.assertChangelogParity(
        () ->
            environment(
                new org.apache.flink.contrib.streaming.state.RocksDBStateBackend(
                    new org.apache.flink.runtime.state.memory.MemoryStateBackend()),
                false,
                false),
        GROUP);
  }

  @Test
  void stockRocksBackendStillAdmitsStatelessSql() throws Exception {
    NativeParity.assertParity(() -> environment(true, false), "SELECT v + 1 FROM t");
  }

  @Test
  void heapBackendStillAdmitsKeyedSql() throws Exception {
    NativeParity.assertChangelogParity(() -> environment(false, false), GROUP);
  }

  @Test
  void changelogHeapBackendDeclinesKeyedSqlBeforeExecution() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(false, false, true),
        GROUP,
        "state backend: Flink 1.18 changelog state is not verified for native keyed state");
  }

  @Test
  void changelogBackendStillAdmitsStatelessSql() throws Exception {
    NativeParity.assertParity(() -> environment(false, false, true), "SELECT v + 1 FROM t");
  }

  private static TableEnvironment environment(boolean explicitRocks, boolean configuredRocks) {
    return environment(explicitRocks, configuredRocks, false);
  }

  private static TableEnvironment environment(
      boolean explicitRocks, boolean configuredRocks, boolean changelog) {
    return environment(
        explicitRocks ? new EmbeddedRocksDBStateBackend() : null, configuredRocks, changelog);
  }

  private static TableEnvironment environment(
      org.apache.flink.runtime.state.StateBackend backend,
      boolean configuredRocks,
      boolean changelog) {
    Configuration configuration = new Configuration();
    if (configuredRocks) configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableChangelogStateBackend(changelog);
    if (backend != null) env.setStateBackend(backend);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        FlinkTestSources.fromData(env, Row.of(1L, 10L), Row.of(1L, 5L), Row.of(2L, 7L))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG)));
    return table;
  }
}
