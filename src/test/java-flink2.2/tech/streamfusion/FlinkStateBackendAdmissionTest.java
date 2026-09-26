package tech.streamfusion;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkStateBackendAdmissionTest {
  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "rocksdb"})
  void changelogStateDeclinesWholeKeyedQuery(String backend) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(backend),
        "SELECT k, SUM(v) FROM t GROUP BY k",
        "state backend: Flink 2.2 changelog state is not verified for native keyed state");
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "rocksdb"})
  void changelogStateStillAdmitsStatelessSql(String backend) throws Exception {
    NativeParity.assertParity(() -> environment(backend), "SELECT v + 1 FROM t");
  }

  private static TableEnvironment environment(String backend) {
    Configuration config = new Configuration();
    config.set(StateBackendOptions.STATE_BACKEND, backend);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.enableChangelogStateBackend(true);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        FlinkTestSources.fromData(env, Row.of(1L, 10L), Row.of(1L, 5L), Row.of(2L, 7L))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG)));
    return table;
  }
}
