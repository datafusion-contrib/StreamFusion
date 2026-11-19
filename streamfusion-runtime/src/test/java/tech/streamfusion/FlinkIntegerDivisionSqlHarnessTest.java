package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkIntegerDivisionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT i / -1, l / -1, MOD(i, -1), MOD(l, -1) FROM n",
      "SELECT i / j, l / k, i / k, l / j FROM n",
      "SELECT i / 2, l / 2, i / -2, l / -2 FROM n"
  })
  void divisionPreservesJavaOverflow(String sql) throws Exception {
    NativeParity.assertParity(FlinkIntegerDivisionSqlHarnessTest::input, sql);
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"i", "l", "j", "k"}, Types.INT, Types.LONG, Types.INT, Types.LONG),
            Row.of(Integer.MIN_VALUE, Long.MIN_VALUE, -1, -1L),
            Row.of(Integer.MAX_VALUE, Long.MAX_VALUE, -1, -1L),
            Row.of(-7, -7L, 2, 2L),
            Row.of(7, 7L, -2, -2L),
            Row.of(null, null, 0, 0L),
            Row.of(1, 1L, null, null)));
    return table;
  }
}
