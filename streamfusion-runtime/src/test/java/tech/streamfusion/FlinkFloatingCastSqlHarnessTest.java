package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFloatingCastSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void floatingNarrowingMatchesJava(String target) throws Exception {
    NativeParity.assertParity(
        FlinkFloatingCastSqlHarnessTest::input,
        "SELECT CAST(d AS " + target + "), CAST(f AS " + target + ") FROM n");
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Double[] values = {
      0.0, -0.0, 127.75, 128.75, -128.75, -129.75, 32767.75, 32768.75,
      -32768.75, -32769.75, 2147483648.0, -2147483649.0, Double.MAX_VALUE,
      -Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, null
    };
    Row[] rows = new Row[values.length];
    for (int i = 0; i < values.length; i++) {
      Double value = values[i];
      rows[i] = Row.of(value, value == null ? null : value.floatValue());
    }
    table.createTemporaryView(
        "n",
        fromData(env, Types.ROW_NAMED(new String[] {"d", "f"}, Types.DOUBLE, Types.FLOAT), rows));
    return table;
  }
}
