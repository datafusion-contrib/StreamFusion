package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkPowerSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "POWER(x, y)",
        "SQRT(x)",
        "POWER(POWER(x, y), y)",
        "POWER(x, y) IS NULL",
        "CASE WHEN y = 0 THEN 1.0 ELSE POWER(x, y) END"
      })
  void powerPreservesExactResultsAndIeeeSpecialValues(String expression) throws Exception {
    NativeParity.assertParity(
        FlinkPowerSqlHarnessTest::input, "SELECT id, " + expression + " FROM src");
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Double[] values = {
      null,
      0.0,
      -0.0,
      1.0,
      -1.0,
      2.0,
      -2.0,
      0.5,
      -0.5,
      3.0,
      -3.0,
      1.2345678901234567,
      123.456,
      Double.MIN_VALUE,
      Double.MIN_NORMAL,
      Double.MAX_VALUE,
      -Double.MAX_VALUE,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      Double.NaN
    };
    Row[] rows = new Row[values.length * values.length];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = Row.of(i, values[i / values.length], values[i % values.length]);
    }
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "x", "y"}, Types.INT, Types.DOUBLE, Types.DOUBLE),
            rows));
    return table;
  }
}
