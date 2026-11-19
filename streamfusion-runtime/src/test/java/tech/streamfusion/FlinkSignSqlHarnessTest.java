package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkSignSqlHarnessTest {
  @Test
  void signPreservesZeroSign() throws Exception {
    NativeParity.assertParity(FlinkSignSqlHarnessTest::input,
        "SELECT SIGN(d), SIGN(f), CAST(1 AS DOUBLE) / SIGN(d), "
            + "CAST(1 AS FLOAT) / SIGN(f) FROM n");
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Double[] values = {0.0, -0.0, -1.5, 1.5, Double.MIN_VALUE, -Double.MIN_VALUE,
        Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null};
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
