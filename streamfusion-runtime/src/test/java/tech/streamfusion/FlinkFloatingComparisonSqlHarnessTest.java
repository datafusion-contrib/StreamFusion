package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFloatingComparisonSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
  void primitiveComparisons(String op) throws Exception {
    NativeParity.assertParity(FlinkFloatingComparisonSqlHarnessTest::input,
        "SELECT id, d " + op + " e, f " + op + " g, d " + op + " g, "
            + "f " + op + " CAST(0 AS FLOAT), d " + op + " CAST(0 AS DOUBLE) FROM n");
  }

  @ParameterizedTest
  @ValueSource(strings = {"d > 0", "f >= 0", "d = 0", "f <> 0", "d < e"})
  void filters(String predicate) throws Exception {
    NativeParity.assertParity(FlinkFloatingComparisonSqlHarnessTest::input,
        "SELECT id FROM n WHERE " + predicate);
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Double[] values = {0.0, -0.0, -1.0, 1.0, Double.NaN,
        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null};
    Row[] rows = new Row[values.length * values.length];
    int id = 0;
    for (Double a : values) {
      for (Double b : values) {
        rows[id] = Row.of(id++, a, b, a == null ? null : a.floatValue(),
            b == null ? null : b.floatValue());
      }
    }
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "d", "e", "f", "g"},
                Types.INT,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.FLOAT,
                Types.FLOAT),
            rows));
    return table;
  }
}
