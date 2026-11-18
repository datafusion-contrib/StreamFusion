package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkGuardedDivisionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT i <> 0 AND 100 / i > 1 FROM n",
      "SELECT i = 0 OR 100 / i > 1 FROM n",
      "SELECT i = 0 OR MOD(100, i) = 0 FROM n",
      "SELECT i FROM n WHERE i <> 0 AND 100 / i > 1",
      "SELECT i FROM n WHERE i = 0 OR 100 / i > 1",
      "SELECT i IS NULL OR i = 0 OR 100 / i > 1 FROM n",
      "SELECT i = 0 OR CAST(100 / i AS BIGINT) > 1 FROM n",
      "SELECT i = 0 OR MOD(100 / i, 2) = 0 FROM n",
      "SELECT i IS NOT NULL OR MOD(i, 0) = 0 FROM n"
  })
  void fallibleBooleanOperandsUseFlink(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(FlinkGuardedDivisionSqlHarnessTest::input,
        sql, "row short-circuiting");
  }

  @Test
  void caseStillSelectsRowsNatively() throws Exception {
    NativeParity.assertParity(FlinkGuardedDivisionSqlHarnessTest::input,
        "SELECT CASE WHEN i = 0 THEN 0 ELSE 100 / i END FROM n");
  }

  @Test
  void infallibleBooleanExpressionsStayNative() throws Exception {
    NativeParity.assertParity(FlinkGuardedDivisionSqlHarnessTest::input,
        "SELECT i = 0 OR i > 1, i <> 0 AND i > 1 FROM n");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT i = 0 OR MOD(i, 2) = 0, i <> 0 AND i / 2 > 1 FROM n",
      "SELECT i = 0 OR MOD(i, -1) = 0, i <> 0 AND i / -1 > 1 FROM n",
      "SELECT i FROM n WHERE i IS NOT NULL AND MOD(i, 2) = 0",
      "SELECT i = 0 OR MOD(CAST(i AS BIGINT), CAST(2 AS BIGINT)) = 0 FROM n"
  })
  void nonzeroIntegerLiteralDivisorsStayNative(String sql) throws Exception {
    NativeParity.assertParity(FlinkGuardedDivisionSqlHarnessTest::input, sql);
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"i"}, Types.INT),
            Row.of(0),
            Row.of(2),
            Row.of(-3),
            Row.of(Integer.MIN_VALUE),
            Row.of(Integer.MAX_VALUE),
            Row.of((Object) null)));
    return table;
  }
}
