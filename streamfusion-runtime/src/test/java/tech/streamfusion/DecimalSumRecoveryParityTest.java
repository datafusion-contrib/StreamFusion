package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DecimalSumRecoveryParityTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT SUM(d) FROM n",
      "SELECT g, SUM(d) FILTER (WHERE keep_value) FROM n GROUP BY g",
      "SELECT SUM(d), AVG(d) FROM n"
  })
  void decimalSumAfterOverflow(String sql) throws Exception {
    NativeParity.assertChangelogParity(DecimalSumRecoveryParityTest::decimals, sql);
  }

  private static TableEnvironment decimals() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    BigDecimal max = new BigDecimal("99999999999999999999999999999999999.999");
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"g", "d", "keep_value"}, Types.INT, Types.BIG_DEC, Types.BOOLEAN),
            Row.of(1, max, true),
            Row.of(1, max, true),
            Row.of(1, null, true),
            Row.of(1, BigDecimal.ONE, true)),
        Schema.newBuilder()
            .column("g", DataTypes.INT())
            .column("d", DataTypes.DECIMAL(38, 3))
            .column("keep_value", DataTypes.BOOLEAN())
            .build());
    return table;
  }
}
