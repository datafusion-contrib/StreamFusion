package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkWideDecimalGroupOrderSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"2,5,1", "3,5,7", "7,17,0", "17,31,0"})
  void overflowSensitiveGroupOrderingKeepsFallback(int keys, int bundle, int seed)
      throws Exception {
    Supplier<TableEnvironment> source =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
          BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
          List<Row> rows = new ArrayList<>();
          for (int repeat = 0; repeat < 3; repeat++) {
            for (BigDecimal value :
                new BigDecimal[] {
                  large, large.subtract(BigDecimal.valueOf(3)), large.negate(), null
                }) {
              for (int key = 0; key < keys; key++) rows.add(Row.of(key, value));
            }
          }
          java.util.Collections.shuffle(rows, new java.util.Random(seed));
          table.createTemporaryView(
              "src",
              table.fromDataStream(
                  fromData(
                      env,
                      rows,
                      Types.ROW_NAMED(new String[] {"k", "d"}, Types.INT, Types.BIG_DEC)),
                  Schema.newBuilder().column("k", INT()).column("d", DECIMAL(38, 0)).build()));
          return table;
        };
    for (String aggregate : List.of("SUM", "AVG")) {
      String sql = "SELECT k, " + aggregate + "(DISTINCT d), COUNT(*) FROM src GROUP BY k";
      String plan = NativePlanner.explain(source.get(), sql);
      assertTrue(plan.contains("LocalGroupAggregate"), plan);
      assertTrue(plan.contains("GlobalGroupAggregate"), plan);
      NativeParity.assertFallbackReasonContains(source, sql, "DECIMAL precision <= 19");
    }
  }
}
