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

class FlinkWideDecimalTimestampSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"3,false", "9,false", "9,true"})
  void timestampGroups(int precision, boolean ltz) throws Exception {
    int keys = 7, bundle = 17, seed = 0;
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
              for (int key = 0; key < keys; key++)
                rows.add(
                    Row.of(
                        ltz
                            ? java.time.Instant.ofEpochSecond(
                                1_700_000_000L, (key + 1L) * (precision == 3 ? 1_000_000L : 1L))
                            : java.time.LocalDateTime.of(2024, 1, 1, 0, 0)
                                .plusNanos((key + 1L) * (precision == 3 ? 1_000_000L : 1L)),
                        value));
            }
          }
          java.util.Collections.shuffle(rows, new java.util.Random(seed));
          table.createTemporaryView(
              "src",
              table.fromDataStream(
                  fromData(
                      env,
                      rows,
                      Types.ROW_NAMED(
                          new String[] {"k", "d"},
                          ltz ? Types.INSTANT : Types.LOCAL_DATE_TIME,
                          Types.BIG_DEC)),
                  Schema.newBuilder()
                      .column("k", ltz ? TIMESTAMP_LTZ(precision) : TIMESTAMP(precision))
                      .column("d", DECIMAL(38, 0))
                      .build()));
          return table;
        };
    for (String aggregate : List.of("SUM", "AVG")) {
      String sql = "SELECT k, " + aggregate + "(DISTINCT d), COUNT(*) FROM src GROUP BY k";
      String plan = NativePlanner.explain(source.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      NativeParity.assertChangelogParity(source, sql);
    }
  }
}
