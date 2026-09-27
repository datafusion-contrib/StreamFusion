package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.time.Instant;
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
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDistinctAggregateCoverageSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void ordinaryAverageBeforeDistinctKeepsViewOffsets() throws Exception {
    compare(
        9,
        "SELECT k, AVG(amount), COUNT(DISTINCT ts), SUM(DISTINCT amount),"
            + " COUNT(DISTINCT b) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(ints = {20, 38})
  void wideDecimalDistinctSumKeepsHostMergeOrder(int precision) throws Exception {
    Supplier<TableEnvironment> source =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "5");
          BigDecimal large = BigDecimal.TEN.pow(precision - 1).multiply(BigDecimal.valueOf(9));
          List<Row> rows =
              List.of(
                  Row.of(1, large),
                  Row.of(1, large.subtract(BigDecimal.ONE)),
                  Row.of(1, large.negate()));
          table.createTemporaryView(
              "src",
              table.fromDataStream(
                  fromData(
                      env,
                      rows,
                      Types.ROW_NAMED(new String[] {"k", "amount"}, Types.INT, Types.BIG_DEC)),
                  Schema.newBuilder()
                      .column("k", INT())
                      .column("amount", DECIMAL(precision, 0))
                      .build()));
          return table;
        };
    String sql = "SELECT k, SUM(DISTINCT amount) FROM src GROUP BY k";
    NativeParity.assertFallbackReasonContains(source, sql, "DECIMAL precision <= 19");
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 9})
  void twoPhaseDistinctRetainsValueTypesAcrossBundles(int precision) throws Exception {
    compare(
        precision,
        "SELECT k, COUNT(DISTINCT b), COUNT(DISTINCT ts), SUM(DISTINCT amount),"
            + " COUNT(DISTINCT amount), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 9})
  void filteredDistinctViewsKeepSeparateMultiplicities(int precision) throws Exception {
    compare(
        precision,
        "SELECT k, COUNT(DISTINCT b) FILTER (WHERE keep),"
            + " COUNT(DISTINCT ts) FILTER (WHERE keep),"
            + " COUNT(DISTINCT ts) FILTER (WHERE NOT keep),"
            + " SUM(DISTINCT amount) FILTER (WHERE keep),"
            + " COUNT(DISTINCT amount) FILTER (WHERE NOT keep), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 9})
  void globalAndEmptyDistinctKeepNullAndCountResults(int precision) throws Exception {
    String sql = "SELECT COUNT(DISTINCT b), COUNT(DISTINCT ts), SUM(DISTINCT amount) FROM src";
    compare(precision, sql);
    compare(precision, sql + " WHERE k < 0");
  }

  private static void compare(int precision, String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> environment(precision);
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(source, sql);
  }

  private static TableEnvironment environment(int precision) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    table.getConfig().set("table.exec.mini-batch.size", "5");
    List<Row> rows = new ArrayList<>();
    Instant epoch = Instant.parse("1960-02-29T12:34:56Z");
    for (int i = 0; i < 80; i++) {
      int value = i % 7;
      rows.add(
          Row.of(
              i % 4,
              i % 11 == 0 ? null : value % 2 == 0,
              i % 13 == 0 ? null : epoch.plusNanos(value * (precision == 3 ? 1_000_000L : 1L)),
              i % 9 == 0 ? null : new BigDecimal(value - 3).movePointLeft(2),
              i % 3 == 0));
    }
    rows.add(Row.of(4, null, null, null, true));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            fromData(
                env,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "b", "ts", "amount", "keep"},
                    Types.INT,
                    Types.BOOLEAN,
                    Types.INSTANT,
                    Types.BIG_DEC,
                    Types.BOOLEAN)),
            Schema.newBuilder()
                .column("k", INT())
                .column("b", BOOLEAN())
                .column("ts", TIMESTAMP_LTZ(precision))
                .column("amount", DECIMAL(19, 2))
                .column("keep", BOOLEAN())
                .build()));
    return table;
  }
}
