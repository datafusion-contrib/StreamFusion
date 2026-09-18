package tech.streamfusion;

import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkFloatingWindowReproTest {
  @Test
  void tumbleNanFirst() throws Exception {
    check("TUMBLE", Double.NaN, 2.0);
  }

  @Test
  void tumbleNanLast() throws Exception {
    check("TUMBLE", 2.0, Double.NaN);
  }

  @Test
  void tumblePositiveZeroFirst() throws Exception {
    check("TUMBLE", 0.0, -0.0);
  }

  @Test
  void hopNanFirst() throws Exception {
    check("HOP", Double.NaN, 2.0);
  }

  @Test
  void sessionNanFirst() throws Exception {
    check("SESSION", Double.NaN, 2.0);
  }

  @Test
  void finiteControl() throws Exception {
    check("TUMBLE", 3.0, 2.0);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "ONE_PHASE,TUMBLE",
    "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP",
    "TWO_PHASE,HOP"
  })
  void filteredExtremaInitializeFromTheFirstSelectedValue(String phase, String function)
      throws Exception {
    for (double[] values :
        new double[][] {
          {Double.NaN, 2.0},
          {2.0, Double.NaN},
          {0.0, -0.0},
          {-0.0, 0.0},
          {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}
        }) {
      String intervals =
          function.equals("HOP")
              ? "INTERVAL '1' SECOND, INTERVAL '2' SECOND"
              : "INTERVAL '1' SECOND";
      NativeParity.assertParity(
          () -> {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var table = StreamTableEnvironment.create(env);
            table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
            table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
            var ts = LocalDateTime.of(2026, 1, 1, 0, 0);
            table.createTemporaryView(
                "n",
                env.fromData(
                    Types.ROW_NAMED(
                        new String[] {"ts", "d", "f", "keep"},
                        Types.LOCAL_DATE_TIME,
                        Types.DOUBLE,
                        Types.FLOAT,
                        Types.BOOLEAN),
                    Row.of(ts, Double.NaN, Float.NaN, false),
                    Row.of(ts, null, null, true),
                    Row.of(ts, values[0], (float) values[0], true),
                    Row.of(ts, -999.0, -999.0f, null),
                    Row.of(ts, values[1], (float) values[1], true)),
                Schema.newBuilder()
                    .column("ts", DataTypes.TIMESTAMP(3))
                    .column("d", DataTypes.DOUBLE())
                    .column("f", DataTypes.FLOAT())
                    .column("keep", DataTypes.BOOLEAN())
                    .watermark("ts", "ts - INTERVAL '1' SECOND")
                    .build());
            return table;
          },
          "SELECT window_start, MIN(d) FILTER (WHERE keep), MAX(d) FILTER (WHERE keep), "
              + "MIN(f) FILTER (WHERE keep), MAX(f) FILTER (WHERE keep) FROM TABLE("
              + function
              + "(TABLE n, DESCRIPTOR(ts), "
              + intervals
              + ")) "
              + "GROUP BY window_start, window_end");
    }
  }

  private static void check(String function, double first, double second) throws Exception {
    String intervals = function.equals("HOP")
        ? "INTERVAL '1' SECOND, INTERVAL '2' SECOND" : "INTERVAL '1' SECOND";
    NativeParity.assertParity(() -> input(first, second),
        "SELECT window_start, MIN(d), MAX(d), MIN(f), MAX(f) FROM TABLE("
            + function + "(TABLE n, DESCRIPTOR(ts), " + intervals + ")) "
            + "GROUP BY window_start, window_end");
  }

  private static TableEnvironment input(double first, double second) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    LocalDateTime ts = LocalDateTime.of(2026, 1, 1, 0, 0);
    table.createTemporaryView("n", env.fromData(
        Types.ROW_NAMED(new String[] {"ts", "d", "f"},
            Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.FLOAT),
        Row.of(ts, first, (float) first), Row.of(ts, second, (float) second)),
        Schema.newBuilder().column("ts", DataTypes.TIMESTAMP(3))
            .column("d", DataTypes.DOUBLE()).column("f", DataTypes.FLOAT())
            .watermark("ts", "ts - INTERVAL '1' SECOND").build());
    return table;
  }
}
