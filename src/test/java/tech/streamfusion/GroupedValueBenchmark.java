package tech.streamfusion;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class GroupedValueBenchmark {
  private static final long ROWS = Long.getLong("grouped.value.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("grouped.value.warmup", 2);
  private static final int RUNS = Integer.getInteger("grouped.value.runs", 5);
  private static final boolean DISTINCT = Boolean.getBoolean("grouped.value.distinct");
  private static final boolean SINGLE = Boolean.getBoolean("grouped.value.single");
  private static final boolean TWO_PHASE =
      Boolean.parseBoolean(System.getProperty("grouped.value.twoPhase", Boolean.toString(SINGLE)));
  private static final String SQL =
      SINGLE
          ? "INSERT INTO sink SELECT k, SINGLE_VALUE(v) FROM inputs GROUP BY k"
          : "INSERT INTO sink SELECT k, FIRST_VALUE("
              + (DISTINCT ? "DISTINCT " : "")
              + "v), LAST_VALUE("
              + (DISTINCT ? "DISTINCT " : "")
              + "v) FROM inputs GROUP BY k";

  @Test
  void groupedValues() throws Exception {
    for (String type :
        System.getProperty("grouped.value.types", DISTINCT || SINGLE ? "STRING" : "BIGINT,STRING")
            .split(",")) {
      String plan = NativePlanner.explain(environment(type), SQL);
      if (!plan.contains("NativeColumnarGroupAggregate")
          || !plan.contains("RowDataToArrow")
          || !plan.contains("ArrowToRowData")
          || (TWO_PHASE && !plan.contains("NativeColumnarLocalGroupAggregate"))) {
        throw new IllegalStateException("Expected native aggregate and both transposes: " + plan);
      }
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          TableEnvironment table = environment(type);
          PhysicalPlanScan scan = engine == 1 ? NativePlanner.install(table) : null;
          long start = System.nanoTime();
          table.executeSql(SQL).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (scan != null && scan.substitutions() == 0) {
            throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
          }
          if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
        }
      }
      double host = median(times[0]);
      double nativeTime = median(times[1]);
      System.out.printf(
          Locale.ROOT,
          "[grouped-value] single=%s distinct=%s type=%s two_phase=%s rows=%d Flink=%.6fs"
              + " Native=%.6fs ratio=%.3fx flink_trials=%s native_trials=%s%n",
          SINGLE,
          DISTINCT,
          type,
          TWO_PHASE,
          ROWS,
          host,
          nativeTime,
          host / nativeTime,
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static double median(double[] times) {
    double[] sorted = times.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment(String type) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", TWO_PHASE ? "TWO_PHASE" : "ONE_PHASE");
    if (TWO_PHASE) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", "1024");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    }
    var dataType =
        switch (type) {
          case "STRING" -> DataTypes.STRING();
          case "BIGINT" -> DataTypes.BIGINT();
          case "TIME" -> DataTypes.TIME(3);
          case "BOOLEAN" -> DataTypes.BOOLEAN();
          default ->
              throw new IllegalArgumentException("Unknown grouped.value.types entry: " + type);
        };
    var valueType =
        switch (type) {
          case "STRING" -> Types.STRING;
          case "TIME" -> Types.LOCAL_TIME;
          case "BOOLEAN" -> Types.BOOLEAN;
          default -> Types.LONG;
        };
    table.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        SINGLE ? i.intValue() : (int) (i % 64),
                        i / 64 % 8 == 0
                            ? null
                            : switch (type) {
                              case "STRING" -> "value-" + (i % 1024);
                              case "TIME" -> java.time.LocalTime.ofNanoOfDay(i % 1024 * 1_000_000);
                              case "BOOLEAN" -> i / 64 % 2 == 0;
                              default -> i % 1024;
                            }))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, valueType)),
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", dataType).build());
    String sqlType = dataType.getLogicalType().asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (k INT, first_v "
            + sqlType
            + (SINGLE ? "" : ", last_v " + sqlType)
            + ") WITH ('connector' = 'blackhole')");
    return table;
  }
}
