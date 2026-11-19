package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

/** Complete row-source/collecting-sink jobs in one shared cluster; both transposes remain timed. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowTemporalExtremaBenchmark {
  private static final long ROWS = Long.getLong("window.temporal.rows", 2_000_000L);
  private static final int GROUPS = Integer.getInteger("window.temporal.groups", 64);
  private static final int WARMUP = Integer.getInteger("window.temporal.warmup", 2);
  private static final int RUNS = Integer.getInteger("window.temporal.runs", 5);
  private static String sql(String shape) {
    String window = shape.equals("SESSION")
        ? "SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '10' SECOND)"
        : shape.equals("HOP")
        ? "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)"
        : shape.equals("CUMULATE")
            ? "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)"
            : "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
    return "SELECT k, window_start, window_end, MIN(v), MAX(v), COUNT(*) FROM TABLE(" + window
        + ") GROUP BY k, window_start, window_end";
  }

  static Stream<Arguments> fixtures() {
    return Arrays.stream(System.getProperty("window.temporal.phases", "ONE_PHASE,TWO_PHASE").split(","))
        .flatMap(phase -> Arrays.stream(System.getProperty("window.temporal.precisions", "3,9").split(","))
            .flatMap(precision -> Arrays.stream(System.getProperty("window.temporal.ltz", "false,true").split(","))
                .flatMap(ltz -> Arrays.stream(System.getProperty("window.temporal.shapes", "TUMBLE").split(","))
                    .map(shape -> Arguments.of(phase, Integer.parseInt(precision), Boolean.parseBoolean(ltz), shape)))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void compareCompleteJobs(String phase, int precision, boolean ltz, String shape) throws Exception {
    String query = sql(shape);
    if (ROWS <= 0 || GROUPS <= 0 || WARMUP < 1 || RUNS < 3 || (precision != 3 && precision != 9)) {
      throw new IllegalArgumentException("invalid fixture");
    }
    if (!List.of("ONE_PHASE", "TWO_PHASE").contains(phase)
        || !List.of("TUMBLE", "HOP", "CUMULATE", "SESSION").contains(shape)
        || (shape.equals("SESSION") && !phase.equals("ONE_PHASE"))) {
      throw new IllegalArgumentException("fixture is outside native temporal window admission");
    }
    java.time.LocalDateTime[] values = new java.time.LocalDateTime[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = java.time.LocalDateTime.of(2024, 11, 3, 5, 30).plusSeconds(i % 61)
          .withNano(123_000_000 + (precision == 9 ? i : 0));
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var witness = environment(metrics, phase, values, precision, ltz);
      witness.executeSql("CREATE TABLE sink (k BIGINT, window_start TIMESTAMP(3), "
          + "window_end TIMESTAMP(3), lo " + valueSqlType(precision, ltz) + ", hi " + valueSqlType(precision, ltz) + ", n BIGINT) WITH ('connector' = 'blackhole')");
      String plan = NativePlanner.explain(witness, "INSERT INTO sink " + query);
      assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
      String operator = shape.equals("SESSION") ? "NativeColumnarSessionWindowAggregate"
          : phase.equals("TWO_PHASE") ? "NativeColumnarGlobalWindowAggregate"
          : "NativeColumnarWindowAggregate";
      assertTrue(plan.contains(operator), plan);
      if (phase.equals("TWO_PHASE")) {
        assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
      }
      double[][] samples = new double[3][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        var outputs = new ArrayList<List<List<Object>>>();
        for (int engine = 0; engine < 3; engine++) outputs.add(null);
        for (int turn = 0; turn < 3; turn++) {
          int engine = (trial + turn) % 3;
          var table = environment(metrics, phase, values, precision, ltz);
          if (engine == 2) {
            table.getConfig().set("streamfusion.operator.windowAggregate.enabled", "false");
            table.getConfig().set("streamfusion.operator.localWindowAggregate.enabled", "false");
            table.getConfig().set("streamfusion.operator.globalWindowAggregate.enabled", "false");
          }
          var scan = engine > 0 ? NativePlanner.install(table) : null;
          var output = new ArrayList<List<Object>>();
          long start = System.nanoTime();
          var result = table.executeSql(query);
          String jobId = result.getJobClient().orElseThrow().getJobID().toString();
          try (var rows = result.collect()) {
            while (rows.hasNext()) {
              Row row = rows.next();
              var fields = new ArrayList<Object>();
              fields.add(row.getKind());
              for (int col = 0; col < row.getArity(); col++) fields.add(row.getField(col));
              output.add(fields);
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(Locale.ROOT,
              "[temporal-window-trial] shape=%s phase=%s precision=%d ltz=%s engine=%d trial=%d warmup=%s seconds=%.6f%n",
              shape, phase, precision, ltz, engine, trial, trial < WARMUP, seconds);
          outputs.set(engine, output);
          if (scan != null) {
            assertEquals(engine == 1, scan.substitutions() > 0, scan.explainSummary());
            if (engine == 1) {
              var execution = metrics.nativeOperators().stream()
                  .filter(row -> jobId.equals(row.get("jobId")))
                  .collect(java.util.stream.Collectors.toList());
              RecoveryExecutionMetrics.assertConsumed(execution, operator);
              if (phase.equals("TWO_PHASE")) {
                RecoveryExecutionMetrics.assertConsumed(execution, "NativeColumnarLocalWindowAggregate");
              }
            }
          }
          if (trial >= WARMUP) samples[engine][trial - WARMUP] = seconds;
        }
        for (int engine = 1; engine < 3; engine++) {
          assertEquals(NativeParity.multiset(outputs.get(0)), NativeParity.multiset(outputs.get(engine)));
        }
      }
      System.out.printf(Locale.ROOT,
          "[temporal-window-job] shape=%s phase=%s precision=%d ltz=%s rows=%d groups=%d stock=%.6fs native=%.6fs fallback=%.6fs stock_native=%.3fx stock_samples=%s native_samples=%s fallback_samples=%s%n",
          shape, phase, precision, ltz, ROWS, GROUPS, median(samples[0]), median(samples[1]), median(samples[2]),
          median(samples[0]) / median(samples[1]), Arrays.toString(samples[0]),
          Arrays.toString(samples[1]), Arrays.toString(samples[2]));
    }
  }

  private static double median(double[] samples) {
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static String valueSqlType(int precision, boolean ltz) {
    return (ltz ? "TIMESTAMP_LTZ(" : "TIMESTAMP(") + precision + ")";
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, String phase,
      java.time.LocalDateTime[] values, int precision, boolean ltz) {
    var env = metrics.environment(new Configuration(), 2, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var valueType = ltz ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision);
    var type = DataTypes.ROW(DataTypes.FIELD("k", DataTypes.BIGINT()),
        DataTypes.FIELD("v", valueType), DataTypes.FIELD("ts", DataTypes.BIGINT()));
    var input = env.fromSequence(0, ROWS - 1).map(id -> {
      var value = (GROUPS > 1 && id % GROUPS == 0) || id % 7 == 0 ? null
          : values[(int) ((id + id / GROUPS) % values.length)];
      return Row.of(id % GROUPS, value == null ? null
          : ltz ? value.toInstant(java.time.ZoneOffset.UTC) : value, 1000L + id % 10000);
    }).returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView("src", input, Schema.newBuilder().fromRowDataType(type)
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
