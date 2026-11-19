package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class OverBasicValuesBenchmark {
  private static final java.math.BigInteger WIDE_BASE = java.math.BigInteger.TEN.pow(37);
  private static final String WIDE_TEXT = "x".repeat(256);
  @Test
  void completeJobsKeepRowSourceSinkAndBothTransposes() throws Exception {
    for (String type :
        System.getProperty("over.values.types", "decimal,string,timestamp,ltz").split(",")) {
      for (String frame : System.getProperty("over.values.frames", "0,1,2,3").split(",")) {
        measure(type, Integer.parseInt(frame));
      }
    }
  }

  private static void measure(String type, int frame) throws Exception {
    int rows = Integer.getInteger("over.values.rows", 1_000_000);
    int keys = Integer.getInteger("over.values.keys", 64);
    int warmups = Integer.getInteger("over.values.warmups", 2);
    int runs = Integer.getInteger("over.values.runs", 3);
    String bounds;
    switch (frame) {
      case 0:
        bounds = "RANGE UNBOUNDED PRECEDING";
        break;
      case 1:
        bounds = "ROWS BETWEEN 16 PRECEDING AND CURRENT ROW";
        break;
      case 2:
        bounds = "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW";
        break;
      case 3:
        bounds = "ROWS UNBOUNDED PRECEDING";
        break;
      default:
        throw new IllegalArgumentException("Unknown frame " + frame);
    }
    if (rows < 1 || keys < 1 || warmups < 1 || runs < 3)
      throw new IllegalArgumentException("Invalid fixture");
    int engines = Integer.getInteger("over.values.engines", 3);
    if (engines < 2 || engines > 3) throw new IllegalArgumentException("Expected two or three engines");
    boolean countOnly = Boolean.getBoolean("over.values.countOnly");
    boolean distinctCount = Boolean.getBoolean("over.values.distinctCount");
    boolean expectNative =
        Boolean.parseBoolean(System.getProperty("over.values.expectNative", "true"));
    String reductions = countOnly ? "COUNT(v) OVER w" : "COUNT(v) OVER w, MIN(v) OVER w, MAX(v) OVER w";
    if (distinctCount) reductions = "COUNT(DISTINCT v) OVER w";
    if (!distinctCount && !countOnly && (!(type.equals("timestamp") || type.equals("ltz") || type.equals("date") || type.equals("time"))
        || tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast())) {
      reductions += ", FIRST_VALUE(v) OVER w, LAST_VALUE(v) OVER w";
    }
    if (!distinctCount && !countOnly && type.equals("decimal")) reductions += ", SUM(v) OVER w, AVG(v) OVER w";
    if (Boolean.getBoolean("over.values.distinctReductions"))
      reductions = "SUM(DISTINCT v) OVER w, AVG(DISTINCT v) OVER w, COUNT(DISTINCT v) OVER w";
    String query =
        "SELECT k, "
            + reductions
            + " FROM src WINDOW w AS (PARTITION BY k ORDER BY rt "
            + bounds
            + ")";
    double[][] samples = new double[engines][runs];
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int turn = 0; turn < engines; turn++) {
          int engine = (trial + turn) % engines;
          var table = environment(metrics, type, rows, keys);
          if (engine == 2) table.getConfig().set("streamfusion.operator.over.enabled", "false");
          var scan = engine > 0 ? NativePlanner.install(table) : null;
          var resultType = table.sqlQuery(query).getResolvedSchema();
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(resultType).build())
                  .build());
          String insert = "INSERT INTO sink " + query;
          String plan = table.explainSql(insert);
          if (engine == 1 && expectNative)
            assertTrue(
                plan.contains("NativeOverAggregate")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          if (engine == 2 || engine == 1 && !expectNative)
            assertTrue(!plan.contains("NativeOverAggregate"), plan);
          long start = System.nanoTime();
          var result = table.executeSql(insert);
          String jobId = result.getJobClient().orElseThrow().getJobID().toString();
          result.await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expectNative) {
            assertTrue(scan.substitutions() > 0, scan.explainSummary());
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators().stream()
                    .filter(row -> jobId.equals(row.get("jobId")))
                    .collect(java.util.stream.Collectors.toList()),
                "NativeOverAggregate");
          }
          System.out.printf(
              Locale.ROOT,
              "[over-value-trial] type=%s frame=%d rows=%d keys=%d engine=%d trial=%d warmup=%s"
                  + " seconds=%.6f%n",
              type,
              frame,
              rows,
              keys,
              engine,
              trial,
              trial < warmups,
              seconds);
          if (trial >= warmups) samples[engine][trial - warmups] = seconds;
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[over-value-job] type=%s frame=%d rows=%d keys=%d stock=%.6f native=%.6f fallback=%.6f"
            + " stock_native=%.3f stock_samples=%s native_samples=%s fallback_samples=%s%n",
        type,
        frame,
        rows,
        keys,
        median(samples[0]),
        median(samples[1]),
        engines == 3 ? median(samples[2]) : Double.NaN,
        median(samples[0]) / median(samples[1]),
        Arrays.toString(samples[0]),
        Arrays.toString(samples[1]),
        engines == 3 ? Arrays.toString(samples[2]) : "[]");
  }

  private static double median(double[] values) {
    double[] copy = values.clone();
    Arrays.sort(copy);
    return copy[copy.length / 2];
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, String type, int rows, int keys) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().setAutoWatermarkInterval(50L);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    org.apache.flink.table.types.DataType valueType;
    switch (type) {
      case "decimal":
        valueType = DataTypes.DECIMAL(12, 2);
        break;
      case "decimal_wide":
      case "decimal_wide_balanced":
        valueType = DataTypes.DECIMAL(38, 0);
        break;
      case "string":
      case "string_wide":
        valueType = DataTypes.STRING();
        break;
      case "timestamp":
        valueType = DataTypes.TIMESTAMP(9);
        break;
      case "ltz":
        valueType = DataTypes.TIMESTAMP_LTZ(9);
        break;
      case "date":
        valueType = DataTypes.DATE();
        break;
      case "time":
        valueType = DataTypes.TIME(3);
        break;
      case "boolean":
        valueType = DataTypes.BOOLEAN();
        break;
      default:
        throw new IllegalArgumentException("Unknown value type " + type);
    }
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.BIGINT()),
            DataTypes.FIELD("v", valueType),
            DataTypes.FIELD("ts", DataTypes.BIGINT()));
    var source =
        env.fromSequence(0, rows - 1)
            .map(
                id -> {
                  Object value = null;
                  if (id % 7 != 0 && (keys == 1 || id % keys != 0)) {
                    switch (type) {
                      case "decimal":
                        value = BigDecimal.valueOf(id % 1000 - 500, 2);
                        break;
                      case "decimal_wide":
                        value = new BigDecimal(WIDE_BASE.add(java.math.BigInteger.valueOf(id / keys % 257)));
                        break;
                      case "decimal_wide_balanced":
                        var wide = WIDE_BASE.add(java.math.BigInteger.valueOf(id / keys % 257));
                        value = new BigDecimal(id / keys % 2 == 0 ? wide : wide.negate());
                        break;
                      case "string":
                      case "string_wide":
                        value =
                            new String[] {"😀", "\ue000", "", "é", "abcdefghijk"}
                                [(int) (id / keys % 5)];
                        if (type.equals("string_wide")) value = value + WIDE_TEXT;
                        break;
                      case "date":
                        value = LocalDate.of(2024, 1, 1).plusDays(id % 31);
                        break;
                      case "time":
                        value = LocalTime.of(12, 34, 56, (int) (id % 1000) * 1000000);
                        break;
                      case "boolean":
                        value = id / keys % 2 == 0;
                        break;
                      default:
                        var time =
                            LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123456789 + (int) (id % 1000));
                        value = type.equals("ltz") ? time.toInstant(ZoneOffset.UTC) : time;
                    }
                  }
                  return Row.of(id % keys, value, 1000L + id / keys * 400L);
                })
            .returns(ExternalTypeInfo.<Row>of(rowType))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(
                        Duration.ofMillis(Long.getLong("over.values.watermarkDelayMillis", 0L)))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .fromRowDataType(rowType)
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
