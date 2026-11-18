package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class OverComplexCountBenchmark {
  @Test
  void completeJobsRetainContainersConversionsAndFrameState() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String type : System.getProperty("over.cases", "array,map,row").split(","))
      for (String frame : System.getProperty("over.frames", "3,1").split(","))
        measure(type, Integer.parseInt(frame));
  }

  private static void measure(String value, int frame) throws Exception {
    int rows = Integer.getInteger("over.rows", 500000);
    int keys = Integer.getInteger("over.keys", 64);
    int width = Integer.getInteger("over.width", 4);
    int warmup = Integer.getInteger("over.warmup", 2);
    int runs = Integer.getInteger("over.runs", 3);
    boolean expected = Boolean.parseBoolean(System.getProperty("over.expectNative", "true"));
    boolean proctime = Boolean.getBoolean("over.proctime");
    String bounds;
    switch (frame) {
      case 0:
        bounds = "RANGE UNBOUNDED PRECEDING";
        break;
      case 1:
        bounds = "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";
        break;
      case 2:
        bounds = "RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW";
        break;
      case 3:
        bounds = "ROWS UNBOUNDED PRECEDING";
        break;
      default:
        throw new IllegalArgumentException(Integer.toString(frame));
    }
    var payloadType =
        value.equals("array")
            ? DataTypes.ARRAY(DataTypes.BIGINT())
            : value.equals("map")
                ? DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())
                : value.equals("multiset")
                    ? DataTypes.MULTISET(DataTypes.STRING())
                    : value.equals("row")
                        ? DataTypes.ROW(DataTypes.FIELD("child", DataTypes.BIGINT()))
                        : null;
    if (payloadType == null) throw new IllegalArgumentException(value);
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", payloadType),
            DataTypes.FIELD("ts", DataTypes.BIGINT()));
    String sql =
        "SELECT k, COUNT(v) OVER w, COUNT(*) OVER w FROM src WINDOW w AS (PARTITION BY k ORDER BY "
            + (proctime ? "pt" : "rt")
            + " "
            + bounds
            + ")";
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmup + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 2, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
          var input =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        Object payload = null;
                        if (i % 7 != 0) {
                          switch (value) {
                            case "array":
                              Long[] array = new Long[i % 3 == 0 ? 0 : width];
                              for (int j = 0; j < array.length; j++)
                                array[j] = j % 3 == 0 ? null : i % 256;
                              payload = array;
                              break;
                            case "map":
                              var map = new java.util.LinkedHashMap<String, Long>();
                              if (i % 3 != 0)
                                for (int j = 0; j < width; j++)
                                  map.put("k" + j, j % 3 == 0 ? null : i % 256);
                              payload = map;
                              break;
                            case "multiset":
                              var multiset = new java.util.LinkedHashMap<String, Integer>();
                              if (i % 3 != 0)
                                for (int j = 0; j < width; j++)
                                  multiset.put(j == 0 ? null : "k" + j, 1 + j % 5);
                              payload = multiset;
                              break;
                            case "row":
                              payload = Row.of(i % 3 == 0 ? null : i % 256);
                              break;
                            default:
                              throw new IllegalArgumentException(value);
                          }
                        }
                        return Row.of((int) (i % keys), payload, 1704067200000L + i / 8);
                      })
                  .returns(ExternalTypeInfo.<Row>of(rowType))
                  .assignTimestampsAndWatermarks(
                      WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                          .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
          table.createTemporaryView(
              "src",
              input,
              Schema.newBuilder()
                  .fromRowDataType(rowType)
                  .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .columnByExpression("pt", "PROCTIME()")
                  .build());
          if (engine == 1) NativePlanner.install(table);
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expected)
            assertTrue(
                plan.contains("NativeOverAggregate")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          if (engine == 1 && !expected) assertFalse(plan.contains("NativeOverAggregate"), plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expected)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeOverAggregate");
          System.out.printf(
              Locale.ROOT,
              "[over-complex-trial] type=%s frame=%d proctime=%s rows=%d keys=%d width=%d engine=%s"
                  + " trial=%d warmup=%s seconds=%.6f%n",
              value,
              frame,
              proctime,
              rows,
              keys,
              width,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmup,
              seconds);
          if (trial >= warmup) times[engine][trial - warmup] = seconds;
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[over-complex] type=%s frame=%d proctime=%s rows=%d keys=%d width=%d host_mean=%.6fs"
            + " native_mean=%.6fs host_trials=%s native_trials=%s%n",
        value,
        frame,
        proctime,
        rows,
        keys,
        width,
        Arrays.stream(times[0]).average().orElseThrow(),
        Arrays.stream(times[1]).average().orElseThrow(),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
