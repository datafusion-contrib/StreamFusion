package tech.streamfusion;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowComplexCountBenchmark {
  @Test
  void completeJobsKeepBothTransposesAndTypedWindowStateInTheMeasurement() throws Exception {
    if (!"false".equals(System.getProperty("streamfusion.exchange.zeroCopyLocal"))) {
      throw new IllegalStateException("Use -Pbench to retain production Arrow IPC shuffle costs");
    }
    for (String value : System.getProperty("window.cases", "array,map,row").split(",")) {
      for (String phase : System.getProperty("window.phases", "ONE_PHASE,TWO_PHASE").split(",")) {
        for (String shape : System.getProperty("window.shapes", "TUMBLE").split(",")) {
          if (shape.equals("SESSION") && phase.equals("TWO_PHASE")) continue;
          measure(value, phase, shape);
        }
      }
    }
  }

  private static void measure(String value, String phase, String shape) throws Exception {
    long rows = Long.getLong("window.rows", 2_000_000L);
    int keys = Integer.getInteger("window.keys", 64);
    int warmup = Integer.getInteger("window.warmup", 2);
    int runs = Integer.getInteger("window.runs", 5);
    boolean expectNative = Boolean.parseBoolean(System.getProperty("window.expectNative", "true"));
    String backend = System.getProperty("window.backend", "memory");
    double[][] times = new double[2][runs];
    String aggregate = "COUNT(v), COUNT(*)";
    String window;
    switch (shape) {
      case "TUMBLE":
        window = "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
        break;
      case "HOP":
        window = "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
        break;
      case "CUMULATE":
        window = "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
        break;
      case "SESSION":
        window = "SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
        break;
      default:
        throw new IllegalArgumentException("Unknown window.shapes entry " + shape);
    }
    String sql =
        "SELECT k, window_start, window_end, "
            + aggregate
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    String operator =
        shape.equals("SESSION")
            ? "NativeColumnarSessionWindowAggregate"
            : phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate";
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < warmup + runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          var jobConfig = new Configuration();
          if (backend.equals("rocksdb")) {
            jobConfig.setString(
                "state.backend.type",
                engine == 1
                    ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory"
                    : "rocksdb");
          } else if (!backend.equals("memory")) {
            throw new IllegalArgumentException("Unknown window.backend " + backend);
          }
          var env = metrics.environment(jobConfig, 2, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
          table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
          table
              .getConfig()
              .set(
                  "table.optimizer.distinct-agg.split.enabled",
                  Boolean.toString(Boolean.getBoolean("window.splitDistinct")));
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "1024");
          org.apache.flink.table.types.DataType payloadType;
          switch (value) {
            case "array":
              payloadType = DataTypes.ARRAY(DataTypes.BIGINT());
              break;
            case "map":
              payloadType = DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT());
              break;
            case "multiset":
              payloadType = DataTypes.MULTISET(DataTypes.STRING());
              break;
            case "row":
              payloadType = DataTypes.ROW(DataTypes.FIELD("child", DataTypes.BIGINT()));
              break;
            default:
              throw new IllegalArgumentException(value);
          }
          var type =
              DataTypes.ROW(
                  DataTypes.FIELD("k", DataTypes.INT()),
                  DataTypes.FIELD("v", payloadType),
                  DataTypes.FIELD("ts", DataTypes.BIGINT()));
          int width = Integer.getInteger("window.width", 4);
          var input =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        Object payload = null;
                        if (i % 7 != 0) {
                          switch (value) {
                            case "array":
                              Long[] array = new Long[(int) (i % 3 == 0 ? 0 : width)];
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
                        return Row.of((int) (i % keys), payload, 1704067200000L + i % 5000);
                      })
                  .returns(ExternalTypeInfo.<Row>of(type))
                  .assignTimestampsAndWatermarks(
                      WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                          .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
          table.createTemporaryView(
              "src",
              input,
              Schema.newBuilder()
                  .fromRowDataType(type)
                  .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                  .watermark("rt", "SOURCE_WATERMARK()")
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
          if (engine == 1
              && expectNative
              && (!plan.contains(operator)
                  || !plan.contains("RowDataToArrow")
                  || !plan.contains("ArrowToRowData")
                  || (phase.equals("TWO_PHASE")
                      && !plan.contains("NativeColumnarLocalWindowAggregate")))) {
            throw new IllegalStateException(
                "Expected typed native windows and both transposes: " + plan);
          }
          if (engine == 1 && !expectNative && plan.contains(operator)) {
            throw new IllegalStateException("Expected previous window aggregate fallback: " + plan);
          }
          if (engine == 1
              && expectNative
              && Boolean.getBoolean("window.splitDistinct")
              && plan.split("NativeColumnarGlobalWindowAggregate", -1).length < 3) {
            throw new IllegalStateException("Expected both split-window global stages: " + plan);
          }
          if (Boolean.getBoolean("window.splitDistinct") && trial == 0)
            System.out.println("[complex-window-plan] engine=" + engine + " " + plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[complex-window-trial] case=%s phase=%s shape=%s engine=%s trial=%d warmup=%s"
                  + " seconds=%.6f%n",
              value,
              phase,
              shape,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmup,
              seconds);
          if (trial >= warmup) times[engine][trial - warmup] = seconds;
        }
      }
      if (expectNative)
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator);
      if (expectNative && phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalWindowAggregate");
    }
    System.out.printf(
        Locale.ROOT,
        "[complex-window] case=%s phase=%s shape=%s backend=%s native_expected=%s rows=%d keys=%d"
            + " host_mean=%.6fs native_mean=%.6fs host_trials=%s native_trials=%s%n",
        value,
        phase,
        shape,
        backend,
        expectNative,
        rows,
        keys,
        Arrays.stream(times[0]).average().orElseThrow(),
        Arrays.stream(times[1]).average().orElseThrow(),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
