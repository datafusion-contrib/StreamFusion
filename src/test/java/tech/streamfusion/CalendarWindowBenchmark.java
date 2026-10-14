package tech.streamfusion;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class CalendarWindowBenchmark {
  @Test
  void compareCompleteCalendarJobs() throws Exception {
    if (!"false".equals(System.getProperty("streamfusion.exchange.zeroCopyLocal")))
      throw new IllegalStateException("Use -Pbench to retain IPC costs");
    for (String zone :
        System.getProperty("calendar.zones", "GMT+05:45,America/Los_Angeles").split(","))
      for (String shape : System.getProperty("calendar.shapes", "TUMBLE,HOP,CUMULATE").split(","))
        for (String consumer :
            System.getProperty("calendar.consumers", "aggregate,two_phase,tvf,rank,dedup,join")
                .split(",")) measure(zone, shape, consumer);
  }

  private static void measure(String zone, String shape, String consumer) throws Exception {
    long rows = Long.getLong("calendar.rows", 2_000_000L);
    int keys = Integer.getInteger("calendar.keys", 64),
        width = Integer.getInteger("calendar.width", 0);
    int warmups = Integer.getInteger("calendar.warmup", 2),
        runs = Integer.getInteger("calendar.runs", 3);
    if (warmups < 1 || runs < 3)
      throw new IllegalArgumentException("Warmup and at least three measurements required");
    String backend = System.getProperty("calendar.backend", "memory");
    String input =
        "TABLE("
            + shape
            + "(TABLE src, DESCRIPTOR(rt), "
            + (shape.equals("TUMBLE") ? "INTERVAL '1' DAY" : "INTERVAL '6' HOUR, INTERVAL '1' DAY")
            + "))";
    String sql;
    if (shape.equals("SESSION")) {
      if (!consumer.equals("aggregate"))
        throw new IllegalArgumentException("SESSION benchmark uses single-phase aggregation");
      sql =
          "SELECT k, SESSION_START(rt, INTERVAL '5' MINUTE) AS window_start, SESSION_END(rt,"
              + " INTERVAL '5' MINUTE) AS window_end, SUM(v), COUNT(*) FROM src GROUP BY k,"
              + " SESSION(rt, INTERVAL '5' MINUTE)";
    } else if (consumer.equals("tvf"))
      sql = "SELECT k, v, rt, window_start, window_end, window_time FROM " + input;
    else if (consumer.equals("aggregate") || consumer.equals("two_phase"))
      sql =
          "SELECT k, window_start, window_end, SUM(v), COUNT(*) FROM "
              + input
              + " GROUP BY k, window_start, window_end";
    else if (consumer.equals("join"))
      sql =
          "SELECT l.k, l.v, r.v FROM "
              + "(SELECT * FROM "
              + input
              + ")"
              + " l JOIN "
              + "(SELECT * FROM "
              + input.replace("TABLE src, DESCRIPTOR(rt)", "TABLE rhs, DESCRIPTOR(right_rt)")
              + ")"
              + " r ON l.k = r.k AND l.v = r.v AND l.window_start = r.window_start AND l.window_end"
              + " = r.window_end";
    else
      sql =
          "SELECT k, v, window_start, window_end, rn FROM (SELECT *, ROW_NUMBER() OVER (PARTITION"
              + " BY k, window_start, window_end ORDER BY "
              + (consumer.equals("dedup") ? "rt DESC" : "v DESC")
              + ") rn FROM "
              + input
              + ") WHERE rn <= "
              + (consumer.equals("dedup") ? "1" : "4");
    String required =
        consumer.equals("join")
            ? "NativeWindowJoin"
            : consumer.equals("rank") || consumer.equals("dedup")
                ? "NativeWindowRank"
                : consumer.equals("tvf")
                    ? "NativeWindowTableFunction"
                    : consumer.equals("two_phase")
                        ? "NativeColumnarGlobalWindowAggregate"
                        : "NativeColumnarWindowAggregate";
    if (shape.equals("SESSION")) required = "NativeColumnarSessionWindowAggregate";
    double[][] seconds = new double[3][runs];
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < warmups + runs; trial++)
        for (int turn = 0; turn < 3; turn++) {
          int engine = (trial + turn) % 3;
          var config = new Configuration();
          if (backend.equals("rocksdb"))
            config.setString(
                "state.backend.type",
                engine == 1
                    ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory"
                    : "rocksdb");
          var env = metrics.environment(config, 1, 0);
          env.getConfig().setAutoWatermarkInterval(50);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(ZoneId.of(zone));
          table.getConfig().set("table.exec.sink.rowtime-inserter", "DISABLED");
          table
              .getConfig()
              .set(
                  "table.optimizer.agg-phase-strategy",
                  consumer.equals("two_phase") ? "TWO_PHASE" : "ONE_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "50 ms");
          table.getConfig().set("table.exec.mini-batch.size", "1024");
          table.createTemporaryView(
              "src",
              env.fromSequence(0, rows - 1)
                  .map(
                      i ->
                          Row.of(
                              "key" + (i % keys) + "x".repeat(width),
                              i,
                              Instant.ofEpochMilli(1710028800000L + i * 1000)))
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"k", "v", "rt"}, Types.STRING, Types.LONG, Types.INSTANT))
                  .assignTimestampsAndWatermarks(
                      WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ZERO)
                          .withTimestampAssigner(
                              (row, previous) -> ((Instant) row.getField(2)).toEpochMilli())),
              Schema.newBuilder()
                  .column("k", DataTypes.STRING())
                  .column("v", DataTypes.BIGINT())
                  .column("rt", DataTypes.TIMESTAMP_LTZ(3))
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .build());
          table.executeSql("CREATE TEMPORARY VIEW rhs AS SELECT k, v, rt AS right_rt FROM src");
          if (engine == 2)
            for (String operator :
                new String[] {
                  "sessionWindowAggregate",
                  "windowAggregate",
                  "localWindowAggregate",
                  "globalWindowAggregate",
                  "windowTableFunction",
                  "windowJoin",
                  "windowRank",
                  "windowDeduplicate",
                  "groupWindowAggregate"
                }) table.getConfig().set("streamfusion.operator." + operator + ".enabled", "false");
          if (engine > 0) NativePlanner.install(table);
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1
              && (!plan.contains(required)
                  || !plan.contains("RowDataToArrow")
                  || !plan.contains("ArrowToRowData")))
            throw new IllegalStateException(
                "Missing calendar native operator or transposes: " + plan);
          if (engine == 2 && plan.contains(required))
            throw new IllegalStateException(
                "Previous fallback control retained native calendar operator: " + plan);
          long start = System.nanoTime();
          var result = table.executeSql(insert);
          result.await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (engine == 1)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), required);
          System.out.printf(
              Locale.ROOT,
              "[calendar-trial] zone=%s shape=%s consumer=%s engine=%d trial=%d warmup=%s"
                  + " seconds=%.6f%n",
              zone,
              shape,
              consumer,
              engine,
              trial,
              trial < warmups,
              elapsed);
          if (trial >= warmups) seconds[engine][trial - warmups] = elapsed;
        }
    }
    System.out.printf(
        Locale.ROOT,
        "[calendar-job] zone=%s shape=%s consumer=%s backend=%s rows=%d keys=%d width=%d stock=%s"
            + " native=%s previous_fallback=%s%n",
        zone,
        shape,
        consumer,
        backend,
        rows,
        keys,
        width,
        Arrays.toString(seconds[0]),
        Arrays.toString(seconds[1]),
        Arrays.toString(seconds[2]));
  }
}
