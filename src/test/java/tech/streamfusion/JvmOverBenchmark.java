package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
class JvmOverBenchmark {
  @Test
  void completeJobsRetainReleasedFramesManagedStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("jvmover.rows", 262144),
        keys = Integer.getInteger("jvmover.keys", 1024);
    int warmups = Integer.getInteger("jvmover.warmup", 2),
        runs = Integer.getInteger("jvmover.runs", 5);
    boolean expected = Boolean.parseBoolean(System.getProperty("jvmover.expectNative", "true"));
    String name = System.getProperty("jvmover.type", "BIGINT");
    String order = System.getProperty("jvmover.order", "rt");
    String frame = System.getProperty("jvmover.frame", "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW");
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", valueType),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    String sql =
        "SELECT k,remember(v) OVER w,COUNT(*) OVER w FROM src WINDOW w AS (PARTITION BY k ORDER BY "
            + order
            + " "
            + frame
            + ")";
    double[][] elapsed = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++)
      try (var metrics = new RecoveryExecutionMetrics()) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) & 1;
          var env = metrics.environment(new Configuration(), 1, 0);
          env.getConfig().enableObjectReuse();
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          var source =
              env.fromSequence(0, rows - 1L)
                  .setParallelism(1)
                  .map(
                      id ->
                          Row.of(
                              id % 31 == 0 ? null : (int) (id % keys),
                              value(name, id),
                              LocalDateTime.ofInstant(
                                  java.time.Instant.ofEpochMilli(1000 + id / 32 * 100),
                                  ZoneOffset.UTC)))
                  .returns(ExternalTypeInfo.<Row>of(type))
                  .setParallelism(1)
                  .assignTimestampsAndWatermarks(
                      WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                          .withTimestampAssigner(
                              (row, previous) ->
                                  ((LocalDateTime) row.getField(2))
                                      .toInstant(ZoneOffset.UTC)
                                      .toEpochMilli()));
          table.createTemporaryView(
              "src",
              source,
              Schema.newBuilder()
                  .fromRowDataType(type)
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .columnByExpression("pt", "PROCTIME()")
                  .build());
          table.createTemporarySystemFunction(
              "remember", new FlinkTypedJvmAggregateSqlHarnessTest.Remember(valueType));
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(
                      Schema.newBuilder()
                          .fromResolvedSchema(table.sqlQuery(sql).getResolvedSchema())
                          .build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          String plan = table.explainSql(insert);
          if (engine == 1) {
            if (expected)
              assertTrue(
                  plan.contains("NativeJvmOver")
                      && plan.contains("RowDataToArrow")
                      && plan.contains("ArrowToRowData"),
                  plan);
            else assertEquals(0, scan.substitutions(), plan);
          }
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[jvm-over-trial] type=%s order=%s frame=%s rows=%d keys=%d engine=%s trial=%d"
                  + " warmup=%s seconds=%.6f%n",
              name,
              order,
              frame,
              rows,
              keys,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              seconds);
          if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
        }
        if (expected)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
        SharedFlinkCluster.assertNativeMemoryReleased("jvm-over-benchmark");
      }
    System.out.printf(
        Locale.ROOT,
        "[jvm-over-result] type=%s order=%s frame=%s rows=%d keys=%d stock=%.6f native=%.6f"
            + " stock_trials=%s native_trials=%s%n",
        name,
        order,
        frame,
        rows,
        keys,
        median(elapsed[0]),
        median(elapsed[1]),
        Arrays.toString(elapsed[0]),
        Arrays.toString(elapsed[1]));
  }

  private static Object value(String name, long id) {
    if (id % 31 == 0) return null;
    switch (name) {
      case "BIGINT":
        return id % 101 - 50;
      case "STRING":
        return "value-é🙂-" + (id % 101);
      case "DECIMAL(38,18)":
        return new java.math.BigDecimal(
            java.math.BigInteger.TEN.pow(37).add(java.math.BigInteger.valueOf(id % 101)), 18);
      default:
        throw new IllegalArgumentException(name);
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
