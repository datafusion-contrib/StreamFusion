package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class StringAggregateBenchmark {
  @Test
  void completeJobsRetainStringStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    long rows = Long.getLong("stringagg.rows", 2_000_000L);
    int keys = Integer.getInteger("stringagg.keys", 65_536);
    int width = Integer.getInteger("stringagg.width", 16);
    int warmups = Integer.getInteger("stringagg.warmup", 2);
    int runs = Integer.getInteger("stringagg.runs", 5);
    boolean updating = Boolean.getBoolean("stringagg.updating");
    boolean expectNative =
        Boolean.parseBoolean(System.getProperty("stringagg.expectNative", "true"));
    String phase = System.getProperty("stringagg.phase", "ONE_PHASE");
    assertTrue(phase.equals("ONE_PHASE") || phase.equals("TWO_PHASE"));
    boolean split = phase.equals("TWO_PHASE");
    boolean miniBatch =
        Boolean.parseBoolean(System.getProperty("stringagg.miniBatch", Boolean.toString(split)));
    assertTrue(!split || miniBatch);
    String retention = System.getProperty("stringagg.ttl", "");
    System.out.println("[string-aggregate-retention] " + retention + " minibatch=" + miniBatch);
    System.out.println("[string-aggregate-phase] " + phase);
    String function = System.getProperty("stringagg.function", "LISTAGG");
    String valueType = System.getProperty("stringagg.value", "string");
    assertTrue(valueType.equals("string") || valueType.equals("bigint"));
    String argument = valueType.equals("bigint") ? "CAST(k AS BIGINT)" : "v";
    System.out.println("[string-aggregate-value] " + valueType);
    String aggregate;
    switch (function) {
      case "LISTAGG":
        aggregate =
            "LISTAGG("
                + (Boolean.getBoolean("stringagg.distinct") ? "DISTINCT " : "")
                + "v"
                + (Boolean.parseBoolean(System.getProperty("stringagg.literalSeparator", "true"))
                    ? ", ','"
                    : "")
                + ")";
        break;
      case "JSON_ARRAYAGG":
        String order = System.getProperty("stringagg.order", "");
        if (!order.isEmpty() && !order.equals("ASC") && !order.equals("DESC"))
          throw new IllegalArgumentException("stringagg.order must be ASC or DESC");
        aggregate =
            "JSON_ARRAYAGG("
                + argument
                + (order.isEmpty() ? "" : " ORDER BY json_key " + order)
                + ")";
        break;
      case "JSON_OBJECTAGG":
        aggregate = "JSON_OBJECTAGG(json_key VALUE " + argument + ")";
        break;
      default:
        throw new IllegalArgumentException("Unknown string aggregate " + function);
    }
    assertTrue(rows > 0 && keys > 0 && width > 0 && runs > 0 && warmups >= 0);
    String sql = "SELECT k, " + aggregate + " AS aggregated_value FROM src GROUP BY k";
    String[] values =
        java.util.stream.IntStream.range(0, 16)
            .mapToObj(i -> Integer.toHexString(i).repeat(width))
            .toArray(String[]::new);
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        int engine = (trial + order) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 1, 0);
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
          table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
          if (miniBatch) {
            table
                .getConfig()
                .set(
                    "table.exec.mini-batch.size",
                    System.getProperty("stringagg.batchSize", "1024"));
            table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          }
          if (!retention.isEmpty()) table.getConfig().set("table.exec.state.ttl", retention);
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        boolean retract = updating && i / keys % 3 == 2;
                        long valueIndex = retract ? i - keys : i;
                        return Row.ofKind(
                            retract ? RowKind.DELETE : RowKind.INSERT,
                            (int) (i % keys),
                            valueIndex % 7 == 0
                                ? null
                                : values[(int) (valueIndex / keys % values.length)],
                            Long.toString(valueIndex));
                      })
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"k", "v", "json_key"},
                          Types.INT,
                          Types.STRING,
                          Types.STRING));
          var input =
              updating
                  ? table.fromChangelogStream(
                      source, Schema.newBuilder().build(), ChangelogMode.all())
                  : table.fromDataStream(source);
          table.createTemporaryView("src", input);
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expectNative) {
            assertTrue(
                plan.contains("NativeColumnarGroupAggregate")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
            if (split) assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
          } else if (engine == 1) assertEquals(0, scan.substitutions(), plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expectNative)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeColumnarGroupAggregate");
          if (engine == 1 && expectNative && split)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
          System.out.printf(
              Locale.ROOT,
              "[string-aggregate-trial] function=%s updating=%s engine=%s rows=%d keys=%d width=%d"
                  + " trial=%d warmup=%s seconds=%.6f%n",
              function,
              updating,
              engine == 0 ? "Flink" : "StreamFusion",
              rows,
              keys,
              width,
              trial,
              trial < warmups,
              seconds);
          if (trial >= warmups) times[engine][trial - warmups] = seconds;
        }
      }
    }
    double[] stock = times[0].clone(), nativeTimes = times[1].clone();
    Arrays.sort(stock);
    Arrays.sort(nativeTimes);
    System.out.printf(
        Locale.ROOT,
        "[string-aggregate] function=%s updating=%s rows=%d keys=%d width=%d stock=%.6f native=%.6f"
            + " stock_trials=%s native_trials=%s%n",
        function,
        updating,
        rows,
        keys,
        width,
        stock[runs / 2],
        nativeTimes[runs / 2],
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
