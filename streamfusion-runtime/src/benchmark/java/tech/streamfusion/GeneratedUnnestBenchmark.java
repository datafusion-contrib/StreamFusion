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
class GeneratedUnnestBenchmark {
  @Test
  void completeJobsRetainConstructorPredicateAndRepresentationCosts() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    long rows = Long.getLong("generated.rows", 2_000_000L);
    int entries = Integer.getInteger("generated.entries", 4);
    int bytes = Integer.getInteger("generated.bytes", 8);
    int keys = Integer.getInteger("generated.keys", 4096);
    int warmup = Integer.getInteger("generated.warmup", 2);
    int runs = Integer.getInteger("generated.runs", 3);
    String shape = System.getProperty("generated.shape", "map_numeric");
    boolean grouped = Boolean.getBoolean("generated.grouped");
    boolean updating = Boolean.getBoolean("generated.updating");
    boolean expected = Boolean.parseBoolean(System.getProperty("generated.expectNative", "true"));
    assertTrue(rows > 0 && entries > 0 && bytes > 0 && keys > 0 && warmup >= 0 && runs > 0);
    String sql;
    if (shape.equals("left_predicate")) {
      sql =
          "SELECT "
              + (grouped ? "e,COUNT(*)" : "id,e")
              + " FROM src LEFT JOIN UNNEST(a) AS u(e) ON MOD(e,2)=0"
              + (grouped ? " GROUP BY e" : "");
    } else {
      StringBuilder operand = new StringBuilder(shape.equals("array_string") ? "ARRAY[" : "MAP[");
      for (int j = 0; j < entries; j++) {
        if (j > 0) operand.append(',');
        if (!shape.equals("array_string"))
          operand
              .append(shape.equals("map_string") ? "'key" + j + "'" : "CAST(" + j + " AS BIGINT)")
              .append(',');
        switch (shape) {
          case "map_numeric":
            operand.append("v+").append(j);
            break;
          case "map_string":
          case "array_string":
            operand.append("CONCAT(s,'").append(j).append("')");
            break;
          case "map_row":
            operand.append("ROW(s,v+").append(j).append(')');
            break;
          default:
            throw new IllegalArgumentException(shape);
        }
      }
      operand.append(']');
      boolean array = shape.equals("array_string");
      sql =
          "SELECT "
              + (grouped ? "mk,COUNT(*)" : array ? "id,mk" : "id,mk,mv")
              + " FROM src CROSS JOIN UNNEST("
              + operand
              + ") AS u("
              + (array ? "mk" : "mk,mv")
              + ")"
              + (grouped ? " GROUP BY mk" : "");
    }
    String text = "x".repeat(bytes);
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmup + runs; trial++)
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          table.getConfig().set("table.exec.mini-batch.enabled", "false");
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        long original = updating ? i / 4 : i;
                        Long[] a =
                            original % 11 == 0 ? null : new Long[original % 13 == 0 ? 0 : entries];
                        if (a != null)
                          for (int j = 0; j < a.length; j++)
                            a[j] = (original + j) % 7 == 0 ? null : (original + j) % keys;
                        return Row.ofKind(
                            updating ? RowKind.fromByteValue((byte) (i % 4)) : RowKind.INSERT,
                            original % keys,
                            original % 11 == 0 ? null : original % keys,
                            original % 17 == 0 ? null : text,
                            a);
                      })
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"id", "v", "s", "a"},
                          Types.LONG,
                          Types.LONG,
                          Types.STRING,
                          Types.OBJECT_ARRAY(Types.LONG)));
          table.createTemporaryView(
              "src",
              updating
                  ? table.fromChangelogStream(
                      source, Schema.newBuilder().build(), ChangelogMode.all())
                  : table.fromDataStream(source));
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "snk",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO snk " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expected) {
            assertTrue(
                plan.contains("NativeGeneratedUnnest")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          } else if (engine == 1) assertEquals(0, scan.substitutions(), plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expected) {
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeGeneratedUnnest");
            if (grouped)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeColumnarGroupAggregate");
          }
          System.out.printf(
              Locale.ROOT,
              "[generated-unnest-trial] shape=%s grouped=%s updating=%s rows=%d entries=%d bytes=%d"
                  + " keys=%d engine=%s trial=%d warmup=%s seconds=%.9f%n",
              shape,
              grouped,
              updating,
              rows,
              entries,
              bytes,
              keys,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmup,
              elapsed);
          if (trial >= warmup) times[engine][trial - warmup] = elapsed;
        }
      }
    double[] stock = times[0].clone(), nativeTimes = times[1].clone();
    Arrays.sort(stock);
    Arrays.sort(nativeTimes);
    System.out.printf(
        Locale.ROOT,
        "[generated-unnest] shape=%s grouped=%s updating=%s rows=%d entries=%d bytes=%d keys=%d"
            + " stock=%.9f native=%.9f stock_trials=%s native_trials=%s%n",
        shape,
        grouped,
        updating,
        rows,
        entries,
        bytes,
        keys,
        stock[runs / 2],
        nativeTimes[runs / 2],
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
