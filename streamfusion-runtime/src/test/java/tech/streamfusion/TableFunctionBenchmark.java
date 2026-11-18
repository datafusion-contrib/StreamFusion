package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TableFunctionBenchmark {
  @Test
  void completeJobsRetainSourceBothTransposesAndRowSink() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("tablefunc.rows", 2_000_000);
    int keys = Integer.getInteger("tablefunc.keys", 16_384);
    int width = Integer.getInteger("tablefunc.width", 16);
    int textCardinality = Integer.getInteger("tablefunc.textCardinality", 1);
    assertTrue(textCardinality > 0);
    System.out.printf("[table-function-config] text_cardinality=%d%n", textCardinality);
    int warmups = Integer.getInteger("tablefunc.warmup", 2);
    int runs = Integer.getInteger("tablefunc.runs", 5);
    boolean expectedNative =
        Boolean.parseBoolean(System.getProperty("tablefunc.expectNative", "true"));
    for (String shape : System.getProperty("tablefunc.cases", "split,group").split(",")) {
      String sql;
      switch (shape) {
        case "split":
          sql =
              "SELECT s.k, t.v, t.pos FROM src s, LATERAL TABLE(test_split(s.text_value)) AS t(v,"
                  + " pos)";
          break;
        case "group":
          sql =
              "SELECT s.k, t.pos, JSON_ARRAYAGG(t.v) FROM src s, "
                  + "LATERAL TABLE(test_split(s.text_value)) AS t(v, pos) GROUP BY s.k, t.pos";
          break;
        case "numeric":
          sql =
              "SELECT s.k, t.n, t.pos FROM src s, LATERAL TABLE(numeric_rows(s.k, s.n)) AS t(n,"
                  + " pos)";
          break;
        case "numeric-group":
          sql =
              "SELECT s.k, SUM(t.n), COUNT(*) FROM src s, "
                  + "LATERAL TABLE(numeric_rows(s.k, s.n)) AS t(n, pos) GROUP BY s.k";
          break;
        default:
          throw new IllegalArgumentException("Unknown tablefunc.cases: " + shape);
      }
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int order = 0; order < 2; order++) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            int engine = (trial + order) & 1;
            var table = environment(metrics, rows, keys, width, shape.startsWith("numeric"));
            table.createTemporaryTable(
                "sink",
                TableDescriptor.forConnector("blackhole")
                    .schema(
                        Schema.newBuilder()
                            .fromRowDataType(
                                table.sqlQuery(sql).getResolvedSchema().toPhysicalRowDataType())
                            .build())
                    .build());
            String insert = "INSERT INTO sink " + sql;
            var scan = engine == 1 ? NativePlanner.install(table) : null;
            String plan = table.explainSql(insert);
            if (engine == 1) {
              if (expectedNative)
                assertTrue(
                    plan.contains("NativeTableFunction")
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan);
              else assertEquals(0, scan.substitutions(), plan);
            } else assertFalse(plan.contains("NativeTableFunction"), plan);
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[table-function-trial] shape=%s engine=%s trial=%d warmup=%s rows=%d keys=%d"
                    + " width=%d start_ms=%d seconds=%.6f%n",
                shape,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                keys,
                width,
                startMillis,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            if (engine == 0 || !expectedNative)
              assertTrue(
                  metrics.nativeOperators().isEmpty(),
                  "fallback control executed native operators");
            if (engine == 1 && expectedNative)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeTableFunction");
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[table-function] shape=%s rows=%d keys=%d width=%d stock=%.6f native=%.6f"
              + " stock_trials=%s native_trials=%s%n",
          shape,
          rows,
          keys,
          width,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, int rows, int keys, int width, boolean numeric) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    if (numeric) {
      var source =
          env.fromSequence(0, Math.max(0, rows - 1))
              .filter(i -> i < rows)
              .map(i -> Row.of((int) (i % keys), i % 8 == 7 ? null : i))
              .returns(Types.ROW_NAMED(new String[] {"k", "n"}, Types.INT, Types.LONG));
      table.createTemporaryView("src", source);
      table.createTemporarySystemFunction("numeric_rows", new NumericRows());
    } else {
      String payload = "é" + "x".repeat(Math.max(0, width - 1));
      int textCardinality = Integer.getInteger("tablefunc.textCardinality", 1);
      var source =
          env.fromSequence(0, Math.max(0, rows - 1))
              .filter(i -> i < rows)
              .map(
                  i -> {
                    String value =
                        textCardinality == 1 ? payload : payload + "_" + i % textCardinality;
                    return Row.of((int) (i % keys), i % 8 == 7 ? null : value + "," + value);
                  })
              .returns(Types.ROW_NAMED(new String[] {"k", "text_value"}, Types.INT, Types.STRING));
      table.createTemporaryView("src", source);
      table.createTemporarySystemFunction("test_split", new PortableSqlFixtures.Split());
    }
    return table;
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = {
        @org.apache.flink.table.annotation.DataTypeHint("INT"),
        @org.apache.flink.table.annotation.DataTypeHint("BIGINT")
      },
      output = @org.apache.flink.table.annotation.DataTypeHint("ROW<n BIGINT, pos INT>"))
  public static class NumericRows extends org.apache.flink.table.functions.TableFunction<Row> {
    public void eval(Integer key, Long value) {
      if (key == null || value == null) return;
      collect(Row.of(value + key, 0));
      collect(Row.of(value + key + 1, 1));
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
