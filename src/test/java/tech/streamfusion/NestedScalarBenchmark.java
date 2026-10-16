package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NestedScalarBenchmark {
  @Test
  void completeJobsKeepNestedConversionAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("nestedbench.rows", 2_000_000);
    int width = Integer.getInteger("nestedbench.width", 16);
    int warmups = Integer.getInteger("nestedbench.warmup", 2);
    int runs = Integer.getInteger("nestedbench.runs", 5);
    boolean expectedNative =
        Boolean.parseBoolean(System.getProperty("nestedbench.expectNative", "true"));
    for (String shape :
        System.getProperty("nestedbench.cases", "identity,transform,rust").split(",")) {
      assertTrue(Arrays.asList("identity", "transform", "rust").contains(shape));
      String sql =
          shape.equals("rust") ? "SELECT id, v + 7 FROM src" : "SELECT id, nested_fn(r) FROM src";
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int order = 0; order < 2; order++) {
          int engine = (trial + order) & 1;
          try (var metrics = new RecoveryExecutionMetrics()) {
            var table = environment(metrics, shape, rows, width);
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
            boolean nativeQuery = expectedNative || shape.equals("rust");
            if (engine == 1 && nativeQuery)
              assertTrue(
                  plan.contains("NativeCalc")
                      && plan.contains("RowDataToArrow")
                      && plan.contains("ArrowToRowData"),
                  plan);
            else if (engine == 1) assertEquals(0, scan.substitutions(), plan);
            else assertFalse(plan.contains("NativeCalc"), plan);
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[nested-scalar-trial] shape=%s engine=%s trial=%d warmup=%s rows=%d width=%d"
                    + " start_ms=%d seconds=%.6f%n",
                shape,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                width,
                startMillis,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            if (engine == 0 || !nativeQuery)
              assertTrue(
                  metrics.nativeOperators().isEmpty(),
                  "fallback control executed native operators");
            else RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[nested-scalar] shape=%s rows=%d width=%d stock=%.6f native=%.6f stock_trials=%s"
              + " native_trials=%s%n",
          shape,
          rows,
          width,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, String shape, int rows, int width) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var sequence = env.fromSequence(0, Math.max(0, rows - 1)).filter(i -> i < rows);
    if (shape.equals("rust")) {
      table.createTemporaryView(
          "src",
          sequence
              .map(i -> Row.of((int) (i % 1024), i % 8 == 7 ? null : i))
              .returns(Types.ROW_NAMED(new String[] {"id", "v"}, Types.INT, Types.LONG)));
    } else {
      String prefix = "é" + "x".repeat(Math.max(0, width - 1));
      var record =
          Types.ROW_NAMED(
              new String[] {"value", "tags"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING));
      table.createTemporaryView(
          "src",
          sequence
              .map(
                  i ->
                      Row.of(
                          (int) (i % 1024),
                          i % 8 == 7
                              ? null
                              : Row.ofKind(
                                  RowKind.values()[(int) (i % 4)],
                                  i % 5 == 0 ? null : i,
                                  i % 11 == 0
                                      ? null
                                      : new String[] {prefix + (i % 65536), null, "tail"})))
              .returns(Types.ROW_NAMED(new String[] {"id", "r"}, Types.INT, record)));
      table.createTemporarySystemFunction(
          "nested_fn",
          shape.equals("identity")
              ? new Identity()
              : new FlinkNestedScalarSqlHarnessTest.Reusable());
    }
    return table;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class Identity extends ScalarFunction {
    public Row eval(Row input) {
      return input;
    }
  }
}
