package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TypedTableFunctionBenchmark {
  @Test
  void completeJobsKeepTypedFanoutBothTransposesAndRowSink() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("tablefunc.rows", 2_000_000);
    int warmups = Integer.getInteger("tablefunc.warmup", 2);
    int runs = Integer.getInteger("tablefunc.runs", 5);
    boolean nativeExpected =
        Boolean.parseBoolean(System.getProperty("tablefunc.expectNative", "true"));
    for (String type :
        System.getProperty(
                "tablefunc.types",
                "BOOLEAN;TINYINT;SMALLINT;DECIMAL(18,8);DECIMAL(38,18);DATE;TIME;TIMESTAMP(9);TIMESTAMP_LTZ(9)")
            .split(";")) {
      for (String shape : System.getProperty("tablefunc.cases", "project,group").split(",")) {
        String sql;
        if (shape.equals("project")) {
          sql =
              "SELECT s.id, t.v, t.pos FROM src s, LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v,"
                  + " pos)";
        } else if (shape.equals("group")) {
          sql =
              "SELECT MOD(s.id, 64), COUNT(t.v), COUNT(*) FROM src s, "
                  + "LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos) GROUP BY MOD(s.id, 64)";
        } else throw new IllegalArgumentException("Unknown tablefunc.cases: " + shape);
        double[][] elapsed = new double[2][runs];
        for (int trial = 0; trial < warmups + runs; trial++) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            for (int order = 0; order < 2; order++) {
              int engine = (trial + order) & 1;
              var table =
                  FlinkTypedTableFunctionSqlHarnessTest.environment(type, "UTC", rows, metrics);
              table.getConfig().set("streamfusion.transpose.batchRows", "1024");
              table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
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
                if (nativeExpected) {
                  assertTrue(
                      plan.contains("NativeTableFunction")
                          && plan.contains("RowDataToArrow")
                          && plan.contains("ArrowToRowData"),
                      plan);
                } else assertEquals(0, scan.substitutions(), plan);
              }
              long start = System.nanoTime();
              table.executeSql(insert).await();
              double seconds = (System.nanoTime() - start) / 1e9;
              System.out.printf(
                  Locale.ROOT,
                  "[typed-table-function-trial] type=%s shape=%s engine=%s trial=%d warmup=%s"
                      + " rows=%d seconds=%.6f%n",
                  type,
                  shape,
                  engine == 0 ? "Flink" : "StreamFusion",
                  trial,
                  trial < warmups,
                  rows,
                  seconds);
              if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            }
            if (nativeExpected)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeTableFunction");
          }
        }
        System.out.printf(
            Locale.ROOT,
            "[typed-table-function] type=%s shape=%s rows=%d stock=%.6f native=%.6f stock_trials=%s"
                + " native_trials=%s%n",
            type,
            shape,
            rows,
            median(elapsed[0]),
            median(elapsed[1]),
            Arrays.toString(elapsed[0]),
            Arrays.toString(elapsed[1]));
      }
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
