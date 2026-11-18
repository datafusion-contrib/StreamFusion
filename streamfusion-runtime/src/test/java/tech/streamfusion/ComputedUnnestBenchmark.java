package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ComputedUnnestBenchmark {
  @Test
  void completeJobsRetainComputedCollectionAndFanoutCosts() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    long rows = Long.getLong("unnest.rows", 2_000_000L);
    int width = Integer.getInteger("unnest.width", 8);
    int keys = Integer.getInteger("unnest.keys", 4096);
    int warmups = Integer.getInteger("unnest.warmup", 2);
    int runs = Integer.getInteger("unnest.runs", 3);
    boolean left = Boolean.getBoolean("unnest.left");
    boolean grouped = Boolean.getBoolean("unnest.grouped");
    boolean updating = Boolean.getBoolean("unnest.updating");
    boolean allKinds = Boolean.getBoolean("unnest.allKinds");
    boolean standaloneNative = Boolean.getBoolean("unnest.standaloneNative");
    assertTrue(!allKinds || updating, "allKinds requires an updating source");
    boolean expectNative = Boolean.parseBoolean(System.getProperty("unnest.expectNative", "true"));
    String operand = System.getProperty("unnest.operand", "append");
    String expression;
    switch (operand) {
      case "identity":
        expression = "vs";
        break;
      case "append":
        expression = "ARRAY_APPEND(vs, id)";
        break;
      case "subscript":
        expression = "ARRAY[vs[1], id]";
        break;
      case "literal":
        expression = "ARRAY[id, id + 1]";
        break;
      case "tinyint":
      case "smallint":
      case "integer":
      case "float":
      case "double":
        String scalarType = operand.toUpperCase(Locale.ROOT);
        expression = "ARRAY[CAST(vs[1] AS " + scalarType + "), CAST(id AS " + scalarType + ")]";
        break;
      case "boolean":
        expression = "ARRAY[id > 3, vs[1] IS NULL]";
        break;
      case "decimal":
        expression = "ARRAY[CAST(id AS DECIMAL(20,3)), CAST(vs[1] AS DECIMAL(20,3))]";
        break;
      case "timestamp":
        expression = "ARRAY[TO_TIMESTAMP_LTZ(id, 3), TO_TIMESTAMP_LTZ(id + 1, 3)]";
        break;
      case "date":
        expression =
            "ARRAY[CAST(TO_TIMESTAMP_LTZ(id, 3) AS DATE), CAST(TO_TIMESTAMP_LTZ(id + 1, 3) AS"
                + " DATE)]";
        break;
      case "time":
        expression =
            "ARRAY[CAST(TO_TIMESTAMP_LTZ(id, 3) AS TIME(0)), CAST(TO_TIMESTAMP_LTZ(id + 1, 3) AS"
                + " TIME(0))]";
        break;
      case "timestamp_days":
        expression =
            "ARRAY[TO_TIMESTAMP_LTZ(id * 86400000, 3), TO_TIMESTAMP_LTZ((id + 1) * 86400000, 3)]";
        break;
      case "date_days":
        expression =
            "ARRAY[CAST(TO_TIMESTAMP_LTZ(id * 86400000, 3) AS DATE), CAST(TO_TIMESTAMP_LTZ((id + 1)"
                + " * 86400000, 3) AS DATE)]";
        break;
      case "time_days":
        expression =
            "ARRAY[CAST(TO_TIMESTAMP_LTZ(id * 86400000, 3) AS TIME(0)), CAST(TO_TIMESTAMP_LTZ((id +"
                + " 1) * 86400000, 3) AS TIME(0))]";
        break;
      default:
        throw new IllegalArgumentException("Unknown operand " + operand);
    }
    assertTrue(rows > 0 && width > 0 && keys > 0 && warmups >= 0 && runs > 0);
    boolean identity = operand.equals("identity");
    String sql =
        identity
            ? "SELECT id, vs FROM src"
            : "SELECT "
                + (grouped ? "e, COUNT(*)" : "id, e")
                + " FROM src "
                + (left ? "LEFT JOIN " : "CROSS JOIN ")
                + "UNNEST("
                + expression
                + ") AS u(e)"
                + (left ? " ON TRUE" : "")
                + (grouped ? " GROUP BY e" : "");
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 1, 0);
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          if (standaloneNative)
            table.getConfig().set("streamfusion.dropUpdateBefore.standalone.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.enabled", "false");
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        boolean retract = updating && i / keys % 3 == 2;
                        long original = allKinds ? i / 4 : retract ? i - keys : i;
                        Long[] values =
                            original % 11 == 0 ? null : new Long[original % 13 == 0 ? 0 : width];
                        if (values != null) {
                          for (int j = 0; j < values.length; j++) {
                            values[j] = (original + j) % 7 == 0 ? null : (original + j) % keys;
                          }
                        }
                        return Row.ofKind(
                            allKinds
                                ? org.apache.flink.types.RowKind.fromByteValue((byte) (i % 4))
                                : retract
                                    ? org.apache.flink.types.RowKind.DELETE
                                    : org.apache.flink.types.RowKind.INSERT,
                            original % keys,
                            values);
                      })
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)));
          var input =
              updating
                  ? table.fromChangelogStream(
                      source,
                      Schema.newBuilder().build(),
                      org.apache.flink.table.connector.ChangelogMode.all())
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
                (identity
                        ? plan.contains("NativeDropUpdateBefore")
                        : plan.contains("NativeUnnest") && plan.contains("NativeCalc"))
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          } else if (engine == 1) {
            assertEquals(0, scan.substitutions(), plan);
          }
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expectNative) {
            if (!identity) {
              RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeUnnest");
              RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
            }
            if (updating && !grouped)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeDropUpdateBefore");
            if (grouped)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeColumnarGroupAggregate");
          }
          System.out.printf(
              Locale.ROOT,
              "[computed-unnest-trial] operand=%s left=%s grouped=%s updating=%s allKinds=%s"
                  + " rows=%d keys=%d width=%d engine=%s trial=%d warmup=%s seconds=%.9f%n",
              operand,
              left,
              grouped,
              updating,
              allKinds,
              rows,
              keys,
              width,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              elapsed);
          if (trial >= warmups) times[engine][trial - warmups] = elapsed;
        }
      }
    }
    double[] stock = times[0].clone(), nativeTimes = times[1].clone();
    Arrays.sort(stock);
    Arrays.sort(nativeTimes);
    System.out.printf(
        Locale.ROOT,
        "[computed-unnest] operand=%s left=%s grouped=%s updating=%s allKinds=%s rows=%d keys=%d"
            + " width=%d stock=%.9f native=%.9f stock_trials=%s native_trials=%s%n",
        operand,
        left,
        grouped,
        updating,
        allKinds,
        rows,
        keys,
        width,
        stock[runs / 2],
        nativeTimes[runs / 2],
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
