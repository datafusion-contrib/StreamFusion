package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
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
class NonTimeRowNumberBenchmark {
  @Test
  void completeJobsRetainAllRanksAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String type : System.getProperty("rank.orderTypes",
        System.getProperty("rank.orderType", "BIGINT")).split(";")) {
      for (String shape : System.getProperty("rank.cases", "ascending,descending").split(",")) {
        measure(shape, type);
      }
    }
  }

  private static void measure(String shape, String orderType) throws Exception {
    assertTrue(shape.equals("ascending") || shape.equals("descending"));
    boolean updating = Boolean.getBoolean("rank.updating");
    int rows = Integer.getInteger("rank.rows", 131072);
    int keys = Integer.getInteger("rank.keys", 1024);
    int nullEvery = Integer.getInteger("rank.nullEvery", 31);
    int warmups = Integer.getInteger("rank.warmup", 2);
    int runs = Integer.getInteger("rank.runs", 5);
    boolean nativeExpected = Boolean.parseBoolean(System.getProperty("rank.expectNative", "true"));
    String order;
    switch (orderType) {
      case "BIGINT": order = "v"; break;
      case "INT":
      case "TINYINT":
      case "SMALLINT":
      case "DECIMAL(20,2)":
      case "CHAR(8)":
      case "STRING": order = "CAST(v AS " + orderType + ")"; break;
      case "BINARY":
        order = "CAST(CAST(v AS STRING) AS BYTES)";
        break;
      case "MONTH":
        order = "CAST(v AS INT) * INTERVAL '1' MONTH";
        break;
      case "MILLIS":
        order = "v * INTERVAL '0.001' SECOND";
        break;
      case "BOOLEAN": order = "MOD(v, 2) = 0"; break;
      case "DATE": order = "CAST(TO_TIMESTAMP_LTZ(v * 86400000, 3) AS DATE)"; break;
      case "TIME": order = "CAST(TO_TIMESTAMP_LTZ(v, 3) AS TIME(3))"; break;
      case "TIMESTAMP(9)": order = "CAST(TO_TIMESTAMP_LTZ(v, 3) AS TIMESTAMP(9))"; break;
      case "TIMESTAMP_LTZ(9)": order = "CAST(TO_TIMESTAMP_LTZ(v, 3) AS TIMESTAMP_LTZ(9))"; break;
      default: throw new IllegalArgumentException("Unknown rank.orderType " + orderType);
    }
    String sql =
        "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY "
            + order
            + " ASC NULLS FIRST) AS rn FROM src";
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          int engine = (trial + turn) % 2;
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
          table.getConfig().set("table.exec.state.ttl", System.getProperty("rank.ttl", "0 ms"));
          var type =
              DataTypes.ROW(
                  DataTypes.FIELD("k", DataTypes.BIGINT()),
                  DataTypes.FIELD("v", DataTypes.BIGINT()),
                  DataTypes.FIELD("id", DataTypes.BIGINT().notNull()));
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        long identity = updating ? i / 3 : i;
                        long value =
                            (shape.equals("descending") ? rows - identity : identity) / keys;
                        if (updating && i % 3 == 2) value++;
                        return Row.ofKind(
                            !updating || i % 3 == 0
                                ? org.apache.flink.types.RowKind.INSERT
                                : i % 3 == 1
                                    ? org.apache.flink.types.RowKind.UPDATE_BEFORE
                                    : org.apache.flink.types.RowKind.UPDATE_AFTER,
                            identity % keys,
                            nullEvery > 0 && identity % nullEvery == 0 ? null : Long.valueOf(value),
                            identity);
                      })
                  .returns(ExternalTypeInfo.<Row>of(type));
          table.createTemporaryView(
              "src",
              updating
                  ? table.fromChangelogStream(
                      source, Schema.newBuilder().fromRowDataType(type).build())
                  : table.fromDataStream(
                      source, Schema.newBuilder().fromRowDataType(type).build()));
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && nativeExpected) {
            assertTrue(
                plan.contains("NativeColumnarTopN")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          } else if (engine == 1) {
            assertFalse(plan.contains("NativeColumnarTopN"), plan);
          }
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[non-time-rank-trial] case=%s engine=%s trial=%d warmup=%s rows=%d keys=%d"
                  + " nullEvery=%d type=%s updating=%s seconds=%.6f%n",
              shape,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              rows,
              keys,
              nullEvery,
              orderType,
              updating,
              seconds);
          if (trial >= warmups) times[engine][trial - warmups] = seconds;
          if (engine == 1 && nativeExpected)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeColumnarTopN");
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[non-time-rank] case=%s rows=%d keys=%d Flink=%.6fs StreamFusion=%.6fs stock_trials=%s"
            + " sf_trials=%s%n",
        shape,
        rows,
        keys,
        median(times[0]),
        median(times[1]),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
