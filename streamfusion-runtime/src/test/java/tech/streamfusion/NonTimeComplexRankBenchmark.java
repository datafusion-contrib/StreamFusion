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
class NonTimeComplexRankBenchmark {
  @Test
  void completeJobsRetainAllRanksAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String type :
        System.getProperty("rank.orderTypes", System.getProperty("rank.orderType", "array,row"))
            .split(";")) {
      for (String shape : System.getProperty("rank.cases", "ascending,descending").split(",")) {
        for (String nulls : System.getProperty("rank.nulls", "FIRST,LAST").split(","))
          measure(shape, type, nulls);
      }
    }
  }

  private static void measure(String shape, String orderType, String nulls) throws Exception {
    assertTrue(shape.equals("ascending") || shape.equals("descending"));
    int rows = Integer.getInteger("rank.rows", 65536);
    int keys = Integer.getInteger("rank.keys", 1024);
    int nullEvery = Integer.getInteger("rank.nullEvery", 31);
    int warmups = Integer.getInteger("rank.warmup", 2);
    int runs = Integer.getInteger("rank.runs", 3);
    boolean nativeExpected = Boolean.parseBoolean(System.getProperty("rank.expectNative", "true"));
    assertTrue(nulls.equals("FIRST") || nulls.equals("LAST"));
    org.apache.flink.table.types.DataType payloadType;
    switch (orderType) {
      case "array":
        payloadType = DataTypes.ARRAY(DataTypes.BIGINT());
        break;
      case "row":
        payloadType =
            DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.BIGINT()), DataTypes.FIELD("b", DataTypes.STRING()));
        break;
      case "nested_array":
        payloadType = DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()));
        break;
      case "row_array":
        payloadType =
            DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.BIGINT())),
                DataTypes.FIELD("b", DataTypes.BIGINT()));
        break;
      case "BIGINT":
        payloadType = DataTypes.BIGINT();
        break;
      default:
        throw new IllegalArgumentException(orderType);
    }
    String sql =
        "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS "
            + nulls
            + ") AS rn FROM src";
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          int engine = (trial + turn) % 2;
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
          var type =
              DataTypes.ROW(
                  DataTypes.FIELD("k", DataTypes.BIGINT()),
                  DataTypes.FIELD("v", payloadType),
                  DataTypes.FIELD("id", DataTypes.BIGINT().notNull()));
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        Long n = (shape.equals("descending") ? rows - i : i) / keys;
                        Object payload;
                        switch (orderType) {
                          case "array":
                            payload = new Long[] {n, null};
                            break;
                          case "row":
                            payload = Row.of(n, (Object) null);
                            break;
                          case "nested_array":
                            payload = new Long[][] {{n, null}, null};
                            break;
                          case "row_array":
                            payload = Row.of(new Long[] {n, null}, n);
                            break;
                          case "BIGINT":
                            payload = n;
                            break;
                          default:
                            throw new IllegalArgumentException(orderType);
                        }
                        return Row.of(
                            i % keys, nullEvery > 0 && i % nullEvery == 0 ? null : payload, i);
                      })
                  .returns(ExternalTypeInfo.<Row>of(type));
          table.createTemporaryView(
              "src",
              table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
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
              "[complex-rank-trial] case=%s engine=%s trial=%d warmup=%s rows=%d keys=%d"
                  + " nullEvery=%d type=%s nulls=%s seconds=%.6f%n",
              shape,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              rows,
              keys,
              nullEvery,
              orderType,
              nulls,
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
        "[complex-rank] case=%s type=%s nulls=%s rows=%d keys=%d Flink=%.6fs StreamFusion=%.6fs"
            + " stock_trials=%s sf_trials=%s%n",
        shape,
        orderType,
        nulls,
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
