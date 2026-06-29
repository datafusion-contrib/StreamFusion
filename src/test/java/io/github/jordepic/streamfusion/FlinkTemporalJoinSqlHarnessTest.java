package io.github.jordepic.streamfusion;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Event-time temporal table join: orders joined to the version of a currency-rates table valid at each
 * order's rowtime ({@code FOR SYSTEM_TIME AS OF o.rt}). The rates table is a versioned table built the
 * canonical way — a row-time deduplication (keep the latest row per currency) over an append-only
 * changelog. Each order matches the rate whose version period covers its time; the result is
 * deterministic (watermark-gated), so it is value-compared to the host.
 */
class FlinkTemporalJoinSqlHarnessTest {

  private static final String JOIN =
      "SELECT o.currency, o.amount, r.rate "
          + "FROM Orders AS o "
          + "JOIN Rates FOR SYSTEM_TIME AS OF o.rt AS r "
          + "ON o.currency = r.currency";

  @Test
  void temporalJoinMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkTemporalJoinSqlHarnessTest::environment, JOIN);
  }

  @Test
  void leftTemporalJoinMatchesHost() throws Exception {
    // A GBP order has no rate version at all → null-padded by the LEFT join.
    NativeParity.assertParity(
        FlinkTemporalJoinSqlHarnessTest::environment,
        "SELECT o.currency, o.amount, r.rate "
            + "FROM Orders AS o "
            + "LEFT JOIN Rates FOR SYSTEM_TIME AS OF o.rt AS r "
            + "ON o.currency = r.currency");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView("Orders", orders(env), schema("amount"));
    tEnv.createTemporaryView("RatesRaw", rates(env), schema("rate"));
    // The versioned rates table: the latest row per currency by rowtime (a row-time deduplication),
    // which Flink treats as a versioned table usable in FOR SYSTEM_TIME AS OF.
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW Rates AS SELECT currency, rate, rt FROM "
            + "(SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rt DESC) AS rn "
            + " FROM RatesRaw) WHERE rn = 1");
    return tEnv;
  }

  // Orders: USD@150, EUR@250, USD@450, GBP@260 (GBP has no rate).
  private static DataStream<Row> orders(StreamExecutionEnvironment env) {
    return env.fromData(
            Types.ROW_NAMED(
                new String[] {"currency", "amount", "ts"}, Types.STRING, Types.LONG, Types.LONG),
            Row.of("USD", 1L, 150L),
            Row.of("EUR", 2L, 250L),
            Row.of("GBP", 4L, 260L),
            Row.of("USD", 3L, 450L))
        .assignTimestampsAndWatermarks(watermarks());
  }

  // Rates versions: USD 10@100 then 20@300; EUR 99@100.
  private static DataStream<Row> rates(StreamExecutionEnvironment env) {
    return env.fromData(
            Types.ROW_NAMED(
                new String[] {"currency", "rate", "ts"}, Types.STRING, Types.LONG, Types.LONG),
            Row.of("USD", 10L, 100L),
            Row.of("EUR", 99L, 100L),
            Row.of("USD", 20L, 300L))
        .assignTimestampsAndWatermarks(watermarks());
  }

  private static WatermarkStrategy<Row> watermarks() {
    return WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((row, ts) -> (Long) row.getField(2));
  }

  private static Schema schema(String valueColumn) {
    return Schema.newBuilder()
        .column("currency", DataTypes.STRING())
        .column(valueColumn, DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()")
        .build();
  }
}
