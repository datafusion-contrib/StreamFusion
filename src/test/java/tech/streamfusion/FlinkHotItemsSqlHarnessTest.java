package tech.streamfusion;

import java.time.Duration;
import java.time.ZoneId;
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
 * Nexmark q5 (Hot Items): the auctions with the most bids per sliding window. It exercises the
 * window-attached window aggregate — a two-phase {@code MAX} re-aggregation over the per-auction counts,
 * grouped by the already-assigned window (no rowtime to slice) — and a windows-only window join (no user
 * equi-key beyond the shared window bounds, with a {@code num >= maxn} residual). Both window aggregates
 * over event time and the INNER join are append-only, so the native result must equal the host's.
 */
class FlinkHotItemsSqlHarnessTest {

  private static final String HOT_ITEMS =
      "SELECT AuctionBids.auction, AuctionBids.num FROM ("
          + "  SELECT auction, count(*) AS num, window_start AS starttime, window_end AS endtime"
          + "  FROM TABLE(HOP(TABLE bid, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND))"
          + "  GROUP BY auction, window_start, window_end"
          + ") AS AuctionBids "
          + "JOIN ("
          + "  SELECT max(CountBids.num) AS maxn, CountBids.starttime, CountBids.endtime FROM ("
          + "    SELECT auction, count(*) AS num, window_start AS starttime, window_end AS endtime"
          + "    FROM TABLE(HOP(TABLE bid, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND))"
          + "    GROUP BY auction, window_start, window_end"
          + "  ) AS CountBids GROUP BY CountBids.starttime, CountBids.endtime"
          + ") AS MaxBids "
          + "ON AuctionBids.starttime = MaxBids.starttime"
          + "  AND AuctionBids.endtime = MaxBids.endtime"
          + "  AND AuctionBids.num >= MaxBids.maxn";

  @Test
  void hotItemsMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkHotItemsSqlHarnessTest::bidEnvironment, HOT_ITEMS);
  }

  private static TableEnvironment bidEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.createTemporaryView("bid", bids(env), schema());
    return tEnv;
  }

  private static DataStream<Row> bids(StreamExecutionEnvironment env) {
    // Bids for auctions 1/2/3 spread across time so several overlapping 10s/2s windows form; auction 1
    // is the hot item in the early windows, auction 2 later — the per-window MAX picks the leaders.
    return env.fromData(
            Types.ROW_NAMED(new String[] {"auction", "ts"}, Types.LONG, Types.LONG),
            Row.of(1L, 1000L),
            Row.of(1L, 1500L),
            Row.of(1L, 2200L),
            Row.of(2L, 1200L),
            Row.of(3L, 1800L),
            Row.of(2L, 5200L),
            Row.of(2L, 5600L),
            Row.of(2L, 6000L),
            Row.of(1L, 5400L))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("auction", DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()")
        .build();
  }
}
