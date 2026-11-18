package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.LocalDateTime;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Nexmark q23's shape: a multi-way (three input) inner equi-join that chains {@code bid ⋈ person}
 * and {@code bid ⋈ auction}. StreamFusion already accelerates chained regular equi-joins, so q23
 * runs fully native. The only reason the canonical q23.sql does not plan in this Flink build is a
 * parser quirk — it references the reserved identifier {@code dateTime} bare ({@code A.dateTime});
 * quoting it as {@code A.`dateTime`} (the form the DDL declares) parses and accelerates
 * identically.
 */
class FlinkMultiwayJoinSqlHarnessTest {

  @ParameterizedTest
  @ValueSource(longs = {-1, 1, 4})
  void threeWayInnerJoinMatchesHost(long miniBatchSize) throws Exception {
    NativeParity.assertKindedParity(
        environment(miniBatchSize),
        "SELECT B.bidder, B.price, B.channel, P.id AS person_id, P.name,"
            + " A.itemName, A.`dateTime` AS auction_dateTime, A.seller"
            + " FROM bid B"
            + " JOIN person P ON P.id = B.bidder"
            + " JOIN auction A ON A.seller = B.bidder");
  }

  private static Supplier<TableEnvironment> environment(long miniBatchSize) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      if (miniBatchSize > 0) {
        tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
        tEnv.getConfig().set("table.exec.mini-batch.size", Long.toString(miniBatchSize));
      }

      DataStream<Row> bid =
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"bidder", "price", "channel"},
                  Types.LONG,
                  Types.LONG,
                  Types.STRING),
              Row.of(1L, 100L, "apple"),
              Row.of(1L, 100L, "apple"),
              Row.of(1L, 150L, "google"),
              Row.of(2L, 200L, "baidu"),
              Row.of(3L, 300L, "facebook"));
      DataStream<Row> person =
          fromData(
              env,
              Types.ROW_NAMED(new String[] {"id", "name"}, Types.LONG, Types.STRING),
              Row.of(1L, "alice"),
              Row.of(2L, "bob"));
      DataStream<Row> auction =
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"seller", "itemName", "dateTime"},
                  Types.LONG,
                  Types.STRING,
                  Types.LOCAL_DATE_TIME),
              Row.of(1L, "widget", LocalDateTime.of(2023, 1, 1, 12, 0, 0)),
              Row.of(1L, "widget", LocalDateTime.of(2023, 1, 1, 12, 0, 0)),
              Row.of(1L, "gadget", LocalDateTime.of(2023, 1, 2, 13, 30, 0)),
              Row.of(2L, "gizmo", LocalDateTime.of(2023, 1, 3, 9, 15, 0)));

      tEnv.createTemporaryView(
          "bid",
          bid,
          Schema.newBuilder()
              .column("bidder", DataTypes.BIGINT())
              .column("price", DataTypes.BIGINT())
              .column("channel", DataTypes.STRING())
              .build());
      tEnv.createTemporaryView(
          "person",
          person,
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .build());
      tEnv.createTemporaryView(
          "auction",
          auction,
          Schema.newBuilder()
              .column("seller", DataTypes.BIGINT())
              .column("itemName", DataTypes.STRING())
              .column("dateTime", DataTypes.TIMESTAMP(3))
              .build());
      return tEnv;
    };
  }
}
