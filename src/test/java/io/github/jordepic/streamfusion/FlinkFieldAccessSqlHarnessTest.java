package io.github.jordepic.streamfusion;

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
 * Nested-ROW field access ({@code bid.price}, {@code bid.auction}) — the Nexmark schema shape: a wide
 * source with struct columns projected through a per-event-type view. The native expression engine
 * extracts the field with {@code get_field}; these assert it matches the host value-for-value,
 * including a null struct (field of a null ROW is NULL) and arithmetic/predicates over an extracted
 * field.
 */
class FlinkFieldAccessSqlHarnessTest {

  @Test
  void projectsExtractedFields() throws Exception {
    // q0-shaped: pure projection of fields pulled out of the bid struct.
    NativeParity.assertParity(
        FlinkFieldAccessSqlHarnessTest::environment,
        "SELECT auction, bidder, price, channel FROM bidv");
  }

  @Test
  void filtersOnExtractedField() throws Exception {
    // q2-shaped: a predicate over an extracted field.
    NativeParity.assertParity(
        FlinkFieldAccessSqlHarnessTest::environment,
        "SELECT auction, price FROM bidv WHERE MOD(auction, 2) = 0");
  }

  @Test
  void computesOverExtractedFields() throws Exception {
    NativeParity.assertParity(
        FlinkFieldAccessSqlHarnessTest::environment,
        "SELECT auction, price + bidder AS s, "
            + "CASE WHEN price > 50 THEN 'hi' ELSE 'lo' END AS band FROM bidv");
  }

  @Test
  void fieldOfNullStructIsNull() throws Exception {
    // A row whose bid struct is NULL: every extracted field is NULL, exactly as the host computes it.
    NativeParity.assertParity(
        FlinkFieldAccessSqlHarnessTest::environment,
        "SELECT event_type, bid.price AS price FROM events");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"event_type", "bid"},
                Types.INT,
                Types.ROW_NAMED(
                    new String[] {"auction", "bidder", "price", "channel"},
                    Types.LONG,
                    Types.LONG,
                    Types.LONG,
                    Types.STRING)),
            Row.of(2, Row.of(100L, 5L, 99L, "web")),
            Row.of(2, Row.of(101L, 6L, 40L, "app")),
            Row.of(2, Row.of(102L, 7L, 200L, "web")),
            Row.of(0, null),
            Row.of(2, null));
    tEnv.createTemporaryView(
        "events",
        source,
        Schema.newBuilder()
            .column("event_type", DataTypes.INT())
            .column(
                "bid",
                DataTypes.ROW(
                    DataTypes.FIELD("auction", DataTypes.BIGINT()),
                    DataTypes.FIELD("bidder", DataTypes.BIGINT()),
                    DataTypes.FIELD("price", DataTypes.BIGINT()),
                    DataTypes.FIELD("channel", DataTypes.STRING())))
            .build());
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bidv AS SELECT bid.auction AS auction, bid.bidder AS bidder, "
            + "bid.price AS price, bid.channel AS channel FROM events WHERE event_type = 2");
    return tEnv;
  }
}
