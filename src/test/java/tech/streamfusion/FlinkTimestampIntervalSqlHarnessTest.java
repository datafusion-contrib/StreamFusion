package tech.streamfusion;

import java.time.LocalDateTime;
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
 * Day-time INTERVAL literals and {@code TIMESTAMP - INTERVAL} arithmetic — Nexmark q7's join residual
 * shape ({@code dateTime BETWEEN b.dateTime - INTERVAL '10' SECOND AND b.dateTime}). The native engine
 * encodes the interval literal as an Arrow IntervalDayTime so the subtraction yields a timestamp;
 * value-compared to the host both as a projection and as a regular-join non-equi predicate.
 */
class FlinkTimestampIntervalSqlHarnessTest {

  @Test
  void timestampMinusIntervalProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampIntervalSqlHarnessTest::environment,
        "SELECT id, tsa, tsa - INTERVAL '5' SECOND AS earlier FROM a");
  }

  @Test
  void regularJoinWithIntervalResidualMatchesHost() throws Exception {
    // Plain TIMESTAMP columns (no time attribute) → a regular join; the time-range residual is its
    // non-equi predicate, with a TIMESTAMP - INTERVAL subtraction inside it (the q7 pattern).
    NativeParity.assertParity(
        FlinkTimestampIntervalSqlHarnessTest::environment,
        "SELECT a.id, a.tsa, b.tsb FROM a JOIN b ON a.id = b.id "
            + "AND a.tsa BETWEEN b.tsb - INTERVAL '10' SECOND AND b.tsb");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "tsa"}, Types.LONG, Types.LOCAL_DATE_TIME),
            Row.of(1L, LocalDateTime.of(2024, 6, 1, 12, 0, 5)),
            Row.of(1L, LocalDateTime.of(2024, 6, 1, 12, 0, 30)),
            Row.of(2L, LocalDateTime.of(2024, 6, 1, 12, 0, 8)));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "tsb"}, Types.LONG, Types.LOCAL_DATE_TIME),
            Row.of(1L, LocalDateTime.of(2024, 6, 1, 12, 0, 12)),
            Row.of(2L, LocalDateTime.of(2024, 6, 1, 12, 0, 9)));
    tEnv.createTemporaryView(
        "a",
        a,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("tsa", DataTypes.TIMESTAMP(3))
            .build());
    tEnv.createTemporaryView(
        "b",
        b,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("tsb", DataTypes.TIMESTAMP(3))
            .build());
    return tEnv;
  }
}
