package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimestampRangeParityTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "0001-01-01T00:00:00.123456789",
        "1582-10-15T23:59:59.999999999",
        "1969-12-31T23:59:59.999999999",
        "2262-04-12T00:00:00.123456789",
        "9999-01-01T00:00:00.999999999"
      })
  void fullRangePassThrough(String value) throws Exception {
    NativeParity.assertParity(() -> timestamps(value),
        "SELECT id + 1, ts, ltz FROM n");
  }

  @Test
  void futureTimestampWindow() throws Exception {
    NativeParity.assertParity(() -> timestamps("2262-04-12T00:00:00"),
        "SELECT window_start, window_end, COUNT(*) FROM "
            + "TABLE(TUMBLE(TABLE n, DESCRIPTOR(ts), INTERVAL '1' DAY)) "
            + "GROUP BY window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0001-01-01T00:00:00.123456789", "1582-10-15T23:59:59.999999999",
      "2262-04-12T00:00:00.123456789", "9999-01-01T00:00:00.999999999"})
  void wideWindowGroupingKeys(String value) throws Exception {
    NativeParity.assertParity(() -> timestamps(value),
        "SELECT ts, window_start, window_end, COUNT(*) FROM "
            + "TABLE(TUMBLE(TABLE n, DESCRIPTOR(ts), INTERVAL '1' DAY)) "
            + "GROUP BY ts, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0001-01-01T00:00:00.123456789", "2262-04-12T00:00:00.123456789",
      "9999-01-01T00:00:00.999999999"})
  void wideTemporalExpressions(String value) throws Exception {
    NativeParity.assertParity(() -> timestamps(value),
        "SELECT CAST(ts AS STRING), ts + INTERVAL '1' SECOND, FLOOR(ts TO SECOND), "
            + "CEIL(ts TO MILLISECOND), ts < TIMESTAMP '9999-12-31 00:00:00', "
            + "EXTRACT(YEAR FROM ts), EXTRACT(QUARTER FROM ts) FROM n");
  }

  @Test
  void timestampDistinct() throws Exception {
    NativeParity.assertParity(() -> timestamps("9999-01-01T00:00:00.123456789"),
        "SELECT COUNT(DISTINCT ts) FROM n");
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MIN_VALUE, Long.MAX_VALUE})
  void roundingAtMillisecondLimitsMatchesFlinkOverflow(long millis) throws Exception {
    String value = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(millis).plusNanos(999_999), java.time.ZoneOffset.UTC).toString();
    NativeParity.assertParity(() -> timestamps(value),
        "SELECT FLOOR(ts TO DAY), CEIL(ts TO DAY), FLOOR(ts TO SECOND), "
            + "CEIL(ts TO SECOND), CEIL(ts TO MILLISECOND) FROM n");
  }

  private static TableEnvironment timestamps(String value) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "ts", "ltz"}, Types.INT, Types.LOCAL_DATE_TIME, Types.INSTANT),
            Row.of(1, LocalDateTime.parse(value), Instant.parse(value + "Z"))),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .column("ltz", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("ts", "ts - INTERVAL '1' SECOND")
            .build());
    return table;
  }
}
