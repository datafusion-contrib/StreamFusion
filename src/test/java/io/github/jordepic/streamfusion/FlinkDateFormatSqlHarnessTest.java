package io.github.jordepic.streamfusion;

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
 * {@code DATE_FORMAT} — Nexmark q10/q15/q16/q17's {@code DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')} /
 * {@code 'HH:mm'}. The native UDF formats the timestamp's UTC wall-clock with a chrono pattern the
 * encoder translated from the Java pattern; value-compared to the host. An unsupported pattern (a text
 * field) must fall back cleanly.
 */
class FlinkDateFormatSqlHarnessTest {

  @Test
  void dateFormatMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkDateFormatSqlHarnessTest::environment,
        "SELECT id, DATE_FORMAT(ts, 'yyyy-MM-dd') AS d, DATE_FORMAT(ts, 'HH:mm') AS hm FROM t");
  }

  @Test
  void dateFormatTimestampPatternMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkDateFormatSqlHarnessTest::environment,
        "SELECT id, DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') AS dt FROM t");
  }

  @Test
  void unsupportedPatternFallsBack() throws Exception {
    // A text day-of-week field ('EEE') has no byte-identical chrono mapping → the Calc falls back.
    NativeParity.assertFallbackReasonContains(
        FlinkDateFormatSqlHarnessTest::environment,
        "SELECT id, DATE_FORMAT(ts, 'EEE yyyy') AS d FROM t",
        "DATE_FORMAT: unsupported format pattern");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME),
            Row.of(1L, LocalDateTime.of(2024, 3, 5, 8, 30, 15)),
            Row.of(2L, LocalDateTime.of(2024, 12, 31, 23, 59, 59)),
            Row.of(3L, (LocalDateTime) null));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .build());
    return tEnv;
  }
}
