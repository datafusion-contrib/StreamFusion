package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
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
 * Event-time INNER window join: two {@code TUMBLE} windowing-TVF inputs joined on their key within
 * the same window. Rows of the same key whose windows coincide join when the watermark closes the
 * window; rows in different windows or with non-matching keys do not. The watermark lags past the
 * data, so windows only close at end-of-input and the result is the full set of matches.
 */
class FlinkWindowJoinSqlHarnessTest {

  private static final String JOIN =
      "SELECT a.k, a.v, b.v FROM "
          + "(SELECT * FROM TABLE(TUMBLE(TABLE A, DESCRIPTOR(rt), INTERVAL '1' SECOND))) a "
          + "JOIN "
          + "(SELECT * FROM TABLE(TUMBLE(TABLE B, DESCRIPTOR(rt), INTERVAL '1' SECOND))) b "
          + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end";

  @Test
  void windowJoinMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkWindowJoinSqlHarnessTest::dataStreamEnvironment, JOIN);
  }

  @Test
  void windowJoinWithNonEquiPredicateMatchesHost() throws Exception {
    // A residual non-equi condition (a.v < b.v) beyond the window-bound equi keys is applied natively
    // as the join filter; the matches must equal the host's.
    NativeParity.assertParity(
        FlinkWindowJoinSqlHarnessTest::dataStreamEnvironment,
        "SELECT a.k, a.v, b.v FROM "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE A, DESCRIPTOR(rt), INTERVAL '1' SECOND))) a "
            + "JOIN "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE B, DESCRIPTOR(rt), INTERVAL '1' SECOND))) b "
            + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end "
            + "AND a.v < b.v");
  }

  @Test
  void windowJoinOverParquetMatchesHost() throws Exception {
    Path left = Files.createTempDirectory("wjoin-left");
    Path right = Files.createTempDirectory("wjoin-right");
    writeRows(left);
    writeRows(right);
    NativeParity.assertParity(() -> parquetEnvironment(left, right), JOIN);
  }

  @Test
  void leftWindowJoinFallsBackToHost() throws Exception {
    // A LEFT outer window join is append-only (so it reaches the matcher) but the native operator
    // only does INNER — it must fall back cleanly. SELECT * so no projection Calc routes either.
    NativeParity.assertFallback(
        FlinkWindowJoinSqlHarnessTest::dataStreamEnvironment,
        "SELECT * FROM "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE A, DESCRIPTOR(rt), INTERVAL '1' SECOND))) a "
            + "LEFT JOIN "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE B, DESCRIPTOR(rt), INTERVAL '1' SECOND))) b "
            + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end");
  }

  private static TableEnvironment dataStreamEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView("A", stream(env), schema());
    tEnv.createTemporaryView("B", stream(env), schema());
    return tEnv;
  }

  private static DataStream<Row> stream(StreamExecutionEnvironment env) {
    return env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
            Row.of(1L, 10L, 100L),
            Row.of(1L, 20L, 1500L),
            Row.of(2L, 30L, 200L),
            Row.of(1L, 40L, 1700L),
            Row.of(3L, 50L, 400L))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("k", DataTypes.BIGINT())
        .column("v", DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()")
        .build();
  }

  private static void writeRows(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES "
                + "(1, 10, TO_TIMESTAMP_LTZ(100, 3)), "
                + "(1, 20, TO_TIMESTAMP_LTZ(1500, 3)), "
                + "(2, 30, TO_TIMESTAMP_LTZ(200, 3)), "
                + "(1, 40, TO_TIMESTAMP_LTZ(1700, 3)), "
                + "(3, 50, TO_TIMESTAMP_LTZ(400, 3))")
        .await();
  }

  private static TableEnvironment parquetEnvironment(Path left, Path right) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    createParquetTable(tEnv, "A", left);
    createParquetTable(tEnv, "B", right);
    return tEnv;
  }

  private static void createParquetTable(StreamTableEnvironment tEnv, String name, Path directory) {
    tEnv.executeSql(
        "CREATE TABLE "
            + name
            + " (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), WATERMARK FOR rt AS rt - INTERVAL '5' SECOND) "
            + "WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
  }
}
