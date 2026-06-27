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
 * Event-time INNER interval join
 * ({@code a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt - X AND b.rt + Y}) matches the host: a row of
 * each side joins the other side's rows of the same key whose rowtime falls in the interval. The
 * watermark lags well past the data, so no row is dropped as late and the result is the full set of
 * matched pairs (the watermark only governs state eviction — divergences/09).
 */
class FlinkIntervalJoinSqlHarnessTest {

  private static final String JOIN =
      "SELECT a.k, a.v, b.v FROM A AS a JOIN B AS b "
          + "ON a.k = b.k "
          + "AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND AND b.rt + INTERVAL '1' SECOND";

  @Test
  void intervalJoinMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment, JOIN);
  }

  @Test
  void intervalJoinWithNonEquiPredicateMatchesHost() throws Exception {
    // A residual non-equi condition (a.v < b.v) beyond the interval bounds is now applied natively,
    // ANDed into the join filter; the pairs must match the host.
    NativeParity.assertParity(
        FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment,
        "SELECT a.k, a.v, b.v FROM A AS a JOIN B AS b "
            + "ON a.k = b.k "
            + "AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND AND b.rt + INTERVAL '1' SECOND "
            + "AND a.v < b.v");
  }

  @Test
  void dateKeyIntervalJoinMatchesHost() throws Exception {
    // Join on a DATE key — exercises the wider equi-key set (boolean/date/timestamp/decimal now
    // admitted, not just bigint/int/string); the native key path is type-general.
    NativeParity.assertParity(
        FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment,
        "SELECT a.k, a.v, b.v FROM A AS a JOIN B AS b "
            + "ON a.d = b.d "
            + "AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND AND b.rt + INTERVAL '1' SECOND");
  }

  @Test
  void leftIntervalJoinMatchesHost() throws Exception {
    // A LEFT outer interval join is append-only: an unmatched left row is null-padded once the
    // watermark closes its interval (emitted once, never retracted), so the result set matches the
    // host. b.v is null for left rows with no in-interval right match.
    NativeParity.assertParity(
        FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment, outerJoin("LEFT"));
  }

  @Test
  void rightIntervalJoinMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment, outerJoin("RIGHT"));
  }

  @Test
  void fullIntervalJoinMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkIntervalJoinSqlHarnessTest::dataStreamEnvironment, outerJoin("FULL"));
  }

  private static String outerJoin(String side) {
    return "SELECT a.k, a.v, b.v FROM A AS a "
        + side
        + " JOIN B AS b ON a.k = b.k "
        + "AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND AND b.rt + INTERVAL '1' SECOND";
  }

  @Test
  void columnarIntervalJoinOverParquetMatchesHost() throws Exception {
    Path left = Files.createTempDirectory("ijoin-left");
    Path right = Files.createTempDirectory("ijoin-right");
    writeRows(left);
    writeRows(right);
    // Fully columnar: each native Parquet source feeds a native watermark assigner and a native
    // columnar exchange (keyed by the join key), and the native interval join emits Arrow pairs —
    // no row transpose anywhere.
    NativeParity.assertParity(() -> parquetEnvironment(left, right), JOIN);
  }

  @Test
  void columnarIntervalJoinAtParallelismTwoMatchesHost() throws Exception {
    Path left = Files.createTempDirectory("ijoin-p2-left");
    Path right = Files.createTempDirectory("ijoin-p2-right");
    writeManyRows(left);
    writeManyRows(right);
    // Parallelism 2: both inputs shuffle through their own columnar exchange, and the same join key
    // must co-locate on the same channel across *both* sides for the matches to be found — the
    // cross-input co-location of divergences/10. Eight keys spread across both channels.
    NativeParity.assertParity(() -> parquetEnvironment(left, right, 2), JOIN);
  }

  /** Two DataStream sources, each with a 5s-bounded watermark (lagging past the data span). */
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
            Types.ROW_NAMED(
                new String[] {"k", "v", "ts", "d"},
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.LOCAL_DATE),
            Row.of(1L, 10L, 1000L, java.time.LocalDate.of(2020, 1, 1)),
            Row.of(1L, 20L, 3000L, java.time.LocalDate.of(2020, 1, 2)),
            Row.of(2L, 30L, 1000L, java.time.LocalDate.of(2020, 1, 1)),
            Row.of(1L, 40L, 1500L, java.time.LocalDate.of(2020, 1, 1)),
            Row.of(2L, 50L, 1500L, java.time.LocalDate.of(2020, 1, 2)),
            Row.of(3L, 60L, 2000L, java.time.LocalDate.of(2020, 1, 1)))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("k", DataTypes.BIGINT())
        .column("v", DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT())
        .column("d", DataTypes.DATE())
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
                + "(1, 10, TO_TIMESTAMP_LTZ(1000, 3)), "
                + "(1, 20, TO_TIMESTAMP_LTZ(3000, 3)), "
                + "(2, 30, TO_TIMESTAMP_LTZ(1000, 3)), "
                + "(1, 40, TO_TIMESTAMP_LTZ(1500, 3)), "
                + "(2, 50, TO_TIMESTAMP_LTZ(1500, 3)), "
                + "(3, 60, TO_TIMESTAMP_LTZ(2000, 3))")
        .await();
  }

  /** Writes 8 keys × 3 rowtimes (parallel, multiple files) so the sharded read has work per subtask. */
  private static void writeManyRows(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromSequence(0, 23)
            .map(
                i ->
                    Row.of(
                        i % 8,
                        i,
                        java.time.Instant.ofEpochMilli((i / 8) * 1000L + (i % 8) * 100L)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "rt"}, Types.LONG, Types.LONG, Types.INSTANT));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .build());
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s").await();
  }

  private static TableEnvironment parquetEnvironment(Path left, Path right) {
    return parquetEnvironment(left, right, 1);
  }

  private static TableEnvironment parquetEnvironment(Path left, Path right, int parallelism) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
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
