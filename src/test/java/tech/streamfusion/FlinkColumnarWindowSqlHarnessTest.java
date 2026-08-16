package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Flink's Parquet source transposes once into a native watermark assigner, columnar exchange, and
 * columnar window. Results must match the host. The rowtime is {@code TIMESTAMP_LTZ}
 * (what the window matcher admits), and the watermark delay keeps every window open until
 * end-of-input MAX so per-batch watermark assignment (divergences/09) does not affect the result.
 */
class FlinkColumnarWindowSqlHarnessTest {

  @Test
  void globalWindowOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-global-in");
    writeInput(input);
    // No grouping key: the exchange is SINGLETON (one channel).
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE"),
        "SELECT window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedWindowOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-keyed-in");
    writeInput(input);
    // GROUP BY k: the exchange is a hash shuffle on k, split by key into channels columnar.
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE"),
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseKeyedWindowOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-2phase-in");
    writeInput(input);
    // Two-phase: a columnar local pre-aggregate emits partial Arrow batches, a columnar exchange
    // splits them by key, and a columnar global merges — the whole two-phase pipeline flows Arrow.
    NativeParity.assertParity(
        () -> readEnvironment(input, "TWO_PHASE"),
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseCumulativeOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("ccum-2phase-in");
    writeInput(input);
    // Fully-columnar two-phase cumulative: a columnar local pre-aggregates per slice, a columnar
    // exchange splits the partials by key, and a columnar global re-buckets each slice into the
    // nested cumulative windows — the whole local → shuffle → global path flows Arrow.
    NativeParity.assertParity(
        () -> readEnvironment(input, "TWO_PHASE"),
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(CUMULATE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '3' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void rowTimeMiniBatchWindowMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-rowtime-mb-in");
    writeInput(input);
    // With mini-batch on and a watermark-requiring window downstream, the planner inserts a
    // ROW-TIME MiniBatchAssigner: upstream event-time watermarks are filtered to the mini-batch
    // interval instead of markers being generated from the clock. The filtered sequence is a pure
    // function of the input watermarks, so the windowed result stays deterministic and must match.
    NativeParity.assertParity(
        () -> {
          TableEnvironment tEnv = readEnvironment(input, "TWO_PHASE");
          tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
          tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
          tEnv.getConfig().set("table.exec.mini-batch.size", "100");
          return tEnv;
        },
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void keyedSessionOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("csession-in");
    writeInput(input);
    // After the source-edge transpose: watermark assigner → columnar keyed exchange → columnar
    // session aggregator. Output rows match the host.
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE"),
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(SESSION(TABLE t PARTITION BY k, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void partitionedOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cover-in");
    writeInput(input);
    // After the source-edge transpose: watermark assigner → columnar keyed exchange → columnar
    // OVER, with the running SUM appended.
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE"),
        "SELECT k, v, SUM(v) OVER (PARTITION BY k ORDER BY rt) AS total FROM t");
  }

  @Test
  void rowNumberOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("crn-in");
    writeInput(input);
    // ROW_NUMBER() rides the same post-transpose columnar OVER path as the running aggregates.
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE"),
        "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt) AS rn FROM t");
  }

  @Test
  void outOfOrderWithinBatchDropsLateRowLikeHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-ooo-in");
    writeOutOfOrderInput(input);
    // Delay 0: the rt=5000 row closes window [0,1s) before the trailing rt=500 row, which the host
    // drops as late (per row). The columnar assigner slices the batch to emit the watermark between
    // them, so the native pipeline drops it too (divergences/09) — the case that previously diverged.
    NativeParity.assertParity(
        () -> readEnvironment(input, "ONE_PHASE", "rt"),
        "SELECT window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void proctimeTumbleWindowRoutesToNative() throws Exception {
    // A proctime TUMBLE window aggregate. The window boundaries depend on wall-clock processing time,
    // so the result is non-deterministic and cannot be byte-compared to the host (see the CLAUDE.md
    // note); this asserts the query routes to native and runs. Correctness of the assignment/fire is
    // covered deterministically by NativeColumnarWindowAggregateOperatorTest (a controlled clock).
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT window_start, window_end, k, SUM(v) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(pt), INTERVAL '5' SECOND)) "
            + "GROUP BY window_start, window_end, k");
  }

  @Test
  void proctimeHopWindowRoutesToNative() throws Exception {
    // A proctime HOP window (slide divides size): overlapping windows close on chained processing-time
    // timers. Non-deterministic boundaries (see the CLAUDE.md note) — assert it routes and runs;
    // NativeColumnarWindowAggregateOperatorTest pins the chained-timer correctness with a fixed clock.
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT window_start, window_end, k, SUM(v) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(pt), INTERVAL '2' SECOND, INTERVAL '4' SECOND)) "
            + "GROUP BY window_start, window_end, k");
  }

  @Test
  void proctimeCumulateWindowRoutesToNative() throws Exception {
    // A proctime CUMULATE window: nested windows sharing a start close on chained timers as the clock
    // crosses each step. Non-deterministic boundaries — assert it routes and runs.
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT window_start, window_end, k, SUM(v) AS s "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(pt), INTERVAL '1' SECOND, INTERVAL '3' SECOND)) "
            + "GROUP BY window_start, window_end, k");
  }

  @Test
  void proctimeSessionWindowRoutesToNative() throws Exception {
    // A proctime SESSION window: the gap is timed on the clock and a session closes on a
    // processing-time timer at the last element's `now + gap`. Non-deterministic boundaries (see the
    // CLAUDE.md note) — assert it routes and runs; NativeColumnarSessionWindowAggregateOperatorTest
    // pins the gap-merge/close correctness with a fixed clock.
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT window_start, window_end, k, SUM(v) AS s "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(pt), INTERVAL '5' SECOND)) "
            + "GROUP BY window_start, window_end, k");
  }

  @Test
  void legacyProctimeTumbleWindowRoutesToNative() throws Exception {
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT k, SUM(v) AS s, "
            + "TUMBLE_START(pt, INTERVAL '5' SECOND) AS starttime, "
            + "TUMBLE_END(pt, INTERVAL '5' SECOND) AS endtime "
            + "FROM src GROUP BY k, TUMBLE(pt, INTERVAL '5' SECOND)");
  }

  @Test
  void legacyProctimeHopWindowRoutesToNative() throws Exception {
    NativeParity.assertRoutes(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT k, SUM(v) AS s, "
            + "HOP_START(pt, INTERVAL '2' SECOND, INTERVAL '4' SECOND) AS starttime, "
            + "HOP_END(pt, INTERVAL '2' SECOND, INTERVAL '4' SECOND) AS endtime "
            + "FROM src GROUP BY k, HOP(pt, INTERVAL '2' SECOND, INTERVAL '4' SECOND)");
  }

  @Test
  void nonDividingLegacyProctimeHopFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT k, SUM(v) AS s FROM src "
            + "GROUP BY k, HOP(pt, INTERVAL '3' SECOND, INTERVAL '10' SECOND)",
        "processing-time HOP requires slide to divide size");
  }

  @Test
  void legacyProctimeSessionFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkColumnarWindowSqlHarnessTest::proctimeEnvironment,
        "SELECT k, SUM(v) AS s FROM src "
            + "GROUP BY k, SESSION(pt, INTERVAL '1' HOUR)",
        "processing-time SESSION is not native");
  }

  @Test
  void legacyProctimePropertyMaterializesNonNull() throws Exception {
    TableEnvironment tEnv = proctimeEnvironment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String sql =
        "SELECT k, SUM(v) AS s, "
            + "TUMBLE_PROCTIME(pt, INTERVAL '5' SECOND) AS window_proctime "
            + "FROM src GROUP BY k, TUMBLE(pt, INTERVAL '5' SECOND)";
    try (CloseableIterator<Row> rows = tEnv.executeSql(sql).collect()) {
      assertTrue(rows.hasNext());
      while (rows.hasNext()) {
        assertNotNull(rows.next().getField(2));
      }
    }
    assertTrue(scan.substitutions() > 0, "legacy proctime window did not route");
  }

  @Test
  void legacyProctimeWindowWithMisalignedSessionZoneFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkColumnarWindowSqlHarnessTest::proctimeKolkataEnvironment,
        "SELECT k, SUM(v) AS s FROM src GROUP BY k, TUMBLE(pt, INTERVAL '1' HOUR)",
        "session-zone offset to align with the window slide");
  }

  @Test
  void legacyRowCountTumbleFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkColumnarWindowSqlHarnessTest::rowCountWindowEnvironment,
        "SELECT * FROM row_count_window",
        "window size and slide must be day-time intervals");
  }

  private static TableEnvironment rowCountWindowEnvironment() {
    TableEnvironment tEnv = proctimeEnvironment();
    Table result =
        tEnv.from("src")
            .window(
                Tumble.over(Expressions.rowInterval(2L))
                    .on(Expressions.$("pt"))
                    .as("w"))
            .groupBy(Expressions.$("w"), Expressions.$("k"))
            .select(Expressions.$("k"), Expressions.$("v").sum().as("s"));
    tEnv.createTemporaryView("row_count_window", result);
    return tEnv;
  }

  private static TableEnvironment proctimeEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L),
            Row.of(1L, 30L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return tEnv;
  }

  private static TableEnvironment proctimeKolkataEnvironment() {
    TableEnvironment tEnv = proctimeEnvironment();
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Kolkata"));
    return tEnv;
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES "
                + "(1, 10, TO_TIMESTAMP_LTZ(0, 3)), "
                + "(1, 20, TO_TIMESTAMP_LTZ(500, 3)), "
                + "(2, 100, TO_TIMESTAMP_LTZ(500, 3)), "
                + "(1, 30, TO_TIMESTAMP_LTZ(1000, 3)), "
                + "(2, 200, TO_TIMESTAMP_LTZ(1500, 3)), "
                + "(1, 40, TO_TIMESTAMP_LTZ(2500, 3))")
        .await();
  }

  private static TableEnvironment readEnvironment(Path directory, String phaseStrategy) {
    return readEnvironment(directory, phaseStrategy, "rt - INTERVAL '2' SECOND");
  }

  private static TableEnvironment readEnvironment(
      Path directory, String phaseStrategy, String watermark) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", phaseStrategy);
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS "
            + watermark
            + ") WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }

  /** Writes three rows out of event-time order into a single file (one batch). */
  private static void writeOutOfOrderInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    // A high rowtime mid-batch jumps the watermark past the first window, then a low rowtime follows
    // — the late row whose window already closed.
    tEnv.executeSql(
            "INSERT INTO in_write VALUES "
                + "(1, 10, TO_TIMESTAMP_LTZ(0, 3)), "
                + "(1, 20, TO_TIMESTAMP_LTZ(5000, 3)), "
                + "(1, 30, TO_TIMESTAMP_LTZ(500, 3))")
        .await();
  }
}
