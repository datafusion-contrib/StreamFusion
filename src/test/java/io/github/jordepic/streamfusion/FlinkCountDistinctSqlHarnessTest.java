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
 * Non-windowed {@code GROUP BY} DISTINCT aggregates. {@code COUNT(DISTINCT x)} keeps a per-key
 * value→multiplicity map (Flink's {@code DistinctAccumulator}) and reports the number of live
 * distinct values, growing it when a value first appears and shrinking it when its last occurrence is
 * retracted. {@code SUM(DISTINCT x)} shares the map and folds a running sum only as values enter and
 * leave the set; {@code MIN}/{@code MAX(DISTINCT x)} run as their plain forms (the extreme ignores
 * multiplicity). The collapsed changelog must match the host. A windowed DISTINCT aggregate falls
 * back — the window operators fold every row, so routing it would over-count.
 */
class FlinkCountDistinctSqlHarnessTest {

  @Test
  void countDistinctMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, COUNT(DISTINCT v) AS dv FROM t GROUP BY k");
  }

  @Test
  void countDistinctStringAndMixedMatchesHost() throws Exception {
    // A string distinct count alongside a plain COUNT(*) and a second distinct count in one GROUP BY.
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, COUNT(DISTINCT s) AS ds, COUNT(*) AS c, COUNT(DISTINCT v) AS dv "
            + "FROM t GROUP BY k");
  }

  @Test
  void sumDistinctMatchesHost() throws Exception {
    // Duplicates per key make SUM(DISTINCT v) differ from SUM(v); running both proves the distinct
    // sum folds each value once while the plain sum folds every row.
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, SUM(DISTINCT v) AS sdv, SUM(v) AS sv FROM t GROUP BY k");
  }

  @Test
  void sumDistinctDoubleAndDecimalMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, SUM(DISTINCT d) AS sdd, SUM(DISTINCT dcm) AS sdec FROM t GROUP BY k");
  }

  @Test
  void minMaxDistinctMatchesHost() throws Exception {
    // MIN/MAX(DISTINCT) equal their plain forms; run both to pin that.
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, MIN(DISTINCT v) AS mn, MAX(DISTINCT v) AS mx, MIN(DISTINCT s) AS ms "
            + "FROM t GROUP BY k");
  }

  @Test
  void sumDistinctOverRetractingInputMatchesHost() throws Exception {
    // The inner GROUP BY emits a changelog, so the outer SUM(DISTINCT) sees retractions: a value
    // must leave the running sum only when its last occurrence retracts.
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT cnt, SUM(DISTINCT mx) AS sd FROM "
            + "(SELECT k, COUNT(*) AS cnt, MAX(v) AS mx FROM t GROUP BY k) GROUP BY cnt");
  }

  @Test
  void windowedDistinctFallsBack() throws Exception {
    // The windowed form dedups inside the window; the native window operators would over-count, so
    // the whole query must stay on Flink and still match.
    NativeParity.assertFallback(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT window_start, SUM(DISTINCT v) AS sdv FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts),"
            + " INTERVAL '1' SECOND)) GROUP BY window_start, window_end");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // Repeats per key so distinct < total: k=1 has v {10,20} (10 twice), s {a,b}; k=2 has v {5}, s {c}.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "v", "s", "d", "dcm", "millis"},
                Types.LONG,
                Types.LONG,
                Types.STRING,
                Types.DOUBLE,
                Types.BIG_DEC,
                Types.LONG),
            Row.of(1L, 10L, "a", 1.5, new java.math.BigDecimal("10.25"), 1_000L),
            Row.of(1L, 10L, "a", 1.5, new java.math.BigDecimal("10.25"), 1_100L),
            Row.of(1L, 20L, "b", 2.5, new java.math.BigDecimal("20.75"), 2_200L),
            Row.of(2L, 5L, "c", 0.5, new java.math.BigDecimal("5.50"), 1_300L),
            Row.of(2L, 5L, "c", 0.5, new java.math.BigDecimal("5.50"), 2_400L),
            Row.of(1L, 20L, "b", 2.5, new java.math.BigDecimal("20.75"), 3_500L));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("d", DataTypes.DOUBLE())
            .column("dcm", DataTypes.DECIMAL(10, 2))
            .column("millis", DataTypes.BIGINT())
            .columnByExpression("ts", "TO_TIMESTAMP_LTZ(millis, 3)")
            .watermark("ts", "ts - INTERVAL '0.001' SECOND")
            .build());
    return tEnv;
  }
}
