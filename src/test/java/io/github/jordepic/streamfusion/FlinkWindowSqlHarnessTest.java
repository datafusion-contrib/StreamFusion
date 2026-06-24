package io.github.jordepic.streamfusion;

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

class FlinkWindowSqlHarnessTest {

  private static final java.time.LocalDate DAY_ONE = java.time.LocalDate.of(2026, 1, 1);
  private static final java.time.LocalDate DAY_TWO = java.time.LocalDate.of(2026, 1, 2);
  private static final java.time.LocalDateTime T0 = java.time.LocalDateTime.of(2026, 1, 1, 9, 0, 0);
  private static final java.time.LocalDateTime T1 = java.time.LocalDateTime.of(2026, 1, 1, 9, 0, 1);

  private static java.math.BigDecimal dec(String v) {
    return new java.math.BigDecimal(v);
  }

  @Test
  void intAvgMatchesHost() throws Exception {
    // Integer AVG over an int column: the sum accumulates in bigint, divides truncating toward
    // zero, and casts back to int — matching the host (which returns the input integer type).
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, AVG(qty) AS mean "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void tumblingSumMatchesHost() throws Exception {
    assertWindowParity("SUM(`value`)");
  }

  @Test
  void tumblingMaxMatchesHost() throws Exception {
    assertWindowParity("MAX(`value`)");
  }

  @Test
  void tumblingAvgMatchesHost() throws Exception {
    assertWindowParity("AVG(`value`)");
  }

  @Test
  void stringKeyTumblingMatchesHost() throws Exception {
    // A string grouping key, keyed natively as a scalar and emitted back as a string.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT s, window_start, window_end, SUM(`value`) AS sm, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, window_start, window_end");
  }

  @Test
  void stringAndBigintKeyTumblingMatchesHost() throws Exception {
    // Mixed key types: a string and a bigint key together.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT s, k, window_start, window_end, SUM(`value`) AS sm "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, k, window_start, window_end");
  }

  @Test
  void stringKeyTwoPhaseMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT s, window_start, window_end, SUM(`value`) AS sm "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, window_start, window_end");
  }

  @Test
  void intKeyTumblingMatchesHost() throws Exception {
    // An int grouping key: keyed natively as int64 but emitted back as INT to match the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT qty, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY qty, window_start, window_end");
  }

  @Test
  void twoKeyTumblingMatchesHost() throws Exception {
    // GROUP BY two bigint keys plus the window: the native composite key must match the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void twoKeySessionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY (k, g), DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void twoPhaseTwoKeyTumblingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void keyedTumblingSumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseTumblingSumMatchesHost() throws Exception {
    // Default (two-phase) planning: a native local pre-aggregate and global merge must agree
    // with the host's local+global aggregation.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void hoppingSumMatchesHost() throws Exception {
    // One-phase HOP: a row falls in two overlapping 2s windows sliding every 1s.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedHoppingMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void doubleAvgMatchesHost() throws Exception {
    // AVG over a double: sum and divide in double, matching the host's double average.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, AVG(amount) AS a "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void doubleValueAggregateMatchesHost() throws Exception {
    // One-phase double value: SUM/MAX over a double column, plus a bigint COUNT alongside.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(amount) AS s, MAX(amount) AS m, COUNT(amount) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void sessionSumMatchesHost() throws Exception {
    // Session windows: consecutive rows within the gap form one window; a larger gap splits them.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(SESSION(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedSessionMultiAggregateMatchesHost() throws Exception {
    // Per-key sessions: each key's gaps are independent, partitioned by the session TVF.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, MAX(`value`) AS m "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void sessionMergeMatchesHost() throws Exception {
    // An out-of-order element lands between two open sessions and bridges them into one; the native
    // merge of the two windows' accumulators must match the host's merging assigner.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentForSessionMerge,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(SESSION(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void cumulativeSumMatchesHost() throws Exception {
    // Cumulative windows: nested windows sharing a bucket start, ends growing by the step.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedCumulativeMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, MAX(`value`) AS m "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseCumulativeMatchesHost() throws Exception {
    // Two-phase (the default plan): a native local pre-aggregates per 1s slice, the host shuffles by
    // key, and a native global re-buckets each slice into the nested cumulative windows up to the 3s
    // max size. Both halves substitute (unlike one-phase, which is a single window operator).
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '3' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void multiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c, MAX(`value`) AS m "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseHoppingSumMatchesHost() throws Exception {
    // Default-planned HOP is two-phase and slice-shared: a local per-slice pre-aggregate, the
    // shuffle, and a global that combines each window's slices must agree with the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseKeyedHoppingMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseDoubleValueMatchesHost() throws Exception {
    // Default-planned (two-phase) aggregation over a double value: the local emits a double sum
    // partial alongside a bigint count partial, and the global merges the mixed-type partials.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(amount) AS s, MAX(amount) AS m, COUNT(amount) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseTumblingCountMatchesHost() throws Exception {
    // COUNT exercises the local=count / global=sum split.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void intValueAggregatesMatchHost() throws Exception {
    // SUM/MIN/MAX/COUNT over a 32-bit int. SUM uses the native wrapping int32 accumulator so it
    // keeps the host's narrow type; AVG over int would truncate and stays on the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(qty) AS s, MIN(qty) AS lo, MAX(qty) AS hi, "
            + "COUNT(qty) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void smallIntMinMaxCountMatchesHost() throws Exception {
    // MIN/MAX/COUNT over SMALLINT: type-preserving comparisons/counts, no arithmetic to diverge.
    assertNarrowAggregatesMatchHost("sm");
  }

  @Test
  void tinyIntMinMaxCountMatchesHost() throws Exception {
    assertNarrowAggregatesMatchHost("tn");
  }

  @Test
  void floatMinMaxCountMatchesHost() throws Exception {
    // FLOAT MIN/MAX keep the 4-byte type (unlike SUM, which Flink would not widen to double).
    assertNarrowAggregatesMatchHost("fl");
  }

  @Test
  void countStarTumblingMatchesHost() throws Exception {
    // COUNT(*) has no value column; the operator synthesizes a non-null column and counts rows.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, COUNT(*) AS n "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedCountStarMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, COUNT(*) AS n "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseCountStarMatchesHost() throws Exception {
    // Default planning: the native local counts rows per slice (COUNT over a synthesized column),
    // and the native global sums the per-slice counts (COUNT's merge is a sum).
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(*) AS n "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseHoppingCountStarMatchesHost() throws Exception {
    // Default-planned HOP COUNT(*): the user COUNT doubles as the slicing count1 (no separate
    // synthetic partial), so the native local must not append one — the global sums the per-slice
    // counts fanned into each window.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(*) AS n "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseHoppingCountStarWithSumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(*) AS n, SUM(`value`) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseCountStarWithValueAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(*) AS n, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void countStarWithValueAggregateMatchesHost() throws Exception {
    // COUNT(*) alongside a value aggregate: COUNT(*) counts a synthesized column, SUM reads its own.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, COUNT(*) AS n, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void multipleValueColumnsMatchHost() throws Exception {
    // Aggregates over three different value columns of different types, in one window.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, MAX(amount) AS m, MIN(qty) AS q "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedMultipleValueColumnsMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, SUM(amount) AS a, COUNT(*) AS n "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseMultipleValueColumnsMatchHost() throws Exception {
    // Default planning over different mergeable value columns: a bigint SUM and a double SUM, whose
    // partials (bigint, double) the global merges side by side.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(`value`) AS s, SUM(amount) AS a "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void decimalMinMaxCountMatchesHost() throws Exception {
    // MIN/MAX/COUNT over DECIMAL: type-preserving (same precision/scale), no arithmetic to diverge.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, MIN(price) AS lo, MAX(price) AS hi, COUNT(price) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void decimalSumFallsBack() throws Exception {
    // SUM over DECIMAL stays on the host: DataFusion's sum precision derivation differs from Flink's.
    NativeParity.assertParity( // still correct via fallback (a trailing projection may route)
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(price) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void decimalKeyTumblingMatchesHost() throws Exception {
    // A DECIMAL grouping key, carried in an Arrow decimal column and emitted back as DecimalData.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT price, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY price, window_start, window_end");
  }

  @Test
  void timestampKeyTumblingMatchesHost() throws Exception {
    // A TIMESTAMP grouping key, carried as int64 nanoseconds and emitted back as TimestampData.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT tsc, window_start, window_end, SUM(`value`) AS s, COUNT(*) AS n "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY tsc, window_start, window_end");
  }

  @Test
  void booleanKeyTumblingMatchesHost() throws Exception {
    // A boolean grouping key, carried natively as a bit column and emitted back as boolean.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT b, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY b, window_start, window_end");
  }

  @Test
  void dateKeyTumblingMatchesHost() throws Exception {
    // A DATE grouping key, carried as the epoch-day int in a Date32 column.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT d, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY d, window_start, window_end");
  }

  @Test
  void smallIntSumAvgMatchesHost() throws Exception {
    // SUM/AVG over SMALLINT: the native wrapping sum and truncating avg keep the input type.
    assertNarrowSumAvgMatchHost("sm");
  }

  @Test
  void tinyIntSumAvgMatchesHost() throws Exception {
    assertNarrowSumAvgMatchHost("tn");
  }

  @Test
  void narrowSumWrapsLikeHost() throws Exception {
    // SUM over narrow ints wraps at the input width on overflow, exactly as the host's cast-back
    // accumulator does (TINYINT 100+100 → -56; SMALLINT 30000+30000 → -5536).
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::narrowOverflowEnvironment,
        "SELECT window_start, window_end, SUM(tn) AS st, SUM(sm) AS ss "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseNarrowSumMatchesHost() throws Exception {
    // Under default planning the narrow SUM's partial type is not one the global merges, so the
    // window aggregate must stay on the host — never a native-local + host-global mismatch.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(sm) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseFloatSumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(fl) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static void assertNarrowSumAvgMatchHost(String column) throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(" + column + ") AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, AVG(" + column + ") AS a "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void floatSumAvgMatchesHost() throws Exception {
    // SUM/AVG over FLOAT: the native float sum keeps 4-byte precision and the float avg sums in
    // double then narrows, matching the host (unlike DataFusion's sum, which widens to double).
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(fl) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, AVG(fl) AS a "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void floatSumMatchesHostUnderAccumulationError() throws Exception {
    // Values whose 4-byte running sum accumulates rounding error: native must fold in float (not
    // double) to match the host bit-for-bit. A double accumulation would drift from the host here.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::floatPrecisionEnvironment,
        "SELECT window_start, window_end, SUM(fl) AS s, AVG(fl) AS a "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static void assertNarrowAggregatesMatchHost(String column) throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, "
            + "MIN(" + column + ") AS lo, MAX(" + column + ") AS hi, COUNT(" + column + ") AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static void assertWindowParity(String aggregate) throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, "
            + aggregate
            + " AS agg "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static TableEnvironment environmentWithSource() {
    return buildEnvironment(true);
  }

  private static TableEnvironment environmentForSessionMerge() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Out-of-order: the row at ts=700 arrives after the rows at 0 and 1500, which would otherwise
    // be separate sessions (gap 1s), and its [700, 1700) window bridges them into [0, 2500).
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"value", "ts"}, Types.LONG, Types.LONG),
                Row.of(1L, 0L),
                Row.of(2L, 1500L),
                Row.of(4L, 700L),
                Row.of(9L, 5000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("value", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static TableEnvironment environmentTwoPhase() {
    return buildEnvironment(false);
  }

  private static TableEnvironment floatPrecisionEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Eight 0.1f's in one 1s window: their float running sum is not 0.8f (rounding accumulates),
    // so the result distinguishes a float accumulation (the host's) from a double one.
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"fl", "ts"}, Types.FLOAT, Types.LONG),
                Row.of(0.1f, 0L),
                Row.of(0.1f, 100L),
                Row.of(0.1f, 200L),
                Row.of(0.1f, 300L),
                Row.of(0.1f, 400L),
                Row.of(0.1f, 500L),
                Row.of(0.1f, 600L),
                Row.of(0.1f, 700L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("fl", DataTypes.FLOAT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static TableEnvironment narrowOverflowEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Two values per 1s window that overflow the narrow width when summed.
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"tn", "sm", "ts"}, Types.BYTE, Types.SHORT, Types.LONG),
                Row.of((byte) 100, (short) 30000, 0L),
                Row.of((byte) 100, (short) 30000, 500L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("tn", DataTypes.TINYINT())
            .column("sm", DataTypes.SMALLINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static TableEnvironment buildEnvironment(boolean onePhase) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    if (onePhase) {
      tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    }

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {
                      "k", "value", "ts", "amount", "qty", "g", "s", "sm", "tn", "fl", "b", "d",
                      "price", "tsc"
                    },
                    Types.LONG,
                    Types.LONG,
                    Types.LONG,
                    Types.DOUBLE,
                    Types.INT,
                    Types.LONG,
                    Types.STRING,
                    Types.SHORT,
                    Types.BYTE,
                    Types.FLOAT,
                    Types.BOOLEAN,
                    Types.LOCAL_DATE,
                    Types.BIG_DEC,
                    Types.LOCAL_DATE_TIME),
                Row.of(7L, 1L, 0L, 1.5, 10, 100L, "a", (short) 10, (byte) 1, 1.5f, true, DAY_ONE, dec("1.10"), T0),
                Row.of(7L, 2L, 500L, 2.5, 20, 100L, "a", (short) 20, (byte) 2, 2.5f, true, DAY_ONE, dec("2.20"), T0),
                Row.of(9L, 3L, 600L, 3.0, 30, 200L, "b", (short) 30, (byte) 3, 3.5f, false, DAY_TWO, dec("3.30"), T1),
                Row.of(7L, 4L, 1500L, 4.5, 40, 100L, "a", (short) 40, (byte) 4, 4.5f, true, DAY_ONE, dec("4.40"), T0),
                Row.of(9L, 5L, 2500L, 5.5, 50, 200L, "b", (short) 50, (byte) 5, 5.5f, false, DAY_TWO, dec("5.50"), T1))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));

    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .column("amount", DataTypes.DOUBLE())
            .column("qty", DataTypes.INT())
            .column("g", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("sm", DataTypes.SMALLINT())
            .column("tn", DataTypes.TINYINT())
            .column("fl", DataTypes.FLOAT())
            .column("b", DataTypes.BOOLEAN())
            .column("d", DataTypes.DATE())
            .column("price", DataTypes.DECIMAL(10, 2))
            .column("tsc", DataTypes.TIMESTAMP(3))
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
