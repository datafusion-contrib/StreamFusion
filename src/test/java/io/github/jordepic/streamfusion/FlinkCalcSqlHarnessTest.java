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

/** General Calc (computed/constant projections, with and without a filter) matches the host. */
class FlinkCalcSqlHarnessTest {

  @Test
  void computedProjectionMatchesHost() throws Exception {
    // A computed column with no filter — a projection-only Calc.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v + k FROM f");
  }

  @Test
  void multiplyProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v * 2 FROM f");
  }

  @Test
  void mixedComputedAndColumnProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v + k, s, k FROM f");
  }

  @Test
  void constantProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT k, 100 FROM f");
  }

  @Test
  void computedProjectionWithFilterMatchesHost() throws Exception {
    // Condition plus a computed projection: filtered first, then projected.
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment, "SELECT v + k, s FROM f WHERE v > 15");
  }

  @Test
  void caseProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT CASE WHEN v > 20 THEN 1 ELSE 0 END FROM f");
  }

  @Test
  void caseWithComputedBranchesMatchesHost() throws Exception {
    // Same-width branches (a column and a computed column); mixed widths need CAST, not yet admitted.
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT CASE WHEN s <> 'a' THEN v ELSE v + 1 END FROM f");
  }

  @Test
  void caseInFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT k FROM f WHERE (CASE WHEN v > 20 THEN 1 ELSE 0 END) = 1");
  }

  @Test
  void wideningCastMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT CAST(v AS BIGINT) FROM f");
  }

  @Test
  void castToDoubleMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT CAST(v AS DOUBLE) FROM f");
  }

  @Test
  void caseWithMixedWidthBranchesMatchesHost() throws Exception {
    // BIGINT/INT branches: the host casts the INT branch up to BIGINT, now an admitted widening cast.
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment, "SELECT CASE WHEN s <> 'a' THEN k ELSE v END FROM f");
  }

  @Test
  void narrowingCastFallsBack() throws Exception {
    // BIGINT → INT is narrowing (overflow semantics differ), so it is not admitted and falls back.
    NativeParity.assertFallback(
        FlinkCalcSqlHarnessTest::environment, "SELECT CAST(k AS INT) FROM f");
  }

  @Test
  void unsupportedProjectionFunctionFallsBack() throws Exception {
    // A function the expression encoder does not admit makes the whole Calc fall back, and the
    // fallback reason names the offending function (ticket 29).
    NativeParity.assertFallbackReasonContains(
        FlinkCalcSqlHarnessTest::environment, "SELECT ABS(v) FROM f", "ABS");
  }

  @Test
  void isNullFilterMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::nullableEnvironment, "SELECT k FROM g WHERE s IS NULL");
  }

  @Test
  void isNotNullFilterMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::nullableEnvironment, "SELECT k FROM g WHERE v IS NOT NULL");
  }

  @Test
  void coalesceMatchesHost() throws Exception {
    // COALESCE lowers to CASE in sql-to-rel, so it rides the admitted CASE path (numeric branches,
    // no cast). The string form COALESCE(s,'x') needs a CHAR→VARCHAR cast, still not admitted.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::nullableEnvironment, "SELECT COALESCE(v, 0) FROM g");
  }

  @Test
  void nullifMatchesHost() throws Exception {
    // NULLIF lowers to CASE WHEN a = b THEN NULL ELSE a, exercising a NULL literal in a branch.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::nullableEnvironment, "SELECT NULLIF(v, 30) FROM g");
  }

  @Test
  void integerDivisionMatchesHost() throws Exception {
    // Integer division truncates toward zero on both sides (Java and Rust agree).
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v / 3 FROM f");
  }

  @Test
  void integerDivisionNegativeMatchesHost() throws Exception {
    // Negative dividends are where truncation-toward-zero vs floor would diverge; (v - 50) goes
    // negative for the small rows, so this pins the sign behavior.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT (v - 50) / 3 FROM f");
  }

  @Test
  void moduloMatchesHost() throws Exception {
    // Modulo takes the sign of the dividend on both sides; the negative dividend pins it.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT (v - 50) % 7 FROM f");
  }

  @Test
  void divisionInFilterMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT k FROM f WHERE v / 3 > 5");
  }

  @Test
  void tinyintAdditionOverflowMatchesHost() throws Exception {
    // a + b overflows the i8 range (100 + 100); pins whether the host wraps in TINYINT or promotes.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::narrowIntEnvironment, "SELECT a + b FROM n");
  }

  @Test
  void smallintMultiplyOverflowMatchesHost() throws Exception {
    // c * c overflows the i16 range (300 * 300); pins SMALLINT arithmetic width.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::narrowIntEnvironment, "SELECT c * c FROM n");
  }

  @Test
  void upperMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT UPPER(s) FROM f");
  }

  @Test
  void lowerMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT LOWER(s) FROM f");
  }

  @Test
  void charLengthMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT CHAR_LENGTH(s) FROM f");
  }

  @Test
  void upperInFilterMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT k FROM f WHERE UPPER(s) = 'B'");
  }

  @Test
  void concatFallsBack() throws Exception {
    // Flink's CONCAT propagates NULL (CONCAT(null,x) = null), but DataFusion's `concat` ignores NULL
    // args — a semantic divergence — so CONCAT is not admitted and the Calc falls back, naming it.
    NativeParity.assertFallbackReasonContains(
        FlinkCalcSqlHarnessTest::nullableEnvironment, "SELECT CONCAT(s, '!') FROM g", "CONCAT");
  }

  @Test
  void trimMatchesHost() throws Exception {
    // Default whitespace both-sides trim maps to DataFusion btrim; spaced values exercise it.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::spacedStringEnvironment, "SELECT TRIM(s) FROM ss");
  }

  @Test
  void trimLeadingFallsBack() throws Exception {
    // Only TRIM(BOTH ' ' …) is admitted; LEADING/TRAILING trims fall back.
    NativeParity.assertFallback(
        FlinkCalcSqlHarnessTest::spacedStringEnvironment, "SELECT TRIM(LEADING FROM s) FROM ss");
  }

  @Test
  void substringFromMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::spacedStringEnvironment, "SELECT SUBSTRING(s FROM 2) FROM ss");
  }

  @Test
  void substringFromForMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::spacedStringEnvironment, "SELECT SUBSTRING(s FROM 2 FOR 3) FROM ss");
  }

  @Test
  void substringStartBelowOneFallsBack() throws Exception {
    // Flink clamps a start below 1 to 1; DataFusion counts the out-of-range prefix against the
    // length. So a literal start < 1 is not admitted and the Calc falls back.
    NativeParity.assertFallback(
        FlinkCalcSqlHarnessTest::spacedStringEnvironment, "SELECT SUBSTRING(s FROM 0 FOR 3) FROM ss");
  }

  @Test
  void substringRuntimePositionFallsBack() throws Exception {
    // A non-literal start (column v) can't be range-checked at plan time, so it falls back, and the
    // reason points at SUBSTRING.
    NativeParity.assertFallbackReasonContains(
        FlinkCalcSqlHarnessTest::environment, "SELECT SUBSTRING(s FROM v) FROM f", "SUBSTRING");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
            Row.of(1L, 10, "a"),
            Row.of(2L, 30, "b"),
            Row.of(3L, 20, "c"),
            Row.of(4L, 40, "d"));
    tEnv.createTemporaryView(
        "f",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }

  private static TableEnvironment spacedStringEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
            Row.of("  pad  "),
            Row.of("x"),
            Row.of(" leading"),
            Row.of("trailing "));
    tEnv.createTemporaryView(
        "ss", source, Schema.newBuilder().column("s", DataTypes.STRING()).build());
    return tEnv;
  }

  private static TableEnvironment narrowIntEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"a", "b", "c"}, Types.BYTE, Types.BYTE, Types.SHORT),
            Row.of((byte) 100, (byte) 100, (short) 300),
            Row.of((byte) 1, (byte) 2, (short) 3));
    tEnv.createTemporaryView(
        "n",
        source,
        Schema.newBuilder()
            .column("a", DataTypes.TINYINT())
            .column("b", DataTypes.TINYINT())
            .column("c", DataTypes.SMALLINT())
            .build());
    return tEnv;
  }

  private static TableEnvironment nullableEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
            Row.of(1L, 10, "a"),
            Row.of(2L, 30, null),
            Row.of(3L, null, "c"),
            Row.of(4L, 40, "d"));
    tEnv.createTemporaryView(
        "g",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
