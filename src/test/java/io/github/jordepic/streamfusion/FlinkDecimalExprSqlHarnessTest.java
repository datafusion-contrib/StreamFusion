package io.github.jordepic.streamfusion;

import java.math.BigDecimal;
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
 * Decimal arithmetic in a Calc — Nexmark q1's {@code 0.908 * price}. All of it runs natively and
 * byte-exactly. Add/subtract/multiply: the operands reach the native side as Decimal128 (columns
 * already are; literals emit as exact Decimal128), Arrow's Decimal128 arithmetic matches Flink's, and
 * the wrapping cast to the declared DECIMAL rounds HALF_UP — the same rounding Flink uses.
 * Division/modulo run through a fused native kernel reproducing Flink's two rounding steps: the
 * quotient to 38 significant digits (HALF_UP), then the rescale to the declared type.
 */
class FlinkDecimalExprSqlHarnessTest {

  @Test
  void decimalTimesDecimalExactByDefault() throws Exception {
    // q1 exactly: a DECIMAL literal times a DECIMAL(23,3) column, widening to DECIMAL(28,6).
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, 0.908 * price AS price FROM t");
  }

  @Test
  void decimalTimesBigintExactByDefault() throws Exception {
    // A DECIMAL literal times a BIGINT column (bigint coerced to DECIMAL(19,0) before the multiply).
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::bigintPriceEnvironment,
        "SELECT auction, 0.908 * price AS price FROM t");
  }

  @Test
  void decimalCastExactByDefault() throws Exception {
    // The sink coercion in q1: the DECIMAL(28,6) product cast down to DECIMAL(23,3), HALF_UP — exact.
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, CAST(0.908 * price AS DECIMAL(23, 3)) AS price FROM t");
  }

  @Test
  void decimalPlusMinusExactByDefault() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, price + 1.5 AS a, price - 0.001 AS b FROM t");
  }

  @Test
  void decimalDivisionExactByDefault() throws Exception {
    // Repeating quotients (÷3, ÷7) exercise both rounding steps; the negated column exercises
    // HALF_UP's away-from-zero on negatives.
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, price / 3 AS a, price / 7.77 AS b, (0 - price) / 3 AS c FROM t");
  }

  @Test
  void decimalDivisionByDecimalColumnExactByDefault() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, 1000.5 / price AS inv FROM t");
  }

  @Test
  void decimalModuloExactByDefault() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, MOD(price, 2.1) AS m, MOD(0 - price, 2.1) AS mn FROM t");
  }

  @Test
  void decimalDivisionCastDownExactByDefault() throws Exception {
    // The declared quotient type then cast down further — both rescales HALF_UP, like Flink.
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, CAST(price / 3 AS DECIMAL(10, 2)) AS a FROM t");
  }

  private static TableEnvironment decimalPriceEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"auction", "price"}, Types.LONG, Types.BIG_DEC),
            Row.of(1L, new BigDecimal("100.000")),
            Row.of(2L, new BigDecimal("999.999")),
            Row.of(3L, new BigDecimal("0.001")),
            Row.of(4L, new BigDecimal("12345.678")));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("auction", DataTypes.BIGINT())
            .column("price", DataTypes.DECIMAL(23, 3))
            .build());
    return tEnv;
  }

  private static TableEnvironment bigintPriceEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"auction", "price"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 999L),
            Row.of(3L, 1L));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("auction", DataTypes.BIGINT())
            .column("price", DataTypes.BIGINT())
            .build());
    return tEnv;
  }
}
