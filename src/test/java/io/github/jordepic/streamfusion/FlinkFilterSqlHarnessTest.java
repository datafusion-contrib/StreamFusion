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

class FlinkFilterSqlHarnessTest {

  @Test
  void numericFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 20");
  }

  @Test
  void reversedOperandFilterMatchesHost() throws Exception {
    // Literal on the left: `20 >= v` is `v <= 20` — the matcher flips the operator.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE 20 >= v");
  }

  @Test
  void bigintInequalityFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE k <> 2");
  }

  @Test
  void conjunctionFilterMatchesHost() throws Exception {
    // A conjunction of comparisons across two columns; the native filter ANDs the masks.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 15 AND k <= 3");
  }

  @Test
  void projectionWithFilterMatchesHost() throws Exception {
    // A column subset/reorder projection alongside the filter — projected after filtering.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT s, k FROM f WHERE v > 15");
  }

  @Test
  void disjunctionFilterMatchesHost() throws Exception {
    // OR of comparisons across columns — disjunctive normal form, ORed in the native filter.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 35 OR k < 2");
  }

  @Test
  void mixedAndOrFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment,
        "SELECT * FROM f WHERE (v >= 30 AND k <> 4) OR s <> 'a'");
  }

  @Test
  void rangeFilterMatchesHost() throws Exception {
    // BETWEEN folds to a SEARCH range, which expands to an AND of two comparisons.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v BETWEEN 20 AND 35");
  }

  @Test
  void stringInequalityFilterMatchesHost() throws Exception {
    // `<>` preserves the column (no constant folding), so a string inequality routes.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE s <> 'b'");
  }

  @Test
  void mixedStringAndNumericFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE s <> 'a' AND v >= 20");
  }

  @Test
  void arithmeticPredicateMatchesHost() throws Exception {
    // Arithmetic in the predicate (across columns of different width) — the general expression path.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT k, v FROM f WHERE v + k > 25");
  }

  @Test
  void multiplicationPredicateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v * 2 > 50");
  }

  @Test
  void integerOverflowArithmeticMatchesHost() throws Exception {
    // `v * 2` on an int near INT_MAX overflows; native must wrap in int32 exactly as the host does,
    // not evaluate in a widened type. The large row wraps negative and is excluded by both.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::overflowEnvironment, "SELECT v FROM o WHERE v * 2 > 50");
  }

  @Test
  void isNotNullFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::nullableEnvironment, "SELECT * FROM n WHERE v IS NOT NULL");
  }

  @Test
  void unsupportedFunctionFallsBack() throws Exception {
    // A function the expression encoder does not admit makes the whole filter fall back to the host.
    NativeParity.assertFallback(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE ABS(v) > 20");
  }

  @Test
  void filterCarriesTimestampColumnMatchesHost() throws Exception {
    // The row carries a TIMESTAMP column through the whole-row converter while filtering on another.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::timestampEnvironment, "SELECT * FROM g WHERE v > 15");
  }

  private static TableEnvironment nullableEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT),
            Row.of(1L, 10),
            Row.of(2L, null),
            Row.of(3L, 20));
    tEnv.createTemporaryView(
        "n",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.INT()).build());
    return tEnv;
  }

  private static TableEnvironment overflowEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(Types.ROW_NAMED(new String[] {"v"}, Types.INT), Row.of(30), Row.of(2_000_000_000));
    tEnv.createTemporaryView(
        "o", source, Schema.newBuilder().column("v", DataTypes.INT()).build());
    return tEnv;
  }

  private static TableEnvironment timestampEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"v", "t"}, Types.INT, Types.LOCAL_DATE_TIME),
            Row.of(10, LocalDateTime.of(2020, 1, 1, 0, 0, 0)),
            Row.of(30, LocalDateTime.of(2020, 1, 2, 12, 30, 0)),
            Row.of(20, LocalDateTime.of(2020, 1, 3, 6, 15, 45)));
    tEnv.createTemporaryView(
        "g",
        source,
        Schema.newBuilder()
            .column("v", DataTypes.INT())
            .column("t", DataTypes.TIMESTAMP(3))
            .build());
    return tEnv;
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
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
}
