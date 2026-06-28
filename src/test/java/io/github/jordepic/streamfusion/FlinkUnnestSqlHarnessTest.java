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
 * INNER {@code UNNEST} of an array matches the host. The native operator fans each input row out to
 * one row per array element — Flink's {@code Correlate} over {@code $UNNEST_ROWS$} — so an empty or
 * null array yields no rows and a null element yields a null row. The array column rides the Arrow
 * boundary (now that nested types are carried) and is unnested natively.
 */
class FlinkUnnestSqlHarnessTest {

  @Test
  void unnestArrayMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment, "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e)");
  }

  @Test
  void unnestArrayCommaSyntaxMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment, "SELECT k, e FROM t, UNNEST(vs) AS u(e)");
  }

  @Test
  void unnestWithPushedFilterMatchesHost() throws Exception {
    // A filter on the unnested element is pushed into the Correlate as a `condition`; it is applied
    // as a native filter over the unnest output (the condition's ref shifted to index the element).
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e) WHERE e > 25");
  }

  @Test
  void unnestWithComputedFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e) WHERE e + 5 < 40");
  }

  @Test
  void unnestStringArrayMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::stringArrayEnvironment,
        "SELECT k, s FROM t CROSS JOIN UNNEST(ss) AS u(s)");
  }

  @Test
  void unnestRowArrayFlattensFieldsMatchesHost() throws Exception {
    // UNNEST of an ARRAY<ROW> flattens the element struct into separate columns ([k, a, b]).
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::rowArrayEnvironment,
        "SELECT k, a, b FROM t CROSS JOIN UNNEST(rs) AS u(a, b)");
  }

  @Test
  void unnestRowArrayWithNullElementMatchesHost() throws Exception {
    // A null ROW element flattens to all-null fields (the struct null mask folded into each child).
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::rowArrayEnvironment,
        "SELECT k, a, b FROM t CROSS JOIN UNNEST(rs) AS u(a, b) WHERE k = 3");
  }

  @Test
  void leftUnnestMatchesHost() throws Exception {
    // LEFT (outer) unnest: an empty array (k=3) or null array (k=4) is kept with a null element.
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e FROM t LEFT JOIN UNNEST(vs) AS u(e) ON TRUE");
  }

  @Test
  void leftUnnestWithOrdinalityMatchesHost() throws Exception {
    // A LEFT null-pad row carries a null ordinal too.
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e, o FROM t LEFT JOIN UNNEST(vs) WITH ORDINALITY AS u(e, o) ON TRUE");
  }

  @Test
  void unnestWithOrdinalityMatchesHost() throws Exception {
    // WITH ORDINALITY appends a 1-based position column (the element's index in its array).
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e, o FROM t CROSS JOIN UNNEST(vs) WITH ORDINALITY AS u(e, o)");
  }

  @Test
  void unnestMapWithOrdinalityMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::mapEnvironment,
        "SELECT k, mk, mv, o FROM t CROSS JOIN UNNEST(m) WITH ORDINALITY AS u(mk, mv, o)");
  }

  @Test
  void unnestMapMatchesHost() throws Exception {
    // UNNEST of a MAP yields one row per entry, appending a key and a value column.
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::mapEnvironment,
        "SELECT k, mk, mv FROM t CROSS JOIN UNNEST(m) AS u(mk, mv)");
  }

  @Test
  void perOperatorFlagKeepsUnnestOnHost() throws Exception {
    System.setProperty("streamfusion.operator.unnest.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkUnnestSqlHarnessTest::environment,
          "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e)",
          "unnest: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.unnest.enabled");
    }
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
            Row.of(1L, new Long[] {10L, 20L}),
            Row.of(2L, new Long[] {30L}),
            Row.of(3L, new Long[] {}), // empty array → no rows (INNER)
            Row.of(4L, null), // null array → no rows (INNER)
            Row.of(5L, new Long[] {40L, null, 50L})); // null element → null row
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("vs", DataTypes.ARRAY(DataTypes.BIGINT()))
            .build());
    return tEnv;
  }

  private static TableEnvironment mapEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "m"}, Types.LONG, Types.MAP(Types.STRING, Types.LONG)),
            Row.of(1L, java.util.Map.of("a", 10L, "b", 20L)),
            Row.of(2L, java.util.Map.of("c", 30L)));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .build());
    return tEnv;
  }

  private static TableEnvironment rowArrayEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "rs"},
                Types.LONG,
                Types.OBJECT_ARRAY(
                    Types.ROW_NAMED(new String[] {"a", "b"}, Types.LONG, Types.STRING))),
            Row.of(1L, new Row[] {Row.of(10L, "x"), Row.of(20L, "y")}),
            Row.of(2L, new Row[] {Row.of(30L, "z")}),
            Row.of(3L, new Row[] {Row.of(40L, "p"), null})); // null ROW element → all-null fields
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column(
                "rs",
                DataTypes.ARRAY(
                    DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.BIGINT()),
                        DataTypes.FIELD("b", DataTypes.STRING()))))
            .build());
    return tEnv;
  }

  private static TableEnvironment stringArrayEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "ss"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING)),
            Row.of(1L, new String[] {"a", "b"}),
            Row.of(2L, new String[] {"c"}));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("ss", DataTypes.ARRAY(DataTypes.STRING()))
            .build());
    return tEnv;
  }
}
