package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTypedDistinctSqlHarnessTest {
  private static final String COUNTS =
      "SELECT k, COUNT(DISTINCT i), COUNT(DISTINCT tiny), COUNT(DISTINCT small),"
          + " COUNT(DISTINCT big), COUNT(DISTINCT d), COUNT(DISTINCT s),"
          + " COUNT(DISTINCT s) FILTER (WHERE keep),"
          + " COUNT(DISTINCT d) FILTER (WHERE NOT keep), COUNT(*) FROM src GROUP BY k";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void typedCountsKeepMultiplicityFiltersAndGroupLifecycle(boolean retract) throws Exception {
    compare(() -> environment(retract, false), COUNTS, false, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void integerDistinctSumsKeepWidthAndRetraction(boolean retract) throws Exception {
    compare(
        () -> environment(retract, false),
        "SELECT k, SUM(DISTINCT i), SUM(DISTINCT tiny), SUM(DISTINCT small),"
            + " SUM(DISTINCT big), SUM(DISTINCT i) FILTER (WHERE keep) FROM src GROUP BY k",
        false,
        true);
  }

  @Test
  void localAndGlobalCountsRetainTypedViews() throws Exception {
    compare(() -> environment(false, true), COUNTS, true, false);
  }

  private static void compare(
      Supplier<TableEnvironment> source, String sql, boolean local, boolean ordered)
      throws Exception {
    var hostTypes = source.get().sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    var nativeEnvironment = source.get();
    NativePlanner.install(nativeEnvironment);
    assertEquals(hostTypes, nativeEnvironment.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    if (local) assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    if (ordered) NativeParity.assertOrderedKindedParity(source, sql);
    else NativeParity.assertChangelogParity(source, sql);
  }

  private static Row row(RowKind kind, int key, Integer value, String text, Boolean keep) {
    return Row.ofKind(
        kind,
        key,
        value,
        value == null ? null : value.byteValue(),
        value == null ? null : value.shortValue(),
        value == null ? null : value.longValue() * 1_000_000_000L,
        value == null ? null : BigDecimal.valueOf(value % 32768).movePointRight(12).setScale(2),
        text,
        keep);
  }

  private static TableEnvironment environment(boolean retract, boolean local) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", local ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(local));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    String longString = "a\0中😀".repeat(80);
    List<Row> rows =
        new ArrayList<>(
            List.of(
                row(RowKind.INSERT, 1, 127, longString, true),
                row(RowKind.INSERT, 1, 127, longString, false),
                row(RowKind.INSERT, 1, 126, "é", true),
                row(RowKind.INSERT, 1, -128, "e\u0301", false),
                row(RowKind.INSERT, 1, null, null, null),
                row(RowKind.INSERT, 2, null, null, true),
                row(RowKind.INSERT, 3, 32767, "", true),
                row(RowKind.INSERT, 3, 1, "short-overflow", false),
                row(RowKind.INSERT, 4, Integer.MAX_VALUE, "int-overflow", true),
                row(RowKind.INSERT, 4, 1, "one", true),
                row(RowKind.INSERT, 4, Integer.MAX_VALUE, "int-overflow", false)));
    if (retract)
      rows.addAll(
          List.of(
              row(RowKind.DELETE, 1, 127, longString, true),
              row(RowKind.DELETE, 1, 127, longString, false),
              row(RowKind.UPDATE_BEFORE, 1, 126, "é", true),
              row(RowKind.UPDATE_AFTER, 1, 127, longString, true),
              row(RowKind.DELETE, 1, -128, "e\u0301", false),
              row(RowKind.DELETE, 1, 127, longString, true),
              row(RowKind.DELETE, 1, null, null, null),
              row(RowKind.DELETE, 2, null, null, true),
              row(RowKind.INSERT, 1, -128, longString, false)));
    var stream =
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"k", "i", "tiny", "small", "big", "d", "s", "keep"},
                Types.INT,
                Types.INT,
                Types.BYTE,
                Types.SHORT,
                Types.LONG,
                Types.BIG_DEC,
                Types.STRING,
                Types.BOOLEAN),
            rows.toArray(Row[]::new));
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("i", DataTypes.INT())
            .column("tiny", DataTypes.TINYINT())
            .column("small", DataTypes.SMALLINT())
            .column("big", DataTypes.BIGINT())
            .column("d", DataTypes.DECIMAL(20, 2))
            .column("s", DataTypes.STRING())
            .column("keep", DataTypes.BOOLEAN())
            .build();
    table.createTemporaryView(
        "src",
        retract ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
    return table;
  }
}
