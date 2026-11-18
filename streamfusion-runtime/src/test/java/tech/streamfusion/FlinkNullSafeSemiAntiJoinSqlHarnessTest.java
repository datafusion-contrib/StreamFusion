package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNullSafeSemiAntiJoinSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"s", "i", "l", "d", "ts"})
  void nullSafeKeysAndResidualsMatchAcrossTypes(String key) throws Exception {
    for (boolean anti : new boolean[] {false, true}) {
      for (boolean residual : new boolean[] {false, true}) {
        String sql = "SELECT a.s, a.i, a.v FROM A a WHERE " + (anti ? "NOT " : "")
            + "EXISTS (SELECT 1 FROM B b WHERE a." + key + " IS NOT DISTINCT FROM b." + key
            + (residual ? " AND a.v < b.v" : "") + ")";
        assertNative(sql, false);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mixedCompositeKeysPreserveOrdinaryNullFiltering(boolean anti) throws Exception {
    String sql = "SELECT a.s, a.i, a.v FROM A a WHERE " + (anti ? "NOT " : "")
        + "EXISTS (SELECT 1 FROM B b WHERE (a.s = b.s OR (a.s IS NULL AND b.s IS NULL))"
        + " AND a.i = b.i AND a.v < b.v)";
    assertNative(sql, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void changelogSourcesPreserveDuplicateMatchCountsAndDeletes(boolean anti) throws Exception {
    assertNative(
        "SELECT a.s, a.i, a.v FROM A a WHERE "
            + (anti ? "NOT " : "")
            + "EXISTS (SELECT 1 FROM B b WHERE a.s IS NOT DISTINCT FROM b.s "
            + "AND a.d IS NOT DISTINCT FROM b.d AND a.ts IS NOT DISTINCT FROM b.ts AND a.v < b.v)",
        true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"INNER", "LEFT", "RIGHT", "FULL"})
  void ordinaryJoinsAlsoPreserveMixedNullSafeKeys(String kind) throws Exception {
    assertNative("SELECT a.s, a.i, a.v, b.s, b.i, b.v FROM A a " + kind
        + " JOIN B b ON a.s IS NOT DISTINCT FROM b.s AND a.i = b.i AND a.v < b.v", false);
    assertNative("SELECT a.s, a.i, a.v, b.s, b.i, b.v FROM A a " + kind
        + " JOIN B b ON a.s IS NOT DISTINCT FROM b.s", true);
  }

  private static void assertNative(String sql, boolean changes) throws Exception {
    String plan = NativePlanner.explain(environment(changes), sql);
    assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
    NativeParity.assertChangelogParity(() -> environment(changes), sql);
  }

  private static TableEnvironment environment(boolean changes) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    var type = Types.ROW_NAMED(new String[] {"s", "i", "l", "d", "ts", "v"},
        Types.STRING, Types.INT, Types.LONG, Types.BIG_DEC, Types.LOCAL_DATE_TIME, Types.INT);
    var schema = Schema.newBuilder().column("s", DataTypes.STRING()).column("i", DataTypes.INT())
        .column("l", DataTypes.BIGINT()).column("d", DataTypes.DECIMAL(12, 2))
        .column("ts", DataTypes.TIMESTAMP(9)).column("v", DataTypes.INT()).build();
    Row[] left = {
      row(null, null, 1), row(null, 1, 2), row("a", 1, 3), row("a", 1, 3), row("missing", 3, 4)
    };
    Row[] right =
        changes
            ? new Row[] {
              row(null, null, 8),
              row(null, null, 8),
              row("a", 1, 9),
              change(RowKind.DELETE, null, null, 8),
              change(RowKind.UPDATE_BEFORE, "a", 1, 9),
              change(RowKind.UPDATE_AFTER, "a", 1, 1),
              row("right", 4, 9)
            }
            : new Row[] {
              row(null, null, 8),
              row(null, 1, 8),
              row("a", 1, 9),
              row("a", 1, 9),
              row("right", 4, 9)
            };
    table.createTemporaryView("A", table.fromChangelogStream(fromData(env, type, left), schema));
    table.createTemporaryView("B", table.fromChangelogStream(fromData(env, type, right), schema));
    return table;
  }

  private static Row row(String text, Integer key, int value) {
    return change(RowKind.INSERT, text, key, value);
  }

  private static Row change(RowKind kind, String text, Integer key, int value) {
    return Row.ofKind(kind, text, key, key == null ? null : key.longValue(),
        key == null ? null : BigDecimal.valueOf(key).setScale(2),
        key == null ? null : LocalDateTime.of(2026, 1, 1, 0, 0, 0, key), value);
  }
}
