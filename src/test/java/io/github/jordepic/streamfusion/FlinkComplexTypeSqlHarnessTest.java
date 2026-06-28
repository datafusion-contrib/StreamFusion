package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * A filter/projection that passes a complex (ARRAY/MAP/ROW) column through matches the host. The
 * native filter only evaluates its (scalar) predicate and forwards the rest of the row, so a nested
 * column rides through the Arrow round-trip ({@code RowDataToArrow} → native filter → {@code
 * ArrowToRowData}) unchanged — the same nested types {@code ArrowConversion} already carries on the
 * Kafka decode path. (Comparison normalizes arrays to lists, since Java array equality is identity.)
 */
class FlinkComplexTypeSqlHarnessTest {

  @Test
  void arrayColumnPassesThroughFilter() throws Exception {
    assertComplexParity(
        FlinkComplexTypeSqlHarnessTest::arrayEnvironment, "SELECT k, arr FROM t WHERE k > 1");
  }

  @Test
  void arrayColumnWithComputedScalarMatchesHost() throws Exception {
    // A computed scalar projection alongside the passed-through array column.
    assertComplexParity(
        FlinkComplexTypeSqlHarnessTest::arrayEnvironment, "SELECT k + 1, arr FROM t WHERE k > 1");
  }

  @Test
  void mapColumnPassesThroughFilter() throws Exception {
    assertComplexParity(
        FlinkComplexTypeSqlHarnessTest::mapEnvironment, "SELECT k, m FROM t WHERE k > 1");
  }

  @Test
  void rowColumnPassesThroughFilter() throws Exception {
    assertComplexParity(
        FlinkComplexTypeSqlHarnessTest::rowEnvironment, "SELECT k, r FROM t WHERE k > 1");
  }

  @Test
  void groupByArrayKeyMatchesHost() throws Exception {
    // GROUP BY an ARRAY key: rows with an equal array group together (a nested-ScalarValue GroupKey),
    // and the array key is rebuilt on emit (cast back to its declared column type).
    assertComplexChangelogParity(
        FlinkComplexTypeSqlHarnessTest::repeatedArrayEnvironment,
        "SELECT arr, COUNT(*) FROM t GROUP BY arr");
  }

  @Test
  void countOfArrayColumnMatchesHost() throws Exception {
    // COUNT(array) — counts non-null without reading a typed value (the value column is read for
    // null-ness only), matching Flink. MAX(array) still falls back (Flink rejects ordering arrays).
    assertComplexChangelogParity(
        FlinkComplexTypeSqlHarnessTest::repeatedArrayEnvironment,
        "SELECT k, COUNT(arr) FROM t GROUP BY k");
  }

  @Test
  void joinOnArrayKeyMatchesHost() throws Exception {
    // Equi-join on an ARRAY key: rows match when their arrays are equal (nested-ScalarValue join key).
    assertComplexChangelogParity(
        FlinkComplexTypeSqlHarnessTest::repeatedArrayEnvironment,
        "SELECT a.k, b.k FROM t a JOIN t b ON a.arr = b.arr");
  }

  @Test
  void groupByRowKeyMatchesHost() throws Exception {
    // GROUP BY a ROW key — a nested-Struct GroupKey.
    assertComplexChangelogParity(
        FlinkComplexTypeSqlHarnessTest::rowEnvironment, "SELECT r, COUNT(*) FROM t GROUP BY r");
  }

  /** Runs host vs native, normalizing nested arrays/rows/maps so content (not identity) is compared. */
  private static void assertComplexParity(Supplier<TableEnvironment> environment, String sql)
      throws Exception {
    List<List<Object>> host = collect(environment.get(), sql);

    TableEnvironment nativeEnv = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnv);
    List<List<Object>> nativeRows = collect(nativeEnv, sql);

    assertTrue(scan.substitutions() > 0, "query did not route to native; parity check is moot");
    assertEquals(sorted(host), sorted(nativeRows), "native result differs from host");
  }

  /** Like {@link #assertComplexParity} but compares the collapsed changelog (for GROUP BY / join). */
  private static void assertComplexChangelogParity(
      Supplier<TableEnvironment> environment, String sql) throws Exception {
    Map<List<Object>, Long> host = collapsed(environment.get(), sql);

    TableEnvironment nativeEnv = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnv);
    Map<List<Object>, Long> nativeRows = collapsed(nativeEnv, sql);

    assertTrue(scan.substitutions() > 0, "query did not route to native; parity check is moot");
    assertEquals(host, nativeRows, "collapsed changelog differs from host");
  }

  private static Map<List<Object>, Long> collapsed(TableEnvironment environment, String sql)
      throws Exception {
    Map<List<Object>, Long> counts = new java.util.HashMap<>();
    try (CloseableIterator<Row> it = environment.executeSql(sql).collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(normalize(row.getField(i)));
        }
        long delta =
            row.getKind() == org.apache.flink.types.RowKind.DELETE
                    || row.getKind() == org.apache.flink.types.RowKind.UPDATE_BEFORE
                ? -1
                : 1;
        long next = counts.getOrDefault(fields, 0L) + delta;
        if (next == 0) {
          counts.remove(fields);
        } else {
          counts.put(fields, next);
        }
      }
    }
    return counts;
  }

  private static List<List<Object>> collect(TableEnvironment environment, String sql)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = environment.executeSql(sql).collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(normalize(row.getField(i)));
        }
        rows.add(fields);
      }
    }
    return rows;
  }

  /** Recursively turns arrays into lists and Rows/Maps into comparable structures. */
  private static Object normalize(Object value) {
    if (value == null) {
      return null;
    }
    if (value.getClass().isArray()) {
      int n = Array.getLength(value);
      List<Object> list = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        list.add(normalize(Array.get(value, i)));
      }
      return list;
    }
    if (value instanceof Row) {
      Row row = (Row) value;
      List<Object> list = new ArrayList<>(row.getArity());
      for (int i = 0; i < row.getArity(); i++) {
        list.add(normalize(row.getField(i)));
      }
      return list;
    }
    if (value instanceof Map) {
      Map<Object, Object> out = new TreeMap<>(Comparator.comparing(String::valueOf));
      ((Map<?, ?>) value).forEach((k, v) -> out.put(normalize(k), normalize(v)));
      return out;
    }
    return value;
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private static TableEnvironment arrayEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "arr"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
            Row.of(1L, new Long[] {10L, 20L}),
            Row.of(2L, new Long[] {30L}),
            Row.of(3L, new Long[] {40L, 50L, 60L}));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("arr", DataTypes.ARRAY(DataTypes.BIGINT()))
            .build());
    return tEnv;
  }

  private static TableEnvironment repeatedArrayEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Two rows share the array [10,20] so grouping/joining by the array key is actually exercised.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "arr"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
            Row.of(1L, new Long[] {10L, 20L}),
            Row.of(2L, new Long[] {10L, 20L}),
            Row.of(3L, new Long[] {30L}));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("arr", DataTypes.ARRAY(DataTypes.BIGINT()))
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
            Row.of(1L, Map.of("a", 1L)),
            Row.of(2L, Map.of("b", 2L, "c", 3L)),
            Row.of(3L, Map.of("d", 4L)));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .build());
    return tEnv;
  }

  private static TableEnvironment rowEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "r"},
                Types.LONG,
                Types.ROW_NAMED(new String[] {"a", "b"}, Types.LONG, Types.STRING)),
            Row.of(1L, Row.of(10L, "x")),
            Row.of(2L, Row.of(20L, "y")),
            Row.of(3L, Row.of(30L, "z")));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column(
                "r",
                DataTypes.ROW(
                    DataTypes.FIELD("a", DataTypes.BIGINT()),
                    DataTypes.FIELD("b", DataTypes.STRING())))
            .build());
    return tEnv;
  }
}
