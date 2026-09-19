package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
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

class FlinkMapJsonJvmSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_STRING(m)",
        "JSON_STRING(ms)",
        "JSON_STRING(nested)",
        "m, ms, JSON_QUERY(s, '$.items[*]')",
        "ROW(m, nested, JSON_QUERY(s, '$.items[*]'))",
        "ARRAY[m, CAST(NULL AS MAP<STRING NOT NULL, STRING>)]",
        "MAP['first', JSON_QUERY(s, '$.items[*]'), 'second', JSON_STRING(m)]",
        "JSON_OBJECT('map' VALUE m, 'bag' VALUE ms)",
        "JSON_ARRAY(m, nested, ms)",
        "JSON_STRING(m['a'])",
        "CASE WHEN id = 2 THEN CAST(NULL AS MAP<STRING NOT NULL, STRING>) ELSE m END, JSON_QUERY(s,"
            + " '$')"
      })
  void mapsAndMultisetsUseTheGeneratedBatchBridge(String projection) throws Exception {
    // The extra dynamic query keeps even a plain MAP result on the complete Calc bridge.
    String sql = "SELECT id, " + projection + ", JSON_QUERY(s, '$.items[*]') FROM inputs";
    String plan = NativePlanner.explain(environment(false), sql);
    assertTrue(plan.contains("NativeCalc") && plan.contains("jsonEvaluation=[JVM]"), plan);
    NativeParity.assertParity(() -> environment(false), sql);
  }

  @Test
  void borrowedMapsPreserveChangelogKindsAndFiltering() throws Exception {
    NativeParity.assertKindedParity(
        () -> environment(true),
        "SELECT id, m, nested, ms, JSON_STRING(m), JSON_QUERY(s, '$') FROM inputs WHERE id <> 2");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"JSON_STRING(ms)", "ROW(ms), JSON_QUERY(s, '$')", "ARRAY[ms], JSON_QUERY(s, '$')"})
  void nullableMultisetElementsStayOnFlink(String projection) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(false, true), "SELECT " + projection + " FROM inputs", "row-fused UDF");
  }

  @Test
  void integerKeysAndExactDecimalValuesMatchFlink() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          Map<Integer, java.math.BigDecimal> values = new LinkedHashMap<>();
          values.put(1, new java.math.BigDecimal("12345678901234567890.123456789"));
          values.put(-2, null);
          table.createTemporaryView(
              "inputs",
              env.fromData(
                  List.of(Row.of(values, "{}"), Row.of(Map.of(), "[]"), Row.of(null, null)),
                  Types.ROW_NAMED(
                      new String[] {"m", "s"}, Types.MAP(Types.INT, Types.BIG_DEC), Types.STRING)),
              Schema.newBuilder()
                  .column("m", DataTypes.MAP(DataTypes.INT().notNull(), DataTypes.DECIMAL(38, 9)))
                  .column("s", DataTypes.STRING())
                  .build());
          return table;
        },
        "SELECT JSON_STRING(m[1]), m, JSON_QUERY(s, '$') FROM inputs");
  }

  @Test
  void jsonMapFilterComposesWithGroupedAggregation() throws Exception {
    String sql =
        "SELECT MOD(id, 2) AS k, COUNT(*) AS n FROM inputs "
            + "WHERE CHAR_LENGTH(JSON_STRING(m)) > 0 GROUP BY MOD(id, 2)";
    String plan = NativePlanner.explain(environment(false), sql);
    assertTrue(
        plan.contains("jsonEvaluation=[JVM]") && plan.contains("NativeColumnarGroupAggregate"),
        plan);
    NativeParity.assertParity(() -> environment(false), sql);
  }

  private static TableEnvironment environment(boolean changelog) {
    return environment(changelog, false);
  }

  private static TableEnvironment environment(boolean changelog, boolean nullableMultisetElements) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Map<String, String> nullableValues = new LinkedHashMap<>();
    nullableValues.put("a", "snowman ☃");
    nullableValues.put("b", null);
    Map<String, String[]> nested = new LinkedHashMap<>();
    nested.put("values", new String[] {"x", null, "y"});
    nested.put("empty", new String[] {});
    nested.put("absent", null);
    Map<String, Integer> bag = new LinkedHashMap<>();
    bag.put("first", 2);
    bag.put("second", 3);
    List<Row> rows =
        new ArrayList<>(
            List.of(
                Row.of(1, nullableValues, nested, bag, "{\"items\":[1,2]}"),
                Row.of(2, Map.of(), Map.of(), Map.of(), "{\"items\":[]}"),
                Row.of(3, null, null, null, "broken"),
                Row.of(
                    4,
                    Map.of("last", "value"),
                    Map.of("last", new String[] {"z"}),
                    Map.of("last", 1),
                    null)));
    if (changelog) {
      Row before = Row.copy(rows.get(0));
      before.setKind(RowKind.UPDATE_BEFORE);
      rows.add(before);
      Row after = Row.copy(rows.get(0));
      after.setKind(RowKind.UPDATE_AFTER);
      after.setField(1, Map.of("updated", "new"));
      rows.add(after);
      Row deleted = Row.copy(rows.get(3));
      deleted.setKind(RowKind.DELETE);
      rows.add(deleted);
    }
    var input =
        env.fromData(
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "m", "nested", "ms", "s"},
                Types.INT,
                Types.MAP(Types.STRING, Types.STRING),
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)),
                new MultisetTypeInfo<>(Types.STRING),
                Types.STRING));
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("m", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING()))
            .column(
                "nested",
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.ARRAY(DataTypes.STRING())))
            .column(
                "ms",
                DataTypes.MULTISET(
                    nullableMultisetElements ? DataTypes.STRING() : DataTypes.STRING().notNull()))
            .column("s", DataTypes.STRING())
            .build();
    table.createTemporaryView(
        "inputs",
        changelog ? table.fromChangelogStream(input, schema) : table.fromDataStream(input, schema));
    return table;
  }
}
