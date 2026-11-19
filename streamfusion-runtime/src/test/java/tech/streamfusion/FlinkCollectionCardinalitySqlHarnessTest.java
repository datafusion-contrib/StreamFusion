package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCollectionCardinalitySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"CARDINALITY(a)", "CARDINALITY(m)"})
  void collectionExtensionCanBeDisabled(String expression) throws Exception {
    NativeParity.assertFallback(
        () -> {
          var table = environment();
          table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
          return table;
        },
        "SELECT " + expression + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CARDINALITY(a)",
        "CARDINALITY(nested)",
        "CARDINALITY(m)",
        "CARDINALITY(a) + CARDINALITY(m)",
        "CASE WHEN id = 0 THEN CARDINALITY(nested) ELSE CARDINALITY(a) END",
        "COALESCE(CARDINALITY(a), -1)"
      })
  void countsOuterElementsAndMapEntries(String expression) throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkCollectionCardinalitySqlHarnessTest::environment,
        "SELECT id, " + expression + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CARDINALITY(a) > 0",
        "CARDINALITY(nested) = 2",
        "CARDINALITY(m) IS NULL",
        "id < 0"
      })
  void filtersAndEmptyBatchesPreserveNullSemantics(String predicate) throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkCollectionCardinalitySqlHarnessTest::environment,
        "SELECT id, CARDINALITY(a), CARDINALITY(nested), CARDINALITY(m) FROM src WHERE "
            + predicate);
  }

  private static TableEnvironment environment() {
    var map = new LinkedHashMap<String, Integer[]>();
    map.put("one", new Integer[] {1, 2, 3});
    map.put("two", null);
    var table =
        BuiltinFunctionParity.environment(
            ROW(
                FIELD("id", INT()),
                FIELD("a", ARRAY(INT())),
                FIELD("nested", ARRAY(ARRAY(INT()))),
                FIELD("m", MAP(STRING().notNull(), ARRAY(INT())))),
            List.of(
                Row.of(0, new Integer[] {1, null, 3}, new Integer[][] {{1, 2}, {3, null, 5}}, map),
                Row.of(1, new Integer[] {}, new Integer[][] {}, Map.of()),
                Row.of(2, null, null, null),
                Row.of(3, new Integer[] {null}, new Integer[][] {null, {}}, map)));
    return table;
  }
}
