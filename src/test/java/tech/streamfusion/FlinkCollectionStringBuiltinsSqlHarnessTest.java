package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkCollectionStringBuiltinsSqlHarnessTest {
  @Test
  void regexArraysRetainCapturesNullElementsAndInvalidPatterns() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("REGEXP_EXTRACT_ALL");
    BuiltinFunctionParity.assertParity(this::regexEnvironment,
        "SELECT REGEXP_EXTRACT_ALL(s,p,n), REGEXP_EXTRACT_ALL(s,p), "
            + "REGEXP_EXTRACT_ALL(s,p,n)[1], CARDINALITY(REGEXP_EXTRACT_ALL(s,p,n)) FROM src");
    BuiltinFunctionParity.assertParity(this::regexEnvironment,
        "SELECT REGEXP_EXTRACT_ALL(s,p,n)[1] FROM src "
            + "WHERE CARDINALITY(REGEXP_EXTRACT_ALL(s,p,n)) > 0");
    BuiltinFunctionParity.assertParity(this::regexEnvironment,
        "SELECT REGEXP_EXTRACT_ALL(s,p,n) FROM src WHERE n = 99");
  }

  @Test
  void mapsRetainDuplicateKeysMissingValuesAndRegexDelimiters() throws Exception {
    BuiltinFunctionParity.assertParity(this::mapEnvironment,
        "SELECT STR_TO_MAP(s), STR_TO_MAP(s,p,q), STR_TO_MAP(s,p,q)['k1'], "
            + "CARDINALITY(STR_TO_MAP(s,p,q)), STR_TO_MAP(s,p,q)['missing'] FROM src");
    BuiltinFunctionParity.assertParity(this::mapEnvironment,
        "SELECT STR_TO_MAP(s,p,q) FROM src WHERE STR_TO_MAP(s,p,q)['k1'] IS NOT NULL");
    BuiltinFunctionParity.assertParity(this::mapEnvironment,
        "SELECT STR_TO_MAP(s,p,q) FROM src WHERE n = 99");
  }

  @Test
  void invalidMapDelimiterRetainsJavaRegexFailureAndEmptyBatchBehavior() throws Exception {
    java.util.function.Supplier<TableEnvironment> input = () -> BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("n", INT())),
        List.of(Row.of("k=v", "[", 0)));
    NativeFailureParity.run(input, "SELECT STR_TO_MAP(s,p,'=') FROM src")
        .assertFailure(java.util.regex.PatternSyntaxException.class, "Unclosed character class",
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(input, "SELECT STR_TO_MAP(s,p,'=') FROM src WHERE n = 99");
  }

  @Test
  void nativeSinksDoNotConsumeBinaryTransportAsUtf8() {
    var table = BuiltinFunctionParity.environment(ROW(FIELD("s", STRING())), List.of(Row.of("/w==")));
    table.executeSql("CREATE TABLE pq (v STRING) WITH "
        + "('connector'='filesystem', 'path'='/tmp/unused-base64-sink', 'format'='parquet')");
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql("INSERT INTO pq SELECT s FROM src");
    org.junit.jupiter.api.Assertions.assertTrue(scan.substitutions() > 0, scan::explainSummary);
    table.explainSql("INSERT INTO pq SELECT FROM_BASE64(s) FROM src");
    org.junit.jupiter.api.Assertions.assertEquals(0, scan.substitutions());
    org.junit.jupiter.api.Assertions.assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("requires a row sink")),
        scan::explainSummary);
  }

  @Test
  void existingStringMapInputsSurviveWholeCalcEvaluation() throws Exception {
    java.util.Map<String, String> values = new java.util.HashMap<>();
    values.put("k", "value");
    values.put("null", null);
    values.put(null, "null key");
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("m", MAP(STRING(), STRING()))),
        List.of(Row.of("k=v", values), Row.of("", java.util.Map.of()), Row.of(null, null))),
        "SELECT STR_TO_MAP(s), m, m['k'] FROM src");
  }

  @Test
  void utf16ConsumersStayWithinFlinkEvaluation() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("REGEXP_EXTRACT_ALL");
    BuiltinFunctionParity.assertParity(this::mapEnvironment,
        "SELECT STR_TO_MAP(s,'','')['?'], "
            + "REGEXP_EXTRACT_ALL(s,p,0)[1] = '?', "
            + "CASE WHEN STR_TO_MAP(s,p,q)['k1'] = '?' THEN 1 ELSE 0 END FROM src");
  }

  private TableEnvironment regexEnvironment() {
    List<Row> samples = List.of(Row.of("abcdeabde", "ab((c)|(.?))de", 2),
        Row.of("100-200, 300-400", "(\\d+)-(\\d+)", 1),
        Row.of("abc", "", 0), Row.of("abc", "z+", 0),
        Row.of("abc", "(", 0), Row.of("abc", "(abc)", -1),
        Row.of("abc", "(abc)", 2), Row.of("abc", "(?<=a)(b)", 1),
        Row.of("abab", "(ab)\\1", 1), Row.of(null, "a", 0),
        Row.of("abc", null, 0), Row.of("abc", "(a)", null),
        Row.of("\ud83d\ude00", ".", 0));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("n", INT())), repeat(samples));
  }

  private TableEnvironment mapEnvironment() {
    List<Row> samples = List.of(Row.of("k1=v1,k2=v2,k1=last", ",", "=", 0),
        Row.of("k1:v1;k2: v2", ";", ":", 1),
        Row.of("k1$$v1|k2$$ v2", "\\|", "\\$\\$", 2),
        Row.of("k1=,k2,=v,", ",", "=", 3), Row.of("", ",", "=", 4),
        Row.of("\ud83d\ude00", "", "", 5), Row.of(null, ",", "=", 6),
        Row.of("k1=v1", null, "=", 7), Row.of("k1=v1", ",", null, 8));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("q", STRING()), FIELD("n", INT())),
        repeat(samples));
  }

  private static List<Row> repeat(List<Row> samples) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return rows;
  }
}
