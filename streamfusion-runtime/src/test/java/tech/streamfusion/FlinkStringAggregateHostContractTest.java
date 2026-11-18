package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Released host contracts that the native string aggregate states must preserve. */
class FlinkStringAggregateHostContractTest {
  @Test
  void separatorMustBeANonNullLiteral() {
    for (String separator : List.of("sep", "NULL", "CAST(NULL AS STRING)")) {
      var failure =
          assertThrows(
              org.apache.flink.table.api.ValidationException.class,
              () -> run(false, "SELECT LISTAGG(v, " + separator + ") FROM src", row("a", ",")));
      assertTrue(failure.getMessage().contains("SQL validation failed"), failure.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void literalSeparatorsPreserveArrivalOrderAndDuplicates(boolean changelog) throws Exception {
    for (String separator : List.of("", ",", "::", "é🚀", "'")) {
      String literal = "'" + separator.replace("'", "''") + "'";
      assertEquals(
          String.join(separator, "a", "b", "a"),
          latest(
              run(
                  changelog,
                  "SELECT LISTAGG(v, " + literal + ") FROM src",
                  row("a", ","),
                  row(null, null),
                  row("b", ";"),
                  row("a", ":"))));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filterSkipsUnselectedAndNullValues(boolean changelog) throws Exception {
    var omitted = row("b", null);
    omitted.setField(4, false);
    var rows =
        run(
            changelog,
            "SELECT LISTAGG(v, ';') FILTER (WHERE keep_row) FROM src",
            row("a", "|"),
            omitted,
            row(null, ":"),
            row("c", ";"));
    assertEquals("a;c", latest(rows));
    assertEquals(List.of("+I[a]", "-U[a]", "+U[a;c]"), strings(rows));
  }

  @Test
  void retractionRemovesTheFirstDuplicate() throws Exception {
    var removed = row("a", "/");
    removed.setKind(RowKind.DELETE);
    assertEquals(
        "b/a",
        latest(
            run(
                true,
                "SELECT LISTAGG(v, '/') FROM src",
                row("a", ":"),
                row("b", ";"),
                row("a", ":"),
                removed)));
  }

  @Test
  void jsonAggregatesRetractTheDepartingRow() throws Exception {
    var first = row("a", ",");
    first.setField(2, "a");
    var second = row("b", ",");
    second.setField(2, "b");
    second.setField(3, 2L);
    var deleted = Row.ofKind(RowKind.DELETE, "a", ",", "a", 1L, true);
    assertEquals(
        "[\"b\"]", latest(run(true, "SELECT JSON_ARRAYAGG(v) FROM src", first, second, deleted)));
    assertEquals(
        "{\"b\":2}",
        latest(
            run(true, "SELECT JSON_OBJECTAGG(json_key VALUE n) FROM src", first, second, deleted)));
  }

  @Test
  void defaultSeparatorSkipsNullsAndPreservesDuplicates() throws Exception {
    assertEquals(
        "a,a,b",
        latest(
            run(
                false,
                "SELECT LISTAGG(v) FROM src",
                row("a", ";"),
                row(null, null),
                row("a", ":"),
                row("b", "|"))));
  }

  @Test
  void jsonArrayNullClausesDuplicatesAndEscaping() throws Exception {
    Row[] input = {row("a", ","), row(null, ","), row("a", ","), row("\"\\\n🚀", ",")};
    assertEquals(
        "[\"a\",\"a\",\"\\\"\\\\\\n🚀\"]",
        latest(run(false, "SELECT JSON_ARRAYAGG(v) FROM src", input)));
    assertEquals(
        "[\"a\",null,\"a\",\"\\\"\\\\\\n🚀\"]",
        latest(run(false, "SELECT JSON_ARRAYAGG(v NULL ON NULL) FROM src", input)));
  }

  @Test
  void jsonObjectEscapesItsKeyAndPreservesNullClause() throws Exception {
    var input = row("unused", ",");
    input.setField(2, "\"\\\n");
    input.setField(3, null);
    assertEquals(
        "{\"\\\"\\\\\\n\":null}",
        latest(run(false, "SELECT JSON_OBJECTAGG(json_key VALUE n NULL ON NULL) FROM src", input)));
    assertEquals(
        "{}",
        latest(
            run(false, "SELECT JSON_OBJECTAGG(json_key VALUE n ABSENT ON NULL) FROM src", input)));
  }

  @Test
  void duplicateObjectKeyFailsBeforeAbsentNullHandling() {
    var first = row("unused", ",");
    var duplicate = row("unused", ",");
    duplicate.setField(3, null);
    var failure =
        assertThrows(
            Exception.class,
            () ->
                run(
                    false,
                    "SELECT JSON_OBJECTAGG(json_key VALUE n ABSENT ON NULL) FROM src",
                    first,
                    duplicate));
    boolean duplicateMessage = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      duplicateMessage |=
          String.valueOf(cause.getMessage())
              .contains("Duplicate keys are not allowed in JSON_OBJECTAGG");
    }
    assertTrue(duplicateMessage, failure.toString());
  }

  @Test
  void emptyStreamingInputEmitsNoAggregateRow() throws Exception {
    for (String expression :
        List.of("LISTAGG(v, ',')", "JSON_ARRAYAGG(v)", "JSON_OBJECTAGG(json_key VALUE n)")) {
      assertEquals(List.of(), run(false, "SELECT " + expression + " FROM src"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void singletonStringRepresentationAtTheAggregateBoundary(boolean changelog) throws Exception {
    var before = environment(changelog, row(new String(new char[] {'\ud800'}), ","));
    before.createTemporarySystemFunction("cache_tag", new CacheTag());
    assertEquals(true, latest(collect(before.sqlQuery("SELECT cache_tag(v) FROM src"))));
    for (boolean objectReuse : new boolean[] {false, true}) {
      for (String grouping : List.of("", " GROUP BY json_key")) {
        var table =
            environment(changelog, objectReuse, row(new String(new char[] {'\ud800'}), ","));
        table.createTemporarySystemFunction("cache_tag", new CacheTag());
        var rows = collect(table.sqlQuery("SELECT cache_tag(LISTAGG(v, ',')) FROM src" + grouping));
        assertEquals(
            false, latest(rows), "changelog=" + changelog + ", reuse=" + objectReuse + grouping);
      }
    }
  }

  @Test
  void jsonObjectKeysUseJavaUtf16Order() throws Exception {
    List<Row> input = new ArrayList<>();
    for (String key : List.of("\ue000", "🚀", "z", "a")) {
      var row = row("unused", ",");
      row.setField(2, key);
      input.add(row);
    }
    assertEquals(
        "{\"a\":1,\"z\":1,\"🚀\":1,\"\ue000\":1}",
        latest(
            run(
                false,
                "SELECT JSON_OBJECTAGG(json_key VALUE n) FROM src",
                input.toArray(new Row[0]))));
  }

  @Test
  void jsonArraysPreserveTypedScalarSerialization() throws Exception {
    var first = row("a", ",");
    var second = row("b", ",");
    second.setField(3, 2L);
    second.setField(4, false);
    var absent = row(null, ",");
    absent.setField(3, null);
    absent.setField(4, null);
    assertEquals(
        "[1,null,2]",
        latest(run(false, "SELECT JSON_ARRAYAGG(n NULL ON NULL) FROM src", first, absent, second)));
    assertEquals(
        legacy() ? "[1,2]" : "[1.00,2.00]",
        latest(
            run(false, "SELECT JSON_ARRAYAGG(CAST(n AS DECIMAL(20,2))) FROM src", first, second)));
    assertEquals(
        "[true,false]",
        latest(run(false, "SELECT JSON_ARRAYAGG(keep_row) FROM src", first, absent, second)));
  }

  @Test
  void jsonAggregateFilterSyntaxIsRejected() {
    for (String expression : List.of("JSON_OBJECTAGG(json_key VALUE n)", "JSON_ARRAYAGG(v)")) {
      assertThrows(
          org.apache.flink.table.api.SqlParserException.class,
          () ->
              run(
                  false,
                  "SELECT " + expression + " FILTER (WHERE keep_row) FROM src",
                  row("unused", ",")));
    }
  }

  @Test
  void jsonObjectNullKeyUsesTheReleasedHostContract() throws Exception {
    var invalid = row("unused", ",");
    invalid.setField(2, null);
    var failure =
        assertThrows(
            Exception.class,
            () -> run(false, "SELECT JSON_OBJECTAGG(json_key VALUE n) FROM src", invalid));
    assertCause(
        failure,
        legacy()
            ? "org.apache.flink.table.api.TableException"
            : "org.apache.flink.table.api.TableRuntimeException",
        legacy()
            ? "The accumulator state could not be serialized."
            : "Map views don't support null keys.");
    invalid.setField(3, null);
    String absent = "SELECT JSON_OBJECTAGG(json_key VALUE n ABSENT ON NULL) FROM src";
    if (legacy()) {
      assertEquals("{}", latest(run(false, absent, invalid)));
    } else {
      var absentFailure = assertThrows(Exception.class, () -> run(false, absent, invalid));
      assertCause(
          absentFailure,
          "org.apache.flink.table.api.TableRuntimeException",
          "Map views don't support null keys.");
    }
  }

  private static boolean legacy() {
    return org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18");
  }

  private static void assertCause(Throwable failure, String type, String message) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause.getClass().getName().equals(type) && message.equals(cause.getMessage())) return;
    }
    fail("Missing " + type + ": " + message, failure);
  }

  public static class CacheTag extends ScalarFunction {
    public Boolean eval(String value) {
      return value != null && !value.isEmpty() && Character.isHighSurrogate(value.charAt(0));
    }
  }

  private static Row row(String value, String separator) {
    return Row.of(value, separator, "key", 1L, true);
  }

  static StreamTableEnvironment environment(boolean changelog, Row... input) {
    return environment(changelog, false, input);
  }

  static StreamTableEnvironment environment(boolean changelog, boolean objectReuse, Row... input) {
    var environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    if (objectReuse) environment.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(environment);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    org.apache.flink.streaming.api.datastream.DataStream<Row> source =
        fromData(
            environment,
            Types.ROW_NAMED(
                new String[] {"v", "sep", "json_key", "n", "keep_row"},
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.LONG,
                Types.BOOLEAN),
            input.length == 0 ? new Row[] {row(null, null)} : input);
    if (input.length == 0) source = source.filter(ignored -> false);
    var schema =
        Schema.newBuilder()
            .column("v", DataTypes.STRING())
            .column("sep", DataTypes.STRING())
            .column("json_key", DataTypes.STRING())
            .column("n", DataTypes.BIGINT())
            .column("keep_row", DataTypes.BOOLEAN())
            .build();
    table.createTemporaryView(
        "src",
        changelog
            ? table.fromChangelogStream(source, schema)
            : table.fromDataStream(source, schema));
    return table;
  }

  private static List<Row> run(boolean changelog, String sql, Row... input) throws Exception {
    var table = environment(changelog, input);
    var query = table.sqlQuery(sql);
    var type = query.getResolvedSchema().getColumnDataTypes().get(0).getLogicalType();
    assertEquals(LogicalTypeRoot.VARCHAR, type.getTypeRoot());
    boolean json = sql.contains("JSON_");
    assertEquals(!json, type.isNullable(), type.asSummaryString());
    assertEquals(json ? 2000 : VarCharType.MAX_LENGTH, ((VarCharType) type).getLength());
    return collect(query);
  }

  private static List<Row> collect(Table query) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (var iterator = query.execute().collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        rows.add(Row.ofKind(row.getKind(), row.getField(0)));
      }
    }
    return rows;
  }

  private static Object latest(List<Row> rows) {
    assertFalse(rows.isEmpty());
    Row last = rows.get(rows.size() - 1);
    assertTrue(
        last.getKind() == RowKind.INSERT || last.getKind() == RowKind.UPDATE_AFTER,
        last.toString());
    return last.getField(0);
  }

  private static List<String> strings(List<Row> rows) {
    var strings = new ArrayList<String>();
    for (Row row : rows) strings.add(row.toString());
    return strings;
  }
}
