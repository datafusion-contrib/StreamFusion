package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkNestedTableFunctionSqlHarnessTest {
  static Stream<String> types() {
    return Stream.of(
        "array_int",
        "array_bigint",
        "array_decimal",
        "array_string",
        "array_boolean",
        "array_tinyint",
        "array_smallint",
        "array_date",
        "array_time",
        "array_bytes",
        "array_nested",
        "array_float",
        "array_double",
        "array_timestamp",
        "array_ltz",
        "array_required",
        "array_fixed",
        "map",
        "int_map",
        "map_array",
        "map_row",
        "map_map",
        "bool_decimal",
        "float_double",
        "date_time",
        "decimal_timestamp",
        "row_int",
        "row_STRING",
        "row_DECIMAL(38,18)",
        "row_BINARY(4)",
        "row_TIMESTAMP(9)",
        "array_row",
        "array_map");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("types")
  void typedContainersRetainNullsValuesAndMutableFanout(String name) {
    for (String join :
        List.of(
            ", LATERAL TABLE(nested_rows(s.id, s.p)) AS t(v, pos)",
            " LEFT JOIN LATERAL TABLE(nested_rows(s.id, s.p)) AS t(v, pos) ON TRUE",
            ", LATERAL TABLE(nested_rows(s.id, s.p)) AS t(v, pos) WHERE t.pos=1 AND t.v IS NOT"
                + " NULL")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(name, 1031, metrics),
                "SELECT s.id, s.p, t.v, t.pos, t.v FROM src s" + join);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "array_int",
        "array_string",
        "array_nested",
        "map_array",
        "row_STRING",
        "array_row"
      })
  void retainedArgumentsAndChainedCallsKeepOwnedValues(String name) throws Exception {
    for (String sql :
        List.of(
            "SELECT s.id, t.v FROM src s, LATERAL TABLE(previous_rows(s.id, s.p)) AS t(v, pos)",
            "SELECT s.id, u.v FROM src s, LATERAL TABLE(nested_rows(s.id, s.p)) AS t(v, pos), "
                + "LATERAL TABLE(nested_rows(t.pos, t.v)) AS u(v, pos)")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        NativeParity.assertOrderedKindedParity(() -> environment(name, 1031, metrics), sql);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"array_int", "array_string", "array_nested", "map_row", "row_STRING", "array_row"})
  void allRowKindsAndRepeatedRecoveryKeepContainerValues(String name) throws Exception {
    DataType type = rowType(name);
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(Row.of(i, value(name, i)));
    for (int i = 0; i < 16; i++) {
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, i, value(name, i)));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, i, value(name, i + 2)));
      rows.add(Row.ofKind(RowKind.DELETE, i, value(name, i + 2)));
    }
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), name),
                () -> register(recovery.get(), name),
                "SELECT id,t.v,t.pos FROM recovery_input s LEFT JOIN LATERAL"
                    + " TABLE(nested_rows(s.id,s.p)) AS t(v,pos) ON TRUE");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
        SharedFlinkCluster.assertNativeMemoryReleased("nested-table-function-recovery");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array_string", "map_row", "row_STRING"})
  void unverifiedStatefulConsumersRetainCompleteQueryFallback(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(name, 96, metrics),
              "SELECT v FROM (SELECT t.v,ROW_NUMBER() OVER (PARTITION BY MOD(s.id,4) ORDER BY s.id"
                  + " DESC) AS rn FROM src s,LATERAL TABLE(nested_rows(s.id,s.p)) AS t(v,pos))"
                  + " WHERE rn<=2");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("collection representation")),
          runs::toString);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array_int", "array_nested", "array_row", "map_array"})
  void generatedUnnestAndScalarConsumersCompose(String name) {
    for (String sql :
        List.of(
            "SELECT s.id,u.* FROM src s,LATERAL TABLE(nested_rows(s.id,s.p)) AS t(v,pos),"
                + " UNNEST(t.v) AS u",
            "SELECT s.id,CARDINALITY(t.v) FROM src s,LATERAL TABLE(nested_rows(s.id,s.p)) AS"
                + " t(v,pos)",
            "SELECT COUNT(*) FROM src s,LATERAL TABLE(nested_rows(s.id,s.p)) AS t(v,pos)")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs = NativeFailureParity.run(() -> environment(name, 1031, metrics), sql);
        boolean fallback =
            name.equals("array_row") && sql.contains("COUNT(*)")
                || name.equals("map_array") && !sql.contains("UNNEST");
        runs.assertSuccess(
            fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
        if (fallback)
          assertTrue(
              runs.nativeRun().fallbackReasons().stream()
                  .anyMatch(reason -> reason.contains("collection representation")),
              runs::toString);
        else
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @Test
  void typedArrayMethodsCopyReusedResultBuffersBeforeReturning() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment("array_int", 1031, metrics);
                table.createTemporarySystemFunction("array_rows", new ArrayRows());
                return table;
              },
              "SELECT s.id,s.p,t.v,t.pos FROM src s LEFT JOIN LATERAL TABLE(array_rows(s.id,s.p))"
                  + " AS t(v,pos) ON TRUE");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array_string", "map", "row_STRING"})
  void unverifiedProjectedContainersRetainTheirCopyBoundary(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(name, 96, metrics, true),
              "SELECT s.id,s.p,t.v,t.pos FROM src s,LATERAL TABLE(nested_rows(s.id,s.p)) AS"
                  + " t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sourceProjectionRetainsTheAnnotatedExternalRowArgument(boolean updating) {
    for (boolean reordered : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var env = metrics.environment(new Configuration(), 1, 0);
                  var table = StreamTableEnvironment.create(env);
                  var type =
                      org.apache.flink.api.common.typeinfo.Types.ROW(
                          org.apache.flink.api.common.typeinfo.Types.INT,
                          org.apache.flink.api.common.typeinfo.Types.BOOLEAN,
                          org.apache.flink.api.common.typeinfo.Types.ROW(
                              org.apache.flink.api.common.typeinfo.Types.INT,
                              org.apache.flink.api.common.typeinfo.Types.INT,
                              org.apache.flink.api.common.typeinfo.Types.INT));
                  var rows = new java.util.ArrayList<Row>();
                  rows.add(Row.of(12, true, Row.of(1, 2, 3)));
                  rows.add(Row.of(13, false, Row.ofKind(RowKind.DELETE, 4, null, 6)));
                  if (updating) {
                    rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, 12, true, Row.of(1, 2, 3)));
                    rows.add(Row.ofKind(RowKind.UPDATE_AFTER, 12, true, Row.of(7, 8, 9)));
                    rows.add(Row.ofKind(RowKind.DELETE, 12, true, Row.of(7, 8, 9)));
                  }
                  var input = tech.streamfusion.compat.FlinkTestSources.fromData(env, rows, type);
                  var source =
                      (updating
                              ? table.fromChangelogStream(
                                  input,
                                  Schema.newBuilder().build(),
                                  org.apache.flink.table.connector.ChangelogMode.all())
                              : table.fromDataStream(input))
                          .as("a", "b", "c");
                  if (reordered)
                    source =
                        source.select(
                            org.apache.flink.table.api.Expressions.$("c"),
                            org.apache.flink.table.api.Expressions.$("a"));
                  table.createTemporaryView("src", source);
                  table.createTemporarySystemFunction("row_arguments", new RowArguments());
                  return table;
                },
                "SELECT s.c,"
                    + (reordered ? "s.a," : "")
                    + "t.f2 FROM src s,LATERAL TABLE(row_arguments(s.c)) AS t(f0,f1,f2)");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @org.apache.flink.table.annotation.DataTypeHint("ROW<a INT,b INT,c INT>")
  public static class RowArguments extends org.apache.flink.table.functions.TableFunction<Row> {
    public void eval(
        @org.apache.flink.table.annotation.DataTypeHint("ROW<a INT,b INT,c INT>") Row value) {
      collect(value);
    }
  }

  @Test
  void computedContainerArgumentsPreserveScalarInputStrings() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkTypedTableFunctionSqlHarnessTest.environment(
                        "STRING", "UTC", 1031, metrics);
                table.createTemporarySystemFunction("string_array_rows", new StringArrayRows());
                return table;
              },
              "SELECT id,t.h,t.n FROM src s,LATERAL TABLE(string_array_rows(ARRAY[s.v])) AS"
                  + " t(h,n)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<STRING>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ROW<h INT,n INT>"))
  public static class StringArrayRows extends org.apache.flink.table.functions.TableFunction<Row> {
    public void eval(String[] values) {
      String value = values[0];
      collect(
          Row.of(value == null ? null : value.hashCode(), value == null ? null : value.length()));
    }
  }

  @Test
  void internalContainerConversionsKeepExplicitFallback() {
    for (String name : List.of("array_int", "map", "row_STRING")) {
      Class<?> internal =
          name.equals("array_int")
              ? org.apache.flink.table.data.ArrayData.class
              : name.equals("map")
                  ? org.apache.flink.table.data.MapData.class
                  : org.apache.flink.table.data.RowData.class;
      for (Class<?> conversion : List.of(internal)) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var runs =
              NativeFailureParity.run(
                  () -> {
                    var table = environment(name, 32, metrics);
                    table.createTemporarySystemFunction(
                        "alternate_rows",
                        new FlinkTypedTableFunctionSqlHarnessTest.TypedRows(
                            valueType(name).bridgedTo(conversion)));
                    return table;
                  },
                  "SELECT t.v FROM src s, LATERAL TABLE(alternate_rows(s.id,s.p)) AS t(v,pos)");
          runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
          assertTrue(
              runs.nativeRun().fallbackReasons().stream()
                  .anyMatch(reason -> reason.contains("verified external Java conversions")),
              runs::toString);
        }
      }
    }
  }

  @Test
  void callbackFailuresRetainTheirCauseAndFilteredCallsStayUninvoked() {
    for (int count : new int[] {0, 32}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        NativeFailureParity.run(
                () -> environment("array_int", count, metrics),
                "SELECT t.v FROM (SELECT * FROM src WHERE id<0) s, LATERAL"
                    + " TABLE(failing_rows(s.id,s.p)) AS t(v,pos)")
            .assertSuccess(NativeFailureParity.Route.NATIVE);
      }
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeFailureParity.run(
              () -> environment("map", 32, metrics),
              "SELECT t.v FROM src s, LATERAL TABLE(failing_rows(s.id,s.p)) AS t(v,pos)")
          .assertFailure(
              IllegalStateException.class,
              "nested table sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  public static class PreviousRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private transient Object previous;

    public PreviousRows(DataType type) {
      super(type);
    }

    @Override
    public void eval(Integer mode, Object value) {
      super.eval(mode, previous);
      previous = value;
    }
  }

  public static class FailingRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    public FailingRows(DataType type) {
      super(type);
    }

    @Override
    public void eval(Integer mode, Object value) {
      if (mode == 2) throw new IllegalStateException("nested table sentinel");
      super.eval(mode, value);
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = {
        @org.apache.flink.table.annotation.DataTypeHint("INT"),
        @org.apache.flink.table.annotation.DataTypeHint("ARRAY<INT>")
      },
      output = @org.apache.flink.table.annotation.DataTypeHint("ROW<v ARRAY<INT>, pos INT>"))
  public static class ArrayRows extends org.apache.flink.table.functions.TableFunction<Row> {
    public void eval(Integer mode, Integer[] input) {
      if (mode == null || mode % 4 == 0) return;
      Integer[] values = input == null ? null : input.clone();
      Row result = Row.of(values, 0);
      collect(result);
      if (values != null && values.length > 0) values[0] = mode;
      result.setField(1, 1);
      collect(result);
      if (values != null) java.util.Arrays.fill(values, 0);
      result.setField(0, null);
    }
  }

  static DataType valueType(String name) {
    if (name.equals("array_required")) return DataTypes.ARRAY(DataTypes.INT().notNull());
    if (name.equals("array_fixed")) return DataTypes.ARRAY(DataTypes.BINARY(4));
    if (name.equals("date_time")) return DataTypes.MAP(DataTypes.DATE(), DataTypes.TIME(0));
    if (name.equals("array_row")) return DataTypes.ARRAY(valueType("row_STRING"));
    if (name.equals("array_map")) return DataTypes.ARRAY(valueType("map"));
    if (name.startsWith("array_"))
      return FlinkArrayScalarUdfSqlHarnessTest.rowType(name.substring(6)).getChildren().get(1);
    if (name.startsWith("row_") && !name.equals("row_int"))
      return DataTypes.ROW(
          DataTypes.FIELD("n", DataTypes.INT()),
          DataTypes.FIELD("v", FlinkTypedTableFunctionSqlHarnessTest.valueType(name.substring(4))));
    return FlinkMapScalarUdfSqlHarnessTest.rowType(name).getChildren().get(3);
  }

  static Object value(String name, int id) {
    if (id % 13 == 0) return null;
    if (name.equals("array_required"))
      return id % 7 == 0 ? new Integer[0] : new Integer[] {id, -id, Integer.MIN_VALUE};
    if (name.equals("array_fixed")) return FlinkArrayScalarUdfSqlHarnessTest.value("bytes", id);
    if (name.equals("array_row"))
      return id % 7 == 0
          ? new Row[0]
          : new Row[] {(Row) value("row_STRING", id), null, Row.ofKind(RowKind.DELETE, null, null)};
    if (name.equals("array_map"))
      return id % 7 == 0
          ? new java.util.Map[0]
          : new java.util.Map[] {
            (java.util.Map<?, ?>) value("map", id), null, new java.util.LinkedHashMap<>()
          };
    if (name.startsWith("array_"))
      return FlinkArrayScalarUdfSqlHarnessTest.value(name.substring(6), id);
    if (name.startsWith("row_") && !name.equals("row_int"))
      return Row.ofKind(
          RowKind.values()[id % 4],
          id % 7 == 0 ? null : id,
          FlinkTypedTableFunctionSqlHarnessTest.value(name.substring(4), id));
    return FlinkMapScalarUdfSqlHarnessTest.value(name, id);
  }

  static DataType rowType(String name) {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("p", valueType(name)));
  }

  static TableEnvironment environment(String name, int count, RecoveryExecutionMetrics metrics) {
    return environment(name, count, metrics, false);
  }

  private static TableEnvironment environment(
      String name, int count, RecoveryExecutionMetrics metrics, boolean aliased) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var type =
        aliased
            ? DataTypes.ROW(
                DataTypes.FIELD("f0", DataTypes.INT()), DataTypes.FIELD("f1", valueType(name)))
            : rowType(name);
    Object[] values =
        java.util.stream.IntStream.range(0, 64).mapToObj(i -> value(name, i)).toArray();
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> Row.of(i.intValue(), values[(int) (i % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    var source = table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build());
    table.createTemporaryView("src", aliased ? source.as("id", "p") : source);
    return register(table, name);
  }

  static TableEnvironment register(TableEnvironment table, String name) {
    table.createTemporarySystemFunction(
        "nested_rows", new FlinkTypedTableFunctionSqlHarnessTest.TypedRows(valueType(name)));
    table.createTemporarySystemFunction("previous_rows", new PreviousRows(valueType(name)));
    table.createTemporarySystemFunction("failing_rows", new FailingRows(valueType(name)));
    return table;
  }
}
