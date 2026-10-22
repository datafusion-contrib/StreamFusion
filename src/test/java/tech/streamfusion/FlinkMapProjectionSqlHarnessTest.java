package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.flink.table.api.DataTypes.*;
import java.util.*;
import java.util.function.Supplier;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapProjectionSqlHarnessTest {
  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("stringKeyProjectionCases")
  void stringKeysProjectInRustWithoutLosingNullsOrEntryOrder(String function, String type)
      throws Exception {
    Supplier<TableEnvironment> environment = () -> stringMaps(type);
    var table = environment.get();
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    String sql = "SELECT id," + function + "(m) FROM src";
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("native=[") && execution.contains(function)
            && execution.contains("jvm=[]")), scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(environment, sql);
    BuiltinFunctionParity.assertParity(environment, sql + " WHERE MOD(id,3)=0");
    BuiltinFunctionParity.assertParity(environment, sql + " WHERE id < 0");
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> stringKeyProjectionCases() {
    return java.util.stream.Stream.of("MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES")
        .flatMap(function -> java.util.stream.Stream.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL")
            .map(type -> org.junit.jupiter.params.provider.Arguments.of(function, type)));
  }

  private static TableEnvironment stringMaps(String type) {
    org.apache.flink.table.types.DataType element;
    Object one, two;
    switch (type) {
      case "BOOLEAN": element=BOOLEAN(); one=true; two=false; break;
      case "TINYINT": element=TINYINT(); one=Byte.MIN_VALUE; two=Byte.MAX_VALUE; break;
      case "SMALLINT": element=SMALLINT(); one=Short.MIN_VALUE; two=Short.MAX_VALUE; break;
      case "INT": element=INT(); one=Integer.MIN_VALUE; two=Integer.MAX_VALUE; break;
      case "BIGINT": element=BIGINT(); one=Long.MIN_VALUE; two=Long.MAX_VALUE; break;
      default:
        element=DECIMAL(38,9);
        one=new java.math.BigDecimal("9007199254740993.000000001");
        two=new java.math.BigDecimal("-9007199254740993.000000001");
    }
    var entries = new LinkedHashMap<String, Object>();
    entries.put(null, one);
    entries.put("Aa", null);
    entries.put("BB", one);
    entries.put("\ud800", two);
    entries.put("中\0", one);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 2053; i++)
      rows.add(Row.of(i, i % 7 == 0 ? null : i % 5 == 0 ? Map.of() : entries));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("m", MAP(STRING(), element))), rows);
  }

  @Test
  void stringMapViewsCanShareOneInputAcrossOutputColumns() throws Exception {
    String sql = "SELECT id,MAP_KEYS(m),MAP_VALUES(m),MAP_ENTRIES(m) FROM src";
    var table = stringMaps("BIGINT");
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("MAP_KEYS") && execution.contains("MAP_VALUES")
            && execution.contains("MAP_ENTRIES") && execution.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(() -> stringMaps("BIGINT"), sql);
    BuiltinFunctionParity.assertParity(() -> stringMaps("BIGINT"),
        "SELECT id,m,MAP_VALUES(m) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings={
      "ARRAY_JOIN(MAP_KEYS(m),'|','NULL')",
      "JSON_QUOTE(MAP_KEYS(m)[4])",
      "CARDINALITY(MAP_VALUES(m))",
      "MAP_KEYS(m),ARRAY_JOIN(MAP_KEYS(m),'|','NULL')"})
  void stringMapConsumersRetainVerifiedWholeCalcEvaluation(String expression) throws Exception {
    if (expression.contains("JSON_QUOTE"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_QUOTE");
    String sql = "SELECT id," + expression + " FROM src";
    var table = stringMaps("BIGINT");
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("jvm=[") && !execution.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(() -> stringMaps("BIGINT"), sql);
  }

  @Test
  void stringMapKeysKeepTheUnverifiedStateBoundaryGuard() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> stringMaps("BIGINT"),
        "SELECT MAP_KEYS(m),COUNT(*) FROM src GROUP BY MAP_KEYS(m)",
        "collection value representation requires a final projection");
  }

  @ParameterizedTest
  @ValueSource(booleans={false, true})
  void runtimeGenericAndBinaryMapsKeepStringCachesAndArrayCopyCategories(boolean binary)
      throws Exception {
    Supplier<TableEnvironment> environment = () -> {
      var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
          .getExecutionEnvironment();
      env.setParallelism(1);
      env.getConfig().enableObjectReuse();
      var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      table.getConfig().set("streamfusion.transpose.batchRows", "2");
      var type = ROW(FIELD("id", INT()), FIELD("m", MAP(STRING(), BIGINT())));
      var rowType = (org.apache.flink.table.types.logical.RowType) type.getLogicalType();
      var stream = env.fromSequence(0, 15).map(id -> {
        var entries = new LinkedHashMap<Object, Object>();
        entries.put(null, -(long) id);
        for (String key : List.of("Aa", "BB", "\ud800", "😀", "\ue000", ""))
          entries.put(org.apache.flink.table.data.StringData.fromString(key), id % 5 == 0 ? null : id);
        org.apache.flink.table.data.MapData map = new org.apache.flink.table.data.GenericMapData(entries);
        if (binary)
          map = new org.apache.flink.table.runtime.typeutils.MapDataSerializer(
              STRING().getLogicalType(), BIGINT().getLogicalType()).toBinaryMap(map).copy();
        return (org.apache.flink.table.data.RowData)
            org.apache.flink.table.data.GenericRowData.of((int) (long) id,
                id % 7 == 0 ? null : map);
      }).returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType));
      table.createTemporaryView("src", stream,
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      return table;
    };
    BuiltinFunctionParity.assertParity(environment,
        "SELECT id,MAP_KEYS(m),MAP_VALUES(m),MAP_ENTRIES(m) FROM src");
  }

  @Test
  void entryRowKindConsumersRetainWholeCalcEvaluation() throws Exception {
    Supplier<TableEnvironment> environment =
        () -> {
          var table = maps("BIGINT");
          table.createTemporarySystemFunction("entry_kinds", new EntryKinds());
          return table;
        };
    String sql = "SELECT id,entry_kinds(MAP_ENTRIES(m)[1]) FROM src";
    var table = environment.get();
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(execution -> execution.contains("MAP_ENTRIES")
                && execution.contains("reason=ROW metadata requires Flink's whole-Calc evaluation")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(environment, sql);
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<`key` BIGINT, `value` BIGINT>"),
      output = @DataTypeHint("STRING"))
  public static class EntryKinds extends ScalarFunction {
    public String eval(Row entry) {
      return entry == null ? null : entry.getKind().name();
    }
  }

  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void keysPreserveNullDuplicatesAndOriginalEntryOrder(String type) throws Exception {
    verify(type,"MAP_KEYS");
  }
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void valuesPreserveNullElementsAndEntryOrder(String type) throws Exception {
    verify(type,"MAP_VALUES");
  }
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void entriesPreserveNullableKeyValuePairs(String type) throws Exception {
    verify(type,"MAP_ENTRIES");
  }
  static org.apache.flink.table.api.TableEnvironment maps(String type) {
    org.apache.flink.table.types.DataType element;
    Object one,two;
    switch(type) {
      case "BOOLEAN": element=BOOLEAN();one=true;two=false;break;
      case "TINYINT":element=TINYINT();one=(byte)1;two=(byte)-2;break;
      case "SMALLINT":element=SMALLINT();one=(short)1;two=(short)-2;break;
      case "INT":element=INT();one=1;two=-2;break;
      case "BIGINT":element=BIGINT();one=1L;two=-2L;break;
      default:element=DECIMAL(38,9);one=new java.math.BigDecimal("9007199254740993.000000001");two=new java.math.BigDecimal("-9007199254740993.000000001");
    }
    var map=new LinkedHashMap<Object,Object>();map.put(null,two);map.put(two,null);map.put(one,two);
    List<Row> rows=new ArrayList<>();
    for(int i=0;i<2053;i++)rows.add(Row.of(i,i%7==0?null:i%5==0?Map.of():map));
    return BuiltinFunctionParity.environment(ROW(FIELD("id",INT()),FIELD("m",MAP(element,element))),rows);
  }
  static void verify(String type,String function) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type,function+"(MAP_FROM_ARRAYS(a,a))",function);
    var table=maps(type);
    var scan=tech.streamfusion.planner.NativePlanner.install(table);
    String sql="SELECT id,"+function+"(m) FROM src";
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains(function)&&x.contains("jvm=[]")),scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(()->maps(type),sql);
    BuiltinFunctionParity.assertParity(()->maps(type),sql+" WHERE id < 0");
  }
}
