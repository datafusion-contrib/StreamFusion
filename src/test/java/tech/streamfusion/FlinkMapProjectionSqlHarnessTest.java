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
