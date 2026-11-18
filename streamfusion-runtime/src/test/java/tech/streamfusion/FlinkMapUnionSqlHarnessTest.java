package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapUnionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void unaryOrdinaryMapInputsKeepTypeNullKeysAndEntryOrder(String type)throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("MAP_UNION");
    var table=FlinkMapProjectionSqlHarnessTest.maps(type);
    var scan=tech.streamfusion.planner.NativePlanner.install(table);
    String sql="SELECT id,MAP_UNION(m) FROM src";table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains("MAP_UNION")&&x.contains("jvm=[]")),scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(()->FlinkMapProjectionSqlHarnessTest.maps(type),sql);
    BuiltinFunctionParity.assertParity(()->FlinkMapProjectionSqlHarnessTest.maps(type),sql+" WHERE id<0");
  }
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void rightEntriesWinAndKeepOrderWithDuplicateAndNullKeys(String type) throws Exception {
    String left="MAP_FROM_ARRAYS(a,a)",right="MAP_FROM_ARRAYS(b,b)";
    // Equal operands make each constructor total; different arrays still exercise map union.
    for(String expression:new String[]{"MAP_UNION("+left+","+right+")",
        "MAP_UNION("+right+","+left+")","MAP_UNION("+left+","+right+","+left+")"})
      FlinkArrayEditSqlHarnessTest.verify(type,expression,"MAP_UNION");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_KEYS(MAP_UNION("+left+","+right+"))","MAP_UNION");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_VALUES(MAP_UNION("+right+","+left+"))","MAP_UNION");
  }
}
