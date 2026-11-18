package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.flink.table.api.DataTypes.*;
import java.util.*;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class FlinkArrayJoinSqlHarnessTest {
  static org.apache.flink.table.api.TableEnvironment environment() {
    var rows=new ArrayList<Row>();
    String[][] values={null,{}, {null,null},{"",null,"é中"},{"\ud800","\udc00","?",null},{"\ud83d\ude00","\u0000","tail"}};
    for(int i=0;i<1031;i++)rows.add(Row.of(i,values[i%values.length],i%7==0?null:i%3==0?"":i%2==0?"\ud800":"|",i%5==0?null:i%2==0?"\udc00":"NULL"));
    var table=BuiltinFunctionParity.environment(ROW(FIELD("id",INT()),FIELD("a",ARRAY(STRING())),FIELD("sep",STRING()),FIELD("replacement",STRING())),rows);
    table.getConfig().set("streamfusion.experimental.collectionExpressions","true");return table;
  }
  @ParameterizedTest
  @ValueSource(strings={"ARRAY_JOIN(a,sep)","ARRAY_JOIN(a,sep,replacement)","ARRAY_JOIN(a,'|','NULL')"})
  void joinMaterializesEachInputBeforeConcatenation(String expression)throws Exception{
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_JOIN");
    var table=environment();var scan=tech.streamfusion.planner.NativePlanner.install(table);
    String sql="SELECT id,"+expression+" FROM src";table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains("ARRAY_JOIN")&&x.contains("jvm=[]")),scan.functionExecutions().toString());
    for(String suffix:List.of(""," WHERE MOD(id,3)=0"," WHERE id<0"))BuiltinFunctionParity.assertParity(FlinkArrayJoinSqlHarnessTest::environment,sql+suffix);
  }
  @Test void rawCoprojectionsAndStringConsumersKeepTheirRepresentations(){
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_JOIN");
    for(String sql:List.of("SELECT a,ARRAY_JOIN(a,sep),replacement FROM src",
        "SELECT ARRAY_JOIN(a,sep),ARRAY_CONTAINS(a,'?') FROM src")) {
      var table=environment();var scan=tech.streamfusion.planner.NativePlanner.install(table);table.explainSql(sql);
      assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains("jvm=[")&&!x.contains("jvm=[]")),scan.functionExecutions().toString());
      var runs=NativeFailureParity.run(FlinkArrayJoinSqlHarnessTest::environment,sql);runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    }
  }
}
