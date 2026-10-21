package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.flink.table.api.DataTypes.*;
import java.util.*;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkNativeObjectSqlHarnessTest {
  static String object(){return "OBJECT_OF('"+NumericItem.class.getName()+"','a',n,'b',id)";}
  static org.apache.flink.table.api.TableEnvironment environment(){
    var table=BuiltinFunctionParity.environment(ROW(FIELD("id",INT()),FIELD("n",INT())),
        List.of(Row.of(0,42),Row.of(1,null),Row.of(2,Integer.MIN_VALUE),Row.of(3,Integer.MAX_VALUE)));
    table.getConfig().set("streamfusion.experimental.collectionExpressions","true");
    table.getConfig().set("streamfusion.transpose.batchRows","2");return table;
  }
  static void verify(String expression,String function)throws Exception{
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var table=environment();var scan=tech.streamfusion.planner.NativePlanner.install(table);
    String sql="SELECT id,"+expression+" FROM src";table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains(function)&&x.contains("jvm=[]")),scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(FlinkNativeObjectSqlHarnessTest::environment,sql);
    BuiltinFunctionParity.assertParity(FlinkNativeObjectSqlHarnessTest::environment,sql+" WHERE id<0");
  }
  @Test void basicObjectsAndFieldAccessUseSharedNativeFields()throws Exception{
    verify(object(),"OBJECT_OF");verify("("+object()+").a","OBJECT_OF");
  }
  @Test void unsharedInlineUpdatesPreserveFields()throws Exception{
    verify("OBJECT_UPDATE("+object()+",'a',id)","OBJECT_UPDATE");
    verify("OBJECT_UPDATE("+object()+",'a',id,'b',n)","OBJECT_UPDATE");
  }
  @Test void aliasesRetainReleasedMutationAndProjectionOrder(){
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("OBJECT_UPDATE");
    for(String sql:List.of(
        "SELECT o,OBJECT_UPDATE(o,'a',id),o FROM (SELECT id,"+object()+" AS o FROM src)",
        "SELECT OBJECT_UPDATE(o,'a',id),o FROM (SELECT id,"+object()+" AS o FROM src)")){
      var runs=NativeFailureParity.run(FlinkNativeObjectSqlHarnessTest::environment,sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    }
  }
  public static class NumericItem {
    public Integer a;
    public Integer b;
    @Override public boolean equals(Object other){return other instanceof NumericItem
        && Objects.equals(a,((NumericItem)other).a)&&Objects.equals(b,((NumericItem)other).b);}
    @Override public int hashCode(){return Objects.hash(a,b);}
    @Override public String toString(){return "NumericItem("+a+","+b+")";}
  }
}
