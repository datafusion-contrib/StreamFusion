package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapConstructorSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void runtimeKeysAreNotDeduplicatedByValue(String type) throws Exception {
    String t=FlinkArrayEditSqlHarnessTest.sqlType(type);
    if (!tech.streamfusion.compat.FlinkCompat.NARROW_CONSTRUCTORS_COMPILE
        && java.util.Set.of("TINYINT","SMALLINT").contains(type)) {
      for(String expression:java.util.List.of("MAP[n,n]","MAP[n,n,CAST(NULL AS "+t+"),n]")) {
        var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment(type),"SELECT "+expression+" FROM src");
        assertNotNull(runs.host().failure());
        runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.FALLBACK);
      }
      return;
    }
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP[n,n]","MAP");
    String expression = "MAP[n,n,CAST(NULL AS " + t + "),n]";
    if (tech.streamfusion.compat.FlinkCompat.ORDERED_MAP_CONSTRUCTORS) {
      FlinkArrayEditSqlHarnessTest.verify(type, expression, "MAP");
    } else {
      String sql = "SELECT id," + expression + " FROM src";
      var table = FlinkArrayEditSqlHarnessTest.environment(type);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      table.explainSql(sql);
      assertTrue(
          scan.functionExecutions().stream().anyMatch(execution -> execution.contains("jvm=[MAP]")),
          scan.functionExecutions().toString());
      BuiltinFunctionParity.assertParity(() -> FlinkArrayEditSqlHarnessTest.environment(type), sql);
      BuiltinFunctionParity.assertParity(
          () -> FlinkArrayEditSqlHarnessTest.environment(type), sql + " WHERE id < 0");
    }
  }
  @ParameterizedTest
  @ValueSource(strings={"MAP[n,n,n,n]","MAP[n,n,n,1/n]","MAP[n,1/n,n,n]"})
  void repeatedKeyExpressionsKeepReleasedDeduplicationAndEvaluation(String projection) {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),"SELECT "+projection+" FROM src");
    if(runs.host().failure()==null)runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.NATIVE);
  }
}
