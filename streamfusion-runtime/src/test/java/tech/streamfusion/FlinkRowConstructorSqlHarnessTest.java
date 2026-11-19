package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.Supplier;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRowConstructorSqlHarnessTest {
  @Test
  void rowKindConsumerRetainsWholeCalcEvaluation() throws Exception {
    Supplier<TableEnvironment> environment =
        () -> {
          var table = FlinkArrayEditSqlHarnessTest.environment("BIGINT");
          table.createTemporarySystemFunction("row_kind", new KindName());
          return table;
        };
    String sql = "SELECT id,row_kind(ROW(n)) FROM src";
    var table = environment.get();
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream().anyMatch(execution ->
            execution.contains("reason=ROW metadata requires Flink's whole-Calc evaluation")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(environment, sql);
  }

  @FunctionHint(input = @DataTypeHint("ROW<n BIGINT>"), output = @DataTypeHint("STRING"))
  public static class KindName extends ScalarFunction {
    public String eval(Row row) {
      return row == null ? null : row.getKind().name();
    }
  }

  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void nativeRowsKeepFieldTypesValuesAndNulls(String type) throws Exception {
    if (!tech.streamfusion.compat.FlinkCompat.NARROW_CONSTRUCTORS_COMPILE
        && java.util.Set.of("TINYINT","SMALLINT").contains(type)) {
      for(String expression:java.util.List.of("ROW(n,n)","(ROW(n,n)).EXPR$0")) {
        var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment(type),"SELECT "+expression+" FROM src");
        assertNotNull(runs.host().failure());
        runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.FALLBACK);
      }
      return;
    }
    FlinkArrayEditSqlHarnessTest.verify(type,"ROW(n,n)","ROW");
    FlinkArrayEditSqlHarnessTest.verify(type,"(ROW(n,n)).EXPR$0","ROW");
  }
}
