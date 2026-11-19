package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class RuntimeSqlTrimEncodingTest {
  @Test
  void runtimeDirectionsKeepTheirSymbolInTheHostProgramAndOnlySendStringArguments() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var text = types.createTypeWithNullability(
        types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    int operation = 13;
    for (var flag : List.of(SqlTrimFunction.Flag.BOTH, SqlTrimFunction.Flag.LEADING,
        SqlTrimFunction.Flag.TRAILING)) {
      var call = rex.makeCall(text, FlinkSqlOperatorTable.TRIM,
          List.of(rex.makeFlag(flag), rex.makeInputRef(text, 0), rex.makeInputRef(text, 1)));
      var constructor = RexExpression.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      var encoder = constructor.newInstance();
      var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
      configure.setAccessible(true);
      configure.invoke(encoder, TableConfig.getDefault());
      var javaStrings = RexExpression.class.getDeclaredField("javaStringInputs");
      javaStrings.setAccessible(true);
      javaStrings.setBoolean(encoder, true);
      var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
      emit.setAccessible(true);
      assertTrue((Boolean) emit.invoke(encoder, call));
      assertArrayEquals(new int[] {44, 0, 0}, encoder.kinds());
      assertEquals(2, encoder.childCounts()[0]);
      assertEquals(operation++, encoder.longs()[2]);
      assertEquals(List.of("TRIM"), encoder.jvmFunctionCalls(), "The exact host evaluator remains bound");
    }
  }
}
