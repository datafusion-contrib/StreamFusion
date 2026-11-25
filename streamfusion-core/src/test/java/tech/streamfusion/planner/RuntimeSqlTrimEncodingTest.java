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
import tech.streamfusion.planner.common.CertifiedOp;
import tech.streamfusion.planner.common.ExpressionKind;

class RuntimeSqlTrimEncodingTest {
  @Test
  void runtimeDirectionsEncodeTheOperationAndBothStringsWithoutAnEvaluator() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var text = types.createTypeWithNullability(
        types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    int operation = CertifiedOp.TRIM;
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
      assertArrayEquals(new int[] {
          ExpressionKind.STRING_TRIM.code(), ExpressionKind.INPUT_REF.code(),
          ExpressionKind.INPUT_REF.code()}, encoder.kinds());
      assertEquals(2, encoder.childCounts()[0]);
      assertArrayEquals(new int[] {operation++, 0, 1}, encoder.payload());
      assertTrue(encoder.udfBinding().isEmpty());
      assertEquals(0, encoder.jvmExpressionCount());
      assertEquals(List.of("TRIM"), encoder.nativeFunctionCalls());
    }
  }
}
