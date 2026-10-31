package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IntegralArithmeticProducerEncodingTest {
  @Test
  void absAdmitsOnlyRecursivelyVerifiedWrappingIntegerArithmetic() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (var name : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT,
        SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var type = types.createSqlType(name);
      RexNode input = rex.makeInputRef(type, 0);
      var one = rex.makeExactLiteral(BigDecimal.ONE, type);
      for (var operator : List.of(FlinkSqlOperatorTable.PLUS,
          FlinkSqlOperatorTable.MINUS, FlinkSqlOperatorTable.MULTIPLY)) {
        for (var leaf : List.of(input, one, rex.makeNullLiteral(type))) {
          var inner = rex.makeCall(type, operator, List.of(leaf, one));
          var nested = rex.makeCall(type, operator, List.of(inner, input));
          for (var operand : List.of(inner, nested)) {
            var call = rex.makeCall(type, FlinkSqlOperatorTable.ABS, List.of(operand));
            var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
            assertNotNull(encoded);
            assertEquals(List.of(), encoded.jvmFunctionCalls(), call.toString());
            assertTrue(encoded.nativeFunctionCalls().contains("ABS"));
            long[] constants = encoded.longs();
            assertSame(constants, encoded.udfBinding().bind(constants, null));
          }
        }
      }
      for (var operator : List.of(FlinkSqlOperatorTable.DIVIDE, FlinkSqlOperatorTable.MOD)) {
        var unsafe = rex.makeCall(type, operator, List.of(input, one));
        var sum = rex.makeCall(type, FlinkSqlOperatorTable.PLUS, List.of(input, unsafe));
        var call = rex.makeCall(type, FlinkSqlOperatorTable.ABS, List.of(sum));
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
        assertNotNull(encoded);
        assertTrue(encoded.jvmFunctionCalls().contains("ABS"), call.toString());
      }
    }
  }
}
