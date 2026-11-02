package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class DecimalAbsEncodingTest {
  @Test
  void everyPrecisionAndScaleRetainsNativeValuesLiteralsNullsAndWidening() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        var type = types.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        assertEquals(precision, type.getPrecision());
        assertEquals(scale, type.getScale());
        var max = new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
        for (RexNode operand :
            List.of(
                rex.makeInputRef(type, 0),
                rex.makeExactLiteral(max.negate(), type),
                rex.makeNullLiteral(type))) {
          var call = rex.makeCall(type, FlinkSqlOperatorTable.ABS, List.of(operand));
          var nested = rex.makeCall(type, FlinkSqlOperatorTable.ABS, List.of(call));
          var wide = types.createSqlType(SqlTypeName.DECIMAL, 38, scale);
          var widened =
              rex.makeCall(wide, FlinkSqlOperatorTable.ABS, List.of(rex.makeCast(wide, operand)));
          for (RexNode expression : List.of(call, nested, widened)) {
            var encoded = RexExpression.encodeProjections(List.of(expression), List.of("d"));
            assertNotNull(encoded, expression.toString());
            assertEquals(6, encoded.kinds()[0]);
            assertEquals(218, encoded.payload()[0]);
            long[] constants = encoded.longs();
            assertSame(constants, encoded.udfBinding().bind(constants, null));
            assertTrue(encoded.nativeFunctionCalls().contains("ABS"));
            assertEquals(List.of(), encoded.jvmFunctionCalls());
          }
        }
      }
    }
  }

  @Test
  void arithmeticRetainsNativeAbsButRescalingAndUnverifiedProducersStayGenerated() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    RexNode input = rex.makeInputRef(type, 0);
    for (var operator : List.of(FlinkSqlOperatorTable.PLUS,
        FlinkSqlOperatorTable.MINUS, FlinkSqlOperatorTable.MULTIPLY)) {
      RexNode arithmetic = rex.makeCall(type, operator, List.of(input, input));
      RexNode nested = rex.makeCall(type, FlinkSqlOperatorTable.ABS, List.of(arithmetic));
      for (RexNode operand : List.of(arithmetic, nested,
          rex.makeCall(type, operator, List.of(nested, input)),
          rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 38, 3), arithmetic))) {
        var call = rex.makeCall(operand.getType(), FlinkSqlOperatorTable.ABS, List.of(operand));
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("d"));
        assertNotNull(encoded, call.toString());
        assertTrue(encoded.nativeFunctionCalls().contains("ABS"));
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
    }
    for (RexNode operand : List.of(
            rex.makeCall(type, FlinkSqlOperatorTable.DIVIDE, List.of(input, input)),
            rex.makeCall(type, FlinkSqlOperatorTable.MOD, List.of(input, input)),
            rex.makeCall(type, FlinkSqlOperatorTable.PLUS,
                List.of(input, rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1))),
            rex.makeCall(type, FlinkSqlOperatorTable.UNARY_MINUS, List.of(input)),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 18, 2), input),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 17, 3), input))) {
      var call = rex.makeCall(operand.getType(), FlinkSqlOperatorTable.ABS, List.of(operand));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("d"));
      assertNotNull(encoded);
      assertTrue(encoded.jvmFunctionCalls().contains("ABS"), encoded.jvmFunctionCalls().toString());
    }
  }
}
