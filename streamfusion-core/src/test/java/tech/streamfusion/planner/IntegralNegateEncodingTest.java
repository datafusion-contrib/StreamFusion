package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IntegralNegateEncodingTest {
  @Test
  void typedInputsMinimaAndNullsUseTheWrappingKernelWithoutBindings() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    for (var name :
        List.of(
            SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var type = types.createSqlType(name);
      long min;
      switch (name) {
        case TINYINT:
          min = Byte.MIN_VALUE;
          break;
        case SMALLINT:
          min = Short.MIN_VALUE;
          break;
        case INTEGER:
          min = Integer.MIN_VALUE;
          break;
        default:
          min = Long.MIN_VALUE;
      }
      for (RexNode operand :
          List.of(
              rex.makeInputRef(type, 0),
              rex.makeExactLiteral(BigDecimal.valueOf(min), type),
              rex.makeNullLiteral(type))) {
        var call = rex.makeCall(type, FlinkSqlOperatorTable.UNARY_MINUS, List.of(operand));
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
        assertNotNull(encoded);
        assertEquals(6, encoded.kinds()[0]);
        assertEquals(
            operand instanceof org.apache.calcite.rex.RexInputRef ? 5 : 219, encoded.payload()[0]);
        long[] constants = encoded.longs();
        assertSame(constants, encoded.udfBinding().bind(constants, null));
        assertEquals(List.of("-"), encoded.nativeFunctionCalls());
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
    }
  }
}
