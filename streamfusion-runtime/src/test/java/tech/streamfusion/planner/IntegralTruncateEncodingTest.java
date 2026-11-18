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

class IntegralTruncateEncodingTest {
  @Test
  void integerTruncateReusesItsOperandWithoutAFunctionOrJvmBinding() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    for (var name : List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var type = types.createSqlType(name);
      for (RexNode operand :
          List.of(
              rex.makeInputRef(type, 0),
              rex.makeExactLiteral(BigDecimal.valueOf(-1), type),
              rex.makeNullLiteral(type))) {
        var call = rex.makeCall(type, FlinkSqlOperatorTable.TRUNCATE, List.of(operand));
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
        var direct = RexExpression.encodeProjections(List.of(operand), List.of("v"));
        assertNotNull(encoded);
        assertNotNull(direct);
        assertArrayEquals(direct.kinds(), encoded.kinds());
        assertArrayEquals(direct.payload(), encoded.payload());
        assertArrayEquals(direct.childCounts(), encoded.childCounts());
        assertArrayEquals(direct.longs(), encoded.longs());
        long[] constants = encoded.longs();
        assertSame(constants, encoded.udfBinding().bind(constants, null));
        assertEquals(List.of("TRUNCATE"), encoded.nativeFunctionCalls());
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
    }
  }
}
