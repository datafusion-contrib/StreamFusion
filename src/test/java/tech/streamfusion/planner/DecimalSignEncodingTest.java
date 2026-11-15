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

class DecimalSignEncodingTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void everyPrecisionAndScaleUsesOnlyTheVerifiedRoute() {
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        var type = types.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        var max = new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
        for (RexNode operand :
            List.of(
                rex.makeInputRef(type, 0),
                rex.makeExactLiteral(max.negate(), type),
                rex.makeNullLiteral(type))) {
          var call = rex.makeCall(type, FlinkSqlOperatorTable.SIGN, List.of(operand));
          var nested = rex.makeCall(type, FlinkSqlOperatorTable.SIGN, List.of(call));
          for (RexNode expression : List.of(call, nested)) {
            var encoded = encode(expression);
            boolean generated = precision <= 18 && precision == scale;
            assertEquals(generated ? 17 : 6, encoded.kinds()[0], expression.toString());
            assertEquals(generated, encoded.jvmFunctionCalls().contains("SIGN"));
            if (!generated) {
              assertEquals(65, encoded.payload()[0]);
              long[] constants = encoded.longs();
              assertSame(constants, encoded.udfBinding().bind(constants, null));
            }
          }
          var wide = types.createSqlType(SqlTypeName.DECIMAL, 38, scale);
          var widened =
              rex.makeCall(wide, FlinkSqlOperatorTable.SIGN, List.of(rex.makeCast(wide, operand)));
          assertEquals(6, encode(widened).kinds()[0]);
        }
      }
    }
  }

  @Test
  void unverifiedProducersAndCompactSignIntermediatesStayGenerated() {
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    RexNode input = rex.makeInputRef(type, 0);
    var compact = types.createSqlType(SqlTypeName.DECIMAL, 18, 18);
    var compactSign =
        rex.makeCall(compact, FlinkSqlOperatorTable.SIGN, List.of(rex.makeInputRef(compact, 0)));
    for (RexNode operand :
        List.of(
            rex.makeCall(
                type,
                FlinkSqlOperatorTable.DIVIDE,
                List.of(input, rex.makeExactLiteral(BigDecimal.ONE, type))),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 18, 2), input),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 17, 3), input),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 38, 18), compactSign))) {
      var call = rex.makeCall(operand.getType(), FlinkSqlOperatorTable.SIGN, List.of(operand));
      assertEquals(17, encode(call).kinds()[0], call.toString());
    }
  }

  private static RexExpression encode(RexNode node) {
    var encoded = RexExpression.encodeProjections(List.of(node), List.of("d"));
    assertNotNull(encoded, node.toString());
    return encoded;
  }
}
