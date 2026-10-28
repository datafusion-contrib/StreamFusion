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

class IntegralRoundPositionEncodingTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void verifiedPositionsWidthsLiteralsAndNullsNeedNoEvaluator() {
    var positionType = types.createSqlType(SqlTypeName.INTEGER);
    for (var name :
        List.of(
            SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var type = types.createSqlType(name);
      for (RexNode input :
          List.of(
              rex.makeInputRef(type, 0),
              rex.makeExactLiteral(BigDecimal.valueOf(-15), type),
              rex.makeNullLiteral(type))) {
        for (int position = -38; position <= 38; position++) {
          assertNative(
              type, input, rex.makeExactLiteral(BigDecimal.valueOf(position), positionType));
        }
        assertNative(type, input, rex.makeNullLiteral(positionType));
      }
    }
  }

  @Test
  void runtimeExtremeAndUnverifiedProducersStayGenerated() {
    var type = types.createSqlType(SqlTypeName.INTEGER);
    var input = rex.makeInputRef(type, 0);
    for (RexNode position :
        List.of(
            rex.makeInputRef(type, 1),
            rex.makeExactLiteral(BigDecimal.valueOf(-39), type),
            rex.makeExactLiteral(BigDecimal.valueOf(Integer.MAX_VALUE), type))) {
      assertGenerated(type, input, position);
    }
    var scale = rex.makeExactLiteral(BigDecimal.valueOf(-1), type);
    assertGenerated(
        type,
        rex.makeCall(FlinkSqlOperatorTable.PLUS, input, rex.makeExactLiteral(BigDecimal.ONE, type)),
        scale);
    assertGenerated(
        type,
        rex.makeCast(type, rex.makeCast(types.createSqlType(SqlTypeName.SMALLINT), input)),
        scale);
  }

  private void assertNative(
      org.apache.calcite.rel.type.RelDataType type, RexNode input, RexNode position) {
    var call = rex.makeCall(type, FlinkSqlOperatorTable.ROUND, List.of(input, position));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    assertEquals(6, encoded.kinds()[0]);
    assertEquals(230, encoded.payload()[0]);
    assertEquals(List.of(), encoded.jvmFunctionCalls());
    assertTrue(encoded.nativeFunctionCalls().contains("ROUND"));
  }

  private void assertGenerated(
      org.apache.calcite.rel.type.RelDataType type, RexNode input, RexNode position) {
    var call = rex.makeCall(type, FlinkSqlOperatorTable.ROUND, List.of(input, position));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    assertTrue(encoded.jvmFunctionCalls().contains("ROUND"));
  }
}
