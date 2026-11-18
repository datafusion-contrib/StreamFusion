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

class IntegralTruncatePositionEncodingTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void verifiedPositionsWidthsLiteralsAndNullsNeedNoEvaluator() {
    var positionType = types.createSqlType(SqlTypeName.INTEGER);
    for (var name : List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
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
  void wrappingArithmeticRetainsItsWidthAtEveryVerifiedPosition() {
    var positionType = types.createSqlType(SqlTypeName.INTEGER);
    for (var name : List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var type = types.createSqlType(name);
      var input = rex.makeInputRef(type, 0);
      var one = rex.makeExactLiteral(BigDecimal.ONE, type);
      for (var operation : List.of(FlinkSqlOperatorTable.PLUS,
          FlinkSqlOperatorTable.MINUS, FlinkSqlOperatorTable.MULTIPLY)) {
        var inner = rex.makeCall(type, operation, List.of(input, one));
        var nested = rex.makeCall(type, operation, List.of(inner, input));
        for (RexNode operand : List.of(inner, nested)) {
          for (int position = -38; position <= 38; position++) {
            assertNative(type, operand,
                rex.makeExactLiteral(BigDecimal.valueOf(position), positionType));
          }
          assertNative(type, operand, rex.makeNullLiteral(positionType));
        }
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
    for (var operation : List.of(FlinkSqlOperatorTable.DIVIDE, FlinkSqlOperatorTable.MOD)) {
      var unsafe = rex.makeCall(type, operation, List.of(input, rex.makeExactLiteral(BigDecimal.ONE, type)));
      assertGenerated(type, rex.makeCall(type, FlinkSqlOperatorTable.PLUS, List.of(input, unsafe)), scale);
      assertGenerated(type, unsafe, rex.makeNullLiteral(type));
    }
    assertGenerated(
        type,
        rex.makeCast(type, rex.makeCast(types.createSqlType(SqlTypeName.SMALLINT), input)),
        scale);
  }

  @Test
  void narrowInputsKeepTheReleasedCompilationContract() {
    var positionType = types.createSqlType(SqlTypeName.INTEGER);
    for (var name : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT)) {
      var type = types.createSqlType(name);
      var call =
          rex.makeCall(
              type,
              FlinkSqlOperatorTable.TRUNCATE,
              List.of(
                  rex.makeInputRef(type, 0),
                  rex.makeExactLiteral(BigDecimal.valueOf(-1), positionType)));
      assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
    }
  }

  private void assertNative(
      org.apache.calcite.rel.type.RelDataType type, RexNode input, RexNode position) {
    var call = rex.makeCall(type, FlinkSqlOperatorTable.TRUNCATE, List.of(input, position));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    if (((org.apache.calcite.rex.RexLiteral) position).isNull()) {
      assertArrayEquals(new int[] {31}, encoded.kinds());
      assertArrayEquals(new int[] {0}, encoded.childCounts());
    } else {
      assertEquals(6, encoded.kinds()[0]);
      assertEquals(231, encoded.payload()[0]);
    }
    assertEquals(List.of(), encoded.jvmFunctionCalls());
    assertTrue(encoded.nativeFunctionCalls().contains("TRUNCATE"));
  }

  private void assertGenerated(
      org.apache.calcite.rel.type.RelDataType type, RexNode input, RexNode position) {
    var call = rex.makeCall(type, FlinkSqlOperatorTable.TRUNCATE, List.of(input, position));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    assertTrue(encoded.jvmFunctionCalls().contains("TRUNCATE"));
  }
}
