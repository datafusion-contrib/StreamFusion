package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class DecimalSignArithmeticEncodingTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void verifiedDecimalArithmeticRetainsNativeSign() {
    for (int[] pair : new int[][] {{18, 3}, {19, 19}, {38, 18}, {38, 38}}) {
      var type = types.createSqlType(SqlTypeName.DECIMAL, pair[0], pair[1]);
      RexNode input = rex.makeInputRef(type, 0);
      for (var operator : List.of(FlinkSqlOperatorTable.PLUS, FlinkSqlOperatorTable.MINUS,
          FlinkSqlOperatorTable.MULTIPLY)) {
        RexNode arithmetic = rex.makeCall(type, operator, List.of(input, input));
        RexNode nested = rex.makeCall(type, FlinkSqlOperatorTable.SIGN, List.of(arithmetic));
        for (RexNode operand : List.of(arithmetic, nested,
            rex.makeCall(type, operator, List.of(nested, input)),
            rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 38, pair[1]), arithmetic))) {
          var call = rex.makeCall(operand.getType(), FlinkSqlOperatorTable.SIGN, List.of(operand));
          var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
          assertNotNull(encoded, call.toString());
          assertEquals(6, encoded.kinds()[0]);
          assertEquals(65, encoded.payload()[0]);
          assertEquals(List.of(), encoded.jvmFunctionCalls());
          assertTrue(encoded.nativeFunctionCalls().contains("SIGN"));
        }
      }
    }
  }

  @Test
  void verifiedUnaryProducersKeepSignNativeWithoutAdmittingCompactResults() {
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        var type = types.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        RexNode input = rex.makeInputRef(type, 0);
        for (var operator : List.of(FlinkSqlOperatorTable.ABS, FlinkSqlOperatorTable.UNARY_MINUS)) {
          var unary = rex.makeCall(type, operator, List.of(input));
          var call = rex.makeCall(type, FlinkSqlOperatorTable.SIGN, List.of(unary));
          var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
          assertNotNull(encoded, call.toString());
          assertEquals(precision <= 18 && precision == scale,
              encoded.jvmFunctionCalls().contains("SIGN"), call.toString());
        }
      }
    }
  }

  @Test
  void unverifiedAndCompactSignProducersStayGenerated() {
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    RexNode input = rex.makeInputRef(type, 0);
    var compact = types.createSqlType(SqlTypeName.DECIMAL, 18, 18);
    var compactSign = rex.makeCall(compact, FlinkSqlOperatorTable.SIGN,
        List.of(rex.makeInputRef(compact, 1)));
    var widenedCompact = rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 38, 18), compactSign);
    for (RexNode operand : List.of(
        rex.makeCall(type, FlinkSqlOperatorTable.DIVIDE, List.of(input, input)),
        rex.makeCall(type, FlinkSqlOperatorTable.MOD, List.of(input, input)),
        rex.makeCall(type, FlinkSqlOperatorTable.PLUS,
            List.of(input, rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 2))),
        rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 18, 2), input),
        rex.makeCast(types.createSqlType(SqlTypeName.DECIMAL, 17, 3), input),
        widenedCompact,
        rex.makeCall(widenedCompact.getType(), FlinkSqlOperatorTable.PLUS,
            List.of(widenedCompact, widenedCompact)))) {
      var call = rex.makeCall(operand.getType(), FlinkSqlOperatorTable.SIGN, List.of(operand));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
      assertNotNull(encoded, call.toString());
      assertTrue(encoded.jvmFunctionCalls().contains("SIGN"), encoded.jvmFunctionCalls().toString());
    }
  }
}
