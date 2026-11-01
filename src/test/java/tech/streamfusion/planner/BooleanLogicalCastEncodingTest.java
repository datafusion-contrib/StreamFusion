package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class BooleanLogicalCastEncodingTest {
  @Test
  void failureFreeLogicalOperandsStayNativeForBothCasts() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var bool = types.createTypeWithNullability(types.createSqlType(SqlTypeName.BOOLEAN), true);
    var input = rex.makeInputRef(bool, 0);
    var other = rex.makeInputRef(bool, 1);
    var operands = new ArrayList<RexNode>();
    for (var operator : List.of(FlinkSqlOperatorTable.NOT, FlinkSqlOperatorTable.IS_NULL,
        FlinkSqlOperatorTable.IS_NOT_NULL, FlinkSqlOperatorTable.IS_TRUE,
        FlinkSqlOperatorTable.IS_NOT_TRUE, FlinkSqlOperatorTable.IS_FALSE,
        FlinkSqlOperatorTable.IS_NOT_FALSE)) {
      operands.add(rex.makeCall(operator, input));
    }
    for (var operator : List.of(FlinkSqlOperatorTable.AND, FlinkSqlOperatorTable.OR)) {
      operands.add(rex.makeCall(operator, input, other));
      operands.add(rex.makeCall(operator, input, other, rex.makeNullLiteral(bool)));
      operands.add(rex.makeCall(operator, rex.makeCall(FlinkSqlOperatorTable.NOT, input),
          rex.makeCall(FlinkSqlOperatorTable.IS_TRUE, other)));
    }
    for (boolean legacy : new boolean[] {false, true}) {
      for (RexNode operand : operands) {
        for (var name : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
          var target = types.createSqlType(name, 8);
          for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
            var cast = rex.makeCall(target, operator, List.of(operand));
            var encoded = BooleanStringCastEncodingTest.configured(cast, legacy);
            assertEquals(244, encoded.payload()[0]);
            assertTrue(encoded.jvmFunctionCalls().isEmpty(), cast.toString());
          }
        }
      }
    }
  }

  @Test
  void logicalWrappingDoesNotAdmitAFailingOrUnverifiedArgument() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var input = rex.makeInputRef(integer, 0);
    var zero = rex.makeZeroLiteral(integer);
    var division = rex.makeCall(FlinkSqlOperatorTable.DIVIDE, input, zero);
    var predicate = rex.makeCall(FlinkSqlOperatorTable.GREATER_THAN, division, zero);
    for (boolean legacy : new boolean[] {false, true}) {
      for (var operator : List.of(FlinkSqlOperatorTable.NOT, FlinkSqlOperatorTable.IS_NULL,
          FlinkSqlOperatorTable.IS_TRUE)) {
        var operand = rex.makeCall(operator, predicate);
        for (var castOperator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
          var cast = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, 8),
              castOperator, List.of(operand));
          assertFalse(BooleanStringCastEncodingTest.configured(cast, legacy)
              .jvmFunctionCalls().isEmpty());
        }
      }
    }
  }
}
