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

class DecimalSignProjectionEncodingTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void onlyNoncompactZeroIntegerWidthSignsNeedTheFinalDefaultContract() {
    for (int precision = 19; precision <= 38; precision++) {
      for (boolean nullable : new boolean[] {false, true}) {
        var decimal = types.createTypeWithNullability(
            types.createSqlType(SqlTypeName.DECIMAL, precision, precision), nullable);
        var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN,
            List.of(rex.makeInputRef(decimal, 0)));
        var bool = types.createTypeWithNullability(types.createSqlType(SqlTypeName.BOOLEAN), nullable);
        var comparison = rex.makeCall(bool, FlinkSqlOperatorTable.GREATER_THAN,
            List.of(sign, rex.makeExactLiteral(BigDecimal.ZERO.setScale(precision), decimal)));
        var integer = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), nullable);
        var cast = rex.makeCall(integer, FlinkSqlOperatorTable.CAST, List.of(sign));
        for (RexNode expression : List.of(comparison, cast)) {
          var encoded = RexExpression.encodeProjections(List.of(expression), List.of("v"));
          assertNotNull(encoded, expression.toString());
          if (nullable && expression == comparison) {
            assertEquals(List.of(), encoded.jvmFunctionCalls());
          } else if (nullable) {
            assertTrue(encoded.nativeFunctionCalls().contains("SIGN"));
            assertFalse(encoded.jvmFunctionCalls().contains("SIGN"));
          } else {
            assertArrayEquals(new int[] {17, 0}, encoded.kinds());
            assertTrue(encoded.jvmFunctionCalls().contains("SIGN"), encoded.jvmFunctionCalls().toString());
          }
        }
        var direct = RexExpression.encodeProjections(List.of(sign), List.of("v"));
        assertNotNull(direct);
        assertEquals(6, direct.kinds()[0]);
        assertEquals(65, direct.payload()[0]);
        assertEquals(List.of(), direct.jvmFunctionCalls());
      }
    }
  }

  @Test
  void ordinarySignsAndRetainedNullAwareConsumersKeepTheirNativeRules() {
    for (int[] pair : new int[][] {{18, 3}, {19, 3}, {38, 18}, {19, 19}, {38, 38}}) {
      var decimal = types.createSqlType(SqlTypeName.DECIMAL, pair[0], pair[1]);
      var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN,
          List.of(rex.makeInputRef(decimal, 0)));
      var bool = types.createSqlType(SqlTypeName.BOOLEAN);
      var comparison = rex.makeCall(bool, FlinkSqlOperatorTable.GREATER_THAN,
          List.of(sign, rex.makeExactLiteral(BigDecimal.ZERO.setScale(pair[1]), decimal)));
      if (pair[0] > pair[1]) {
        var encoded = RexExpression.encodeProjections(List.of(comparison), List.of("v"));
        assertNotNull(encoded);
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
      for (var operator : List.of(FlinkSqlOperatorTable.IS_NULL, FlinkSqlOperatorTable.IS_NOT_NULL,
          FlinkSqlOperatorTable.IS_TRUE, FlinkSqlOperatorTable.IS_NOT_TRUE,
          FlinkSqlOperatorTable.IS_FALSE, FlinkSqlOperatorTable.IS_NOT_FALSE)) {
        var test = rex.makeCall(bool, operator, List.of(comparison));
        var encoded = RexExpression.encodeProjections(List.of(test), List.of("v"));
        assertNotNull(encoded, test.toString());
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
    }
  }
}
