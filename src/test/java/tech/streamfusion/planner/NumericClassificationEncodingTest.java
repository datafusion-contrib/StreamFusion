package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class NumericClassificationEncodingTest {
  @Test
  void exactNumericPredicatesKeepTheirOperandsWithoutAnEvaluator() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (var type : List.of(types.createSqlType(SqlTypeName.TINYINT),
        types.createSqlType(SqlTypeName.SMALLINT), types.createSqlType(SqlTypeName.INTEGER),
        types.createSqlType(SqlTypeName.BIGINT), types.createSqlType(SqlTypeName.DECIMAL, 18, 2),
        types.createSqlType(SqlTypeName.DECIMAL, 38, 18))) {
      for (boolean nullable : new boolean[] {false, true}) {
        var input = rex.makeInputRef(types.createTypeWithNullability(type, nullable), 0);
        for (var value : List.of(input, rex.makeNullLiteral(type),
            rex.makeExactLiteral(BigDecimal.ONE, type))) {
          var call = rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN),
              FlinkSqlOperatorTable.IS_ALPHA, List.of(value));
          var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
          assertNotNull(encoded);
          assertEquals(6, encoded.kinds()[0]);
          assertEquals(241, encoded.payload()[0]);
          assertEquals(1, encoded.childCounts()[0]);
          assertTrue(encoded.jvmFunctionCalls().isEmpty());
          assertTrue(encoded.nativeFunctionCalls().contains("IS_ALPHA"));
        }
      }
    }
  }

  @Test
  void onlyProvenFailureFreeIntegerTreesAreDiscarded() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.INTEGER);
    var input = rex.makeInputRef(type, 0);
    for (var operation : List.of(FlinkSqlOperatorTable.PLUS, FlinkSqlOperatorTable.MINUS,
        FlinkSqlOperatorTable.MULTIPLY, FlinkSqlOperatorTable.DIVIDE, FlinkSqlOperatorTable.MOD)) {
      var argument = rex.makeCall(type, operation, List.of(input, input));
      var call = rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN),
          FlinkSqlOperatorTable.IS_ALPHA, List.of(argument));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
      assertNotNull(encoded);
      boolean safe = operation != FlinkSqlOperatorTable.DIVIDE && operation != FlinkSqlOperatorTable.MOD;
      assertEquals(safe, encoded.childCounts()[0] == 0, call.toString());
      if (safe) assertTrue(encoded.jvmFunctionCalls().isEmpty());
      else assertTrue(encoded.jvmFunctionCalls().contains("IS_ALPHA"));
    }
  }

  @Test
  void remainingNumericPredicatesAndFloatingSignaturesStayGenerated() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (var function : List.of(FlinkSqlOperatorTable.IS_ALPHA,
        FlinkSqlOperatorTable.IS_DIGIT, FlinkSqlOperatorTable.IS_DECIMAL)) {
      for (var name : List.of(SqlTypeName.INTEGER, SqlTypeName.DOUBLE)) {
        if (function == FlinkSqlOperatorTable.IS_ALPHA && name == SqlTypeName.INTEGER) continue;
        var call = rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN), function,
            List.of(rex.makeInputRef(types.createSqlType(name), 0)));
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
        assertNotNull(encoded);
        assertTrue(encoded.jvmFunctionCalls().contains(function.getName()));
      }
    }
  }
}
