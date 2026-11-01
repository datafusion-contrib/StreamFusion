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

class DecimalPrecisionProjectionEncodingTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void onlyTheAffectedProjectionUsesGeneratedEvaluation() {
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var abs =
        rex.makeCall(integer, FlinkSqlOperatorTable.ABS, List.of(rex.makeInputRef(integer, 1)));
    for (var name :
        List.of(
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.FLOAT,
            SqlTypeName.DOUBLE,
            SqlTypeName.DECIMAL)) {
      var inputType =
          name == SqlTypeName.DECIMAL
              ? types.createSqlType(name, 18, 2)
              : types.createSqlType(name);
      var cast =
          rex.makeCall(
              types.createSqlType(SqlTypeName.DECIMAL, 1, 0),
              FlinkSqlOperatorTable.CAST,
              List.of(rex.makeInputRef(inputType, 0)));
      var encoded =
          RexExpression.encodeProjections(List.of(positive(cast), abs), List.of("v", "a"));
      assertNotNull(encoded, name.toString());
      assertArrayEquals(new int[] {17, 0, 6, 0}, encoded.kinds(), name.toString());
      assertTrue(encoded.jvmFunctionCalls().contains("CAST"));
      assertTrue(encoded.nativeFunctionCalls().contains("ABS"));
      assertFalse(encoded.requiresWholeStructInputs());
    }
  }

  @Test
  void losslessCastsStayNativeButRoundingCarryKeepsTheConsumer() {
    for (var name :
        List.of(
            SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      var input = rex.makeInputRef(types.createSqlType(name), 0);
      var cast =
          rex.makeCall(
              types.createSqlType(SqlTypeName.DECIMAL, 38, 0),
              FlinkSqlOperatorTable.CAST,
              List.of(input));
      var encoded = RexExpression.encodeProjections(List.of(positive(cast)), List.of("v"));
      assertNotNull(encoded);
      assertTrue(encoded.jvmFunctionCalls().isEmpty(), name.toString());
    }
    var input = rex.makeInputRef(types.createSqlType(SqlTypeName.DECIMAL, 3, 2), 0);
    for (int[] target : new int[][] {{1, 0}, {2, 0}, {3, 2}, {4, 3}}) {
      var cast =
          rex.makeCall(
              types.createSqlType(SqlTypeName.DECIMAL, target[0], target[1]),
              FlinkSqlOperatorTable.CAST,
              List.of(input));
      var encoded = RexExpression.encodeProjections(List.of(positive(cast)), List.of("v"));
      assertNotNull(encoded);
      assertEquals(target[0] == 1, encoded.jvmFunctionCalls().contains("CAST"));
    }
  }

  private RexNode positive(RexNode value) {
    return rex.makeCall(
        types.createSqlType(SqlTypeName.BOOLEAN),
        FlinkSqlOperatorTable.GREATER_THAN,
        List.of(value, rex.makeExactLiteral(BigDecimal.ZERO)));
  }
}
