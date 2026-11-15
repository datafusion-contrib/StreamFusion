package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class DecimalSignConsumerEncodingTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void compactZeroIntegerWidthSignAndCastUseOneGeneratedExpression() throws Exception {
    for (int precision = 1; precision <= 18; precision++) {
      var decimal = types.createSqlType(SqlTypeName.DECIMAL, precision, precision);
      var input = rex.makeInputRef(decimal, 0);
      var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN, List.of(input));
      var cast = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
          FlinkSqlOperatorTable.CAST, List.of(sign));
      var encoded = encode(cast);
      assertArrayEquals(new int[] {17, 0}, encoded.kinds());
      assertEquals(List.of("CAST", "SIGN"), encoded.jvmFunctionCalls());
    }
  }

  @Test
  void compactSignSelectionsKeepReleasedBranchPrecisionValidation() throws Exception {
    var decimal = types.createSqlType(SqlTypeName.DECIMAL, 1, 1);
    var input = rex.makeInputRef(decimal, 0);
    var condition = rex.makeInputRef(types.createSqlType(SqlTypeName.BOOLEAN), 1);
    var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN, List.of(input));
    var selection = rex.makeCall(decimal, FlinkSqlOperatorTable.CASE,
        List.of(condition, sign, input));
    var encoded = encode(selection);
    assertEquals(17, encoded.kinds()[0]);
    assertTrue(encoded.jvmFunctionCalls().contains("SIGN"));
    assertTrue(java.util.Arrays.stream(encoded.kinds()).skip(1).allMatch(kind -> kind == 0));
  }

  @Test
  void verifiedDecimalSignsRunNativelyBeforeTheGeneratedCast() throws Exception {
    for (int[] pair : new int[][] {{18, 3}, {19, 19}, {38, 38}}) {
      var decimal = types.createSqlType(SqlTypeName.DECIMAL, pair[0], pair[1]);
      var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN,
          List.of(rex.makeInputRef(decimal, 0)));
      var cast = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
          FlinkSqlOperatorTable.CAST, List.of(sign));
      assertArrayEquals(new int[] {17, 6, 0}, encode(cast).kinds());
      assertArrayEquals(new int[] {6, 0}, encode(sign).kinds());
    }
  }

  @Test
  void unrelatedProjectionsKeepNativeEvaluation() throws Exception {
    var decimal = types.createSqlType(SqlTypeName.DECIMAL, 1, 1);
    var input = rex.makeInputRef(decimal, 0);
    var sign = rex.makeCall(decimal, FlinkSqlOperatorTable.SIGN, List.of(input));
    var cast = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
        FlinkSqlOperatorTable.CAST, List.of(sign));
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var abs = rex.makeCall(integer, FlinkSqlOperatorTable.ABS,
        List.of(rex.makeInputRef(integer, 1)));
    var encoded = encode(cast, abs);
    assertArrayEquals(new int[] {17, 0, 6, 0}, encoded.kinds());
    assertTrue(encoded.nativeFunctionCalls().contains("ABS"));
  }

  private static RexExpression encode(RexNode... expressions) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, TableConfig.getDefault());
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    for (var expression : expressions)
      assertTrue((Boolean) emit.invoke(encoded, expression), expression.toString());
    return encoded;
  }
}
