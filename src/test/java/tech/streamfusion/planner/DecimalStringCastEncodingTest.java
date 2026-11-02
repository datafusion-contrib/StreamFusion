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

class DecimalStringCastEncodingTest {
  @Test
  void everyPrecisionScaleAndLiteralUsesAnUnboundNativeCast() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var string = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        var type = types.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        var max = new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
        for (RexNode value : List.of(rex.makeInputRef(type, 0),
            rex.makeExactLiteral(max.negate(), type), rex.makeNullLiteral(type))) {
          for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
            var call = rex.makeCall(string, operator, List.of(value));
            var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
            assertNotNull(encoded, call.toString());
            assertEquals(11, encoded.kinds()[0]);
            assertEquals(6, encoded.payload()[0]);
            assertTrue(encoded.jvmFunctionCalls().isEmpty());
            long[] constants = encoded.longs();
            assertSame(constants, encoded.udfBinding().bind(constants, null));
          }
        }
      }
    }
  }

  @Test
  void boundedTargetsReuseFormattingAndCharacterWidthKernels() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    var input = rex.makeInputRef(type, 0);
    for (boolean legacy : new boolean[] {false, true}) {
      for (var name : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
        for (int length : new int[] {1, 3, 8, 20, 44, 80, Integer.MAX_VALUE}) {
          for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
            var call = rex.makeCall(types.createSqlType(name, length), operator, List.of(input));
            var encoded = BooleanStringCastEncodingTest.configured(call, legacy);
            assertTrue(encoded.jvmFunctionCalls().isEmpty());
            assertEquals(legacy || length == Integer.MAX_VALUE ? 11 : 6, encoded.kinds()[0]);
            assertEquals(legacy || length == Integer.MAX_VALUE ? 6
                : name == SqlTypeName.CHAR ? 129 : 126, encoded.payload()[0]);
          }
        }
      }
    }
  }

  @Test
  void boundedTryCastStillRequiresTheConfiguredLengthSemantics() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var input = rex.makeInputRef(types.createSqlType(SqlTypeName.DECIMAL, 18, 3), 0);
    for (var name : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      var call = rex.makeCall(types.createSqlType(name, 8), FlinkSqlOperatorTable.TRY_CAST,
          List.of(input));
      assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
    }
  }

  @Test
  void verifiedUnaryCastsPreservePrecisionScaleAndLengthRules() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (int[] pair : new int[][] {{1, 1}, {4, 3}, {18, 0}, {18, 18}, {19, 0}, {38, 38}}) {
      var type = types.createSqlType(SqlTypeName.DECIMAL, pair[0], pair[1]);
      var wide = types.createSqlType(SqlTypeName.DECIMAL, 38, pair[1]);
      var input = rex.makeInputRef(type, 0);
      for (var unary : List.of(FlinkSqlOperatorTable.ABS, FlinkSqlOperatorTable.UNARY_MINUS)) {
        var once = rex.makeCall(type, unary, List.of(input));
        for (var operand : List.of(once, rex.makeCall(type, unary, List.of(once)),
            rex.makeCall(wide, FlinkSqlOperatorTable.CAST, List.of(once)),
            rex.makeCall(wide, unary,
                List.of(rex.makeCall(wide, FlinkSqlOperatorTable.CAST, List.of(input)))))) {
          for (boolean legacy : new boolean[] {false, true}) {
            for (var name : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
              for (int length : new int[] {1, 8, 44, Integer.MAX_VALUE}) {
                for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
                  var call = rex.makeCall(types.createSqlType(name, length), operator, List.of(operand));
                  var encoded = BooleanStringCastEncodingTest.configured(call, legacy);
                  assertTrue(encoded.jvmFunctionCalls().isEmpty(), call.toString());
                  assertEquals(legacy || length == Integer.MAX_VALUE ? 11 : 6, encoded.kinds()[0]);
                  assertEquals(legacy || length == Integer.MAX_VALUE ? 6
                      : name == SqlTypeName.CHAR ? 129 : 126, encoded.payload()[0]);
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  void computedDecimalsKeepTheirExistingEvaluator() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    var input = rex.makeInputRef(type, 0);
    var sum = rex.makeCall(type, FlinkSqlOperatorTable.PLUS,
        List.of(input, rex.makeExactLiteral(BigDecimal.ONE, type)));
    var call = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
        FlinkSqlOperatorTable.CAST, List.of(sum));
    var encoded = configured(call);
    assertNotNull(encoded);
    assertEquals(17, encoded.kinds()[0]);
  }

  private static RexExpression configured(RexNode call) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var configure = RexExpression.class.getDeclaredMethod("configure",
        org.apache.flink.table.api.TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, org.apache.flink.table.api.TableConfig.getDefault());
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    assertTrue((Boolean) emit.invoke(encoded, call));
    return encoded;
  }
}
