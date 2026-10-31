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
  void boundedTargetsAndComputedDecimalsKeepTheirExistingEvaluator() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.DECIMAL, 18, 3);
    var input = rex.makeInputRef(type, 0);
    for (var target : List.of(types.createSqlType(SqlTypeName.CHAR, 8),
        types.createSqlType(SqlTypeName.VARCHAR, 4))) {
      var call = rex.makeCall(target, FlinkSqlOperatorTable.CAST, List.of(input));
      var encoded = configured(call);
      assertNotNull(encoded);
      assertEquals(17, encoded.kinds()[0]);
    }
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
