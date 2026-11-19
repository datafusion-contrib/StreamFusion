package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class NativeStringFusionTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);

  private RexCall producer(int kind, boolean explicitLength) {
    var string = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var source = rex.makeInputRef(string, 0);
    var number =
        rex.makeInputRef(
            types.createSqlType(kind == 2 ? SqlTypeName.BIGINT : SqlTypeName.INTEGER), 1);
    if (kind < 2)
      return (RexCall)
          rex.makeCall(
              string,
              kind == 0 ? FlinkSqlOperatorTable.LPAD : FlinkSqlOperatorTable.RPAD,
              List.of(source, number, rex.makeLiteral("x")));
    return (RexCall)
        rex.makeCall(
            string,
            SqlStdOperatorTable.OVERLAY,
            explicitLength
                ? List.of(source, rex.makeLiteral("X"), number, number)
                : List.of(source, rex.makeLiteral("X"), number));
  }

  @Test
  void consumersEncodeFlatArgumentsAndRetainTheExactExpression() {
    for (int kind = 0; kind < 3; kind++) {
      for (int consumer = 0; consumer < 3; consumer++) {
        for (boolean reversed : new boolean[] {false, true}) {
          var producer = producer(kind, false);
          RexCall call =
              consumer == 2
                  ? (RexCall)
                      rex.makeCall(
                          producer.getType(),
                          FlinkSqlOperatorTable.REPLACE,
                          List.of(producer, rex.makeLiteral("?"), rex.makeLiteral("Z")))
                  : (RexCall)
                      rex.makeCall(
                          consumer == 0
                              ? SqlStdOperatorTable.EQUALS
                              : SqlStdOperatorTable.NOT_EQUALS,
                          reversed
                              ? List.of(rex.makeLiteral("?"), producer)
                              : List.of(producer, rex.makeLiteral("?")));
          var fusion = NativeStringFusion.match(call);
          assertNotNull(fusion);
          assertEquals(20 + kind + consumer * 3, fusion.operation);
          assertEquals(consumer == 2 ? 5 : 4, fusion.operands.size());
          assertEquals(call, fusion.expression(fusion.operands));
          var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
          assertNotNull(encoded, call.toString());
          assertEquals(44, encoded.kinds()[0]);
          assertEquals(fusion.operation, encoded.longs()[2]);
        }
      }
    }
    var call =
        (RexCall)
            rex.makeCall(SqlStdOperatorTable.EQUALS, producer(2, true), rex.makeLiteral("X?"));
    assertEquals(5, NativeStringFusion.match(call).operands.size());
  }

  @Test
  void dynamicConsumersAndUnverifiedReplacementPatternsRetainExactEvaluation() {
    var producer = producer(0, false);
    RexNode dynamic = rex.makeInputRef(producer.getType(), 2);
    assertNull(
        NativeStringFusion.match(
            (RexCall) rex.makeCall(SqlStdOperatorTable.EQUALS, producer, dynamic)));
    for (String old : List.of("", "ab", "\u00e9")) {
      assertNull(
          NativeStringFusion.match(
              (RexCall)
                  rex.makeCall(
                      producer.getType(),
                      SqlStdOperatorTable.REPLACE,
                      List.of(producer, rex.makeLiteral(old), rex.makeLiteral("Z")))));
    }
    assertNull(
        NativeStringFusion.match(
            (RexCall)
                rex.makeCall(
                    producer.getType(),
                    SqlStdOperatorTable.REPLACE,
                    List.of(producer, rex.makeLiteral("?"), rex.makeLiteral("\u00e9")))));
    for (String replacement : List.of("", "ZZ")) {
      assertNull(
          NativeStringFusion.match(
              (RexCall)
                  rex.makeCall(
                      producer.getType(),
                      FlinkSqlOperatorTable.REPLACE,
                      List.of(producer, rex.makeLiteral("?"), rex.makeLiteral(replacement)))));
    }
    var length =
        rex.makeCall(
            SqlStdOperatorTable.PLUS,
            rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1),
            rex.makeExactLiteral(BigDecimal.ONE));
    var complex =
        rex.makeCall(
            producer.getType(),
            FlinkSqlOperatorTable.LPAD,
            List.of(producer.getOperands().get(0), length, rex.makeLiteral("x")));
    assertNull(
        NativeStringFusion.match(
            (RexCall) rex.makeCall(SqlStdOperatorTable.EQUALS, complex, rex.makeLiteral("?"))));
  }
}
