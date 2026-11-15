package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class RuntimeSha2EncodingTest {
  @Test
  void fixedWidthsUseTheDigestKernelWithoutJvmBindings() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var text = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    int[] widths = {224, 256, 384, 512};
    for (int i = 0; i < widths.length; i++) {
      var call =
          rex.makeCall(
              text,
              FlinkSqlOperatorTable.SHA2,
              List.of(
                  rex.makeInputRef(text, 0), rex.makeExactLiteral(BigDecimal.valueOf(widths[i]))));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
      assertNotNull(encoded);
      assertEquals(6, encoded.kinds()[0]);
      assertEquals(96 + i, encoded.payload()[0]);
      assertEquals(1, encoded.childCounts()[0]);
    }
  }

  @Test
  void runtimeAndNullIntegerWidthsHaveACertifiedNativeRoute() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var text = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var number = types.createSqlType(SqlTypeName.INTEGER);
    for (var bits : List.of(rex.makeInputRef(number, 1), rex.makeNullLiteral(number))) {
      var call =
          rex.makeCall(text, FlinkSqlOperatorTable.SHA2, List.of(rex.makeInputRef(text, 0), bits));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
      assertNotNull(encoded);
      assertEquals(44, encoded.kinds()[0]);
      assertEquals(12, encoded.longs()[2]);
      assertEquals(2, encoded.childCounts()[0]);
    }
  }
}
