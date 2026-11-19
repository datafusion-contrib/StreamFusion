package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IntegerSplitIndexEncodingTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void directIntegerOverloadUsesTheCacheAwareRustCertificate() {
    var text = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var number = types.createSqlType(SqlTypeName.INTEGER);
    var call =
        rex.makeCall(
            text,
            FlinkSqlOperatorTable.SPLIT_INDEX,
            List.of(
                rex.makeInputRef(text, 0),
                rex.makeInputRef(number, 1),
                rex.makeInputRef(number, 2)));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    assertEquals(44, encoded.kinds()[0]);
    assertEquals(11, encoded.longs()[2]);
    assertEquals(3, encoded.childCounts()[0]);
  }

  @Test
  void characterOverloadKeepsItsExistingNativeKernel() {
    var text = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var number = types.createSqlType(SqlTypeName.INTEGER);
    var call =
        rex.makeCall(
            text,
            FlinkSqlOperatorTable.SPLIT_INDEX,
            List.of(
                rex.makeInputRef(text, 0), rex.makeInputRef(text, 1), rex.makeInputRef(number, 2)));
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    assertNotNull(encoded);
    assertEquals(6, encoded.kinds()[0]);
    assertEquals(130, encoded.payload()[0]);
  }
}
