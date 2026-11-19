package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class FixedBinaryBase64EncodingTest {
  @Test
  void binaryInputsAndTypedNullsUseTheSharedBinaryEncoder() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (SqlTypeName name : List.of(SqlTypeName.BINARY, SqlTypeName.VARBINARY)) {
      for (int width : new int[] {1, 2, 3, 16, 256}) {
        var type = types.createSqlType(name, width);
        for (var operand : List.of(rex.makeInputRef(type, 0), rex.makeNullLiteral(type))) {
          var call = rex.makeCall(FlinkSqlOperatorTable.TO_BASE64, operand);
          var encoded = RexExpression.encodeProjections(List.of(call), List.of("b"));
          assertNotNull(encoded);
          assertEquals(6, encoded.kinds()[0]);
          assertEquals(151, encoded.payload()[0]);
          assertTrue(encoded.jvmFunctionCalls().isEmpty());
          long[] constants = encoded.longs();
          assertSame(constants, encoded.udfBinding().bind(constants, null));
        }
      }
    }
  }
}
