package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class FromBase64EncodingTest {
  @Test
  void finalCharacterAndBinaryDecodingHasNoScalarBinding() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (var name :
        List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR, SqlTypeName.BINARY, SqlTypeName.VARBINARY)) {
      var type = types.createSqlType(name, 8);
      for (var input : List.of(rex.makeInputRef(type, 0), rex.makeNullLiteral(type))) {
        var call = rex.makeCall(FlinkSqlOperatorTable.FROM_BASE64, input);
        var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
        assertNotNull(encoded);
        assertEquals(6, encoded.kinds()[0]);
        assertEquals(232, encoded.payload()[0]);
        assertTrue(encoded.isBinaryStringProjection(0));
        assertTrue(encoded.jvmFunctionCalls().isEmpty());
        var constants = encoded.longs();
        assertSame(constants, encoded.udfBinding().bind(constants, null));
      }
    }
  }
}
