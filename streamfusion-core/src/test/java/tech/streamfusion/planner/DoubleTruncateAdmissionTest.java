package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class DoubleTruncateAdmissionTest {
  @Test
  void defaultScaleIsGeneratedAndDynamicScaleRemainsAnIntInput() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var result = types.createSqlType(SqlTypeName.DOUBLE);
    var value = rex.makeInputRef(result, 0);
    var scale = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1);
    for (boolean explicit : new boolean[] {false, true}) {
      var call = rex.makeCall(result, FlinkSqlOperatorTable.TRUNCATE,
          explicit ? List.of(value, scale) : List.of(value));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("value"));
      assertNotNull(encoded);
      assertArrayEquals(explicit ? new int[] {17, 0, 0} : new int[] {17, 0}, encoded.kinds());
      assertEquals(3, encoded.longs()[1]);
    }
  }
}
