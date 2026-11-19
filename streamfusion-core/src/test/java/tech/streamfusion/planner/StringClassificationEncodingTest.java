package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class StringClassificationEncodingTest {
  @Test
  void characterInputsAndLiteralsDoNotRegisterAnEvaluator() {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (var function : List.of(FlinkSqlOperatorTable.IS_ALPHA, FlinkSqlOperatorTable.IS_DIGIT,
        FlinkSqlOperatorTable.IS_DECIMAL)) {
      for (var type : List.of(types.createSqlType(SqlTypeName.CHAR, 4),
          types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE))) {
        for (boolean nullable : new boolean[] {false, true}) {
          var input = rex.makeInputRef(types.createTypeWithNullability(type, nullable), 0);
          for (var value : List.of(input, rex.makeNullLiteral(type), rex.makeLiteral("a"))) {
            var call = rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN),
                function, List.of(value));
            var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
            assertNotNull(encoded);
            assertEquals(6, encoded.kinds()[0]);
            assertEquals(function == FlinkSqlOperatorTable.IS_ALPHA ? 238
                : function == FlinkSqlOperatorTable.IS_DIGIT ? 239 : 240, encoded.payload()[0]);
            assertTrue(encoded.jvmFunctionCalls().isEmpty());
            long[] constants = encoded.longs();
            assertSame(constants, encoded.udfBinding().bind(constants, null));
          }
        }
      }
    }
  }
}
