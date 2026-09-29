package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class TryBooleanCastAdmissionTest {
  @Test
  void plainCharacterInputNeedsNoHostCallbackOrCastConfiguration() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    for (SqlTypeName source : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      var call = rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN),
          FlinkSqlOperatorTable.TRY_CAST,
          List.of(rex.makeInputRef(types.createSqlType(source, 32), 0)));
      var encoded = RexExpression.encode(call);
      assertNotNull(encoded);
      var binding = encoded.udfBinding();
      long[] literals = encoded.longs();
      try {
        assertSame(literals, binding.bind(literals));
      } finally {
        binding.unbind();
      }
    }
  }
}
