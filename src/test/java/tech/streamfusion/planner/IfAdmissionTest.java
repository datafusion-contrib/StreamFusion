package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IfAdmissionTest {
  @Test
  void booleanBranchesUseNativeCaseLowering() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.BOOLEAN);
    var condition = rex.makeInputRef(type, 0);
    var left = rex.makeInputRef(type, 1);
    var right = rex.makeInputRef(type, 2);
    var call = rex.makeCall(type, FlinkSqlOperatorTable.IF, List.of(condition, left, right));
    assertNotNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
  }

  @Test
  void unverifiedResultTypesStayOutsideNativeCaseLowering() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var condition = rex.makeInputRef(types.createSqlType(SqlTypeName.BOOLEAN), 0);
    for (var type :
        List.of(
            types.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
            types.createArrayType(types.createSqlType(SqlTypeName.INTEGER), -1))) {
      var branch = rex.makeInputRef(type, 1);
      var call = rex.makeCall(type, FlinkSqlOperatorTable.IF, List.of(condition, branch, branch));
      assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")), type.toString());
    }
  }
}
