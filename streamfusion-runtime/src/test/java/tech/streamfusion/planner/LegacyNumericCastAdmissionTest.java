package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class LegacyNumericCastAdmissionTest {
  @Test
  void onlyDirectVerifiedCastsReceiveTheNativeBatchShortcut() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    for (var source : List.of(types.createSqlType(SqlTypeName.FLOAT),
        types.createSqlType(SqlTypeName.DOUBLE), types.createSqlType(SqlTypeName.CHAR, 8))) {
      var target = source.getSqlTypeName() == SqlTypeName.CHAR
          ? types.createSqlType(SqlTypeName.DOUBLE) : types.createSqlType(SqlTypeName.DECIMAL, 10, 1);
      var cast = rex.makeCall(target, FlinkSqlOperatorTable.CAST,
          List.of(rex.makeInputRef(source, 0)));
      assertEquals(42, encode(cast, true).kinds()[0], cast.toString());
      assertEquals(17, encode(cast, false).kinds()[0], cast.toString());
    }
    var number = types.createSqlType(SqlTypeName.DOUBLE);
    var sum = rex.makeCall(number, SqlStdOperatorTable.PLUS,
        List.of(rex.makeInputRef(number, 0), rex.makeInputRef(number, 1)));
    var composed = rex.makeCall(types.createSqlType(SqlTypeName.DECIMAL, 15, 4),
        FlinkSqlOperatorTable.CAST, List.of(sum));
    assertEquals(17, encode(composed, true).kinds()[0],
        "A composed operand must execute before its cast");
  }

  private static RexExpression encode(RexNode call, boolean legacy) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var config = TableConfig.getDefault();
    config.getConfiguration().setString("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, config);
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    assertTrue((Boolean) emit.invoke(encoded, call), call.toString());
    return encoded;
  }
}
