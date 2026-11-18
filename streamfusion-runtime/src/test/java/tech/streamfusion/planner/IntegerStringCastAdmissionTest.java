package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IntegerStringCastAdmissionTest {
  @Test
  void supportedPairsEncodeWithoutAnyHostCallback() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    for (boolean legacy : new boolean[] {false, true}) {
      for (SqlTypeName integer : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT,
          SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
        var numeric = types.createSqlType(integer);
        for (var text : List.of(types.createSqlType(SqlTypeName.CHAR, 8),
            types.createSqlType(SqlTypeName.VARCHAR, 4),
            types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE))) {
          for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
            assertNative(rex.makeCall(numeric, operator, List.of(rex.makeInputRef(text, 0))), legacy, 33);
            assertNative(rex.makeCall(text, operator, List.of(rex.makeInputRef(numeric, 0))), legacy, 34);
          }
        }
      }
    }
  }

  @Test
  void unrelatedFloatStringPairStillUsesTheHostCast() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var call = rex.makeCall(types.createSqlType(SqlTypeName.DOUBLE), FlinkSqlOperatorTable.CAST,
        List.of(rex.makeInputRef(types.createSqlType(SqlTypeName.VARCHAR, 32), 0)));
    assertEquals(17, encode(call, false).kinds()[0]);
  }

  private static void assertNative(RexNode call, boolean legacy, int kind) throws Exception {
    var encoded = encode(call, legacy);
    assertNotNull(encoded);
    assertEquals(kind, encoded.kinds()[0]);
    long[] literals = encoded.longs();
    var binding = encoded.udfBinding();
    try {
      assertSame(literals, binding.bind(literals), "Native casts must have an empty UDF binding");
    } finally {
      binding.unbind();
    }
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
