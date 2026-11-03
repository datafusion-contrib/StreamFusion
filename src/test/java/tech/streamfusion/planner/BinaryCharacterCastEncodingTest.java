package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class BinaryCharacterCastEncodingTest {
  @Test
  void releasedBinaryToStringCastsReuseTheUnboundUtf8Decoder() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (boolean legacy : new boolean[] {false, true}) {
      for (var source : List.of(types.createSqlType(SqlTypeName.BINARY, 8),
          types.createSqlType(SqlTypeName.VARBINARY, 8))) {
        for (var target : List.of(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
            types.createSqlType(SqlTypeName.CHAR, 1), types.createSqlType(SqlTypeName.VARCHAR, 1))) {
          boolean bounded = !legacy && target.getPrecision() != Integer.MAX_VALUE;
          for (var operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
            if (bounded && operator == FlinkSqlOperatorTable.TRY_CAST) continue;
            RexNode call = rex.makeCall(target, operator, List.of(rex.makeInputRef(source, 0)));
            var encoded = encode(call, legacy);
            assertEquals(237, encoded.payload()[0]);
            assertArrayEquals(new String[0], encoded.strings());
            assertTrue(encoded.jvmFunctionCalls().isEmpty());
            long[] literals = encoded.longs();
            assertSame(literals, encoded.udfBinding().bind(literals, null));
            if (!bounded) {
              assertArrayEquals(new int[] {6, 0}, encoded.kinds());
              continue;
            }
            // CHAR(n) pads, so its length literal is negative.
            assertEquals(3, encoded.kinds().length);
            assertEquals(2, encoded.childCounts()[0]);
            assertArrayEquals(
                new long[] {target.getSqlTypeName() == SqlTypeName.CHAR ? -1 : 1}, literals);
          }
        }
      }
    }
  }

  private static RexExpression encode(RexNode call, boolean legacy) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var config = TableConfig.getDefault();
    config.getConfiguration().setString("table.exec.legacy-cast-behaviour",
        legacy ? "ENABLED" : "DISABLED");
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, config);
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    assertTrue((Boolean) emit.invoke(encoded, call), call.toString());
    return encoded;
  }
}
