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

class BooleanStringCastEncodingTest {
  @Test
  void directBooleanValuesCarryTheConfiguredCaseAndLengthWithoutAnEvaluator() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var bool = types.createSqlType(SqlTypeName.BOOLEAN);
    for (boolean legacy : new boolean[] {false, true}) {
      for (var target : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
        for (int length : new int[] {1, 3, 4, 5, 8, 32, Integer.MAX_VALUE}) {
          for (RexNode input : List.of(rex.makeInputRef(bool, 0), rex.makeLiteral(true),
              rex.makeLiteral(false), rex.makeNullLiteral(bool))) {
            var call = rex.makeCall(types.createSqlType(target, length),
                FlinkSqlOperatorTable.CAST, List.of(input));
            var encoded = configured(call, legacy);
            assertEquals(6, encoded.kinds()[0]);
            assertEquals(244, encoded.payload()[0]);
            assertEquals(3, encoded.childCounts()[0]);
            assertTrue(encoded.jvmFunctionCalls().isEmpty());
            var options = encoded.longs();
            assertEquals(legacy ? 1 : 0, options[options.length - 2]);
            assertEquals(target == SqlTypeName.CHAR ? -length : length, options[options.length - 1]);
          }
        }
      }
    }
  }

  @Test
  void computedOperandsAndTryCastRetainTheirGeneratedEvaluation() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var bool = types.createSqlType(SqlTypeName.BOOLEAN);
    var input = rex.makeInputRef(bool, 0);
    var target = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    for (boolean legacy : new boolean[] {false, true}) {
      for (var call : List.of(
          rex.makeCall(target, FlinkSqlOperatorTable.TRY_CAST, List.of(input)),
          rex.makeCall(target, FlinkSqlOperatorTable.CAST,
              List.of(rex.makeCall(FlinkSqlOperatorTable.NOT, input))))) {
        assertFalse(configured(call, legacy).jvmFunctionCalls().isEmpty());
      }
    }
  }

  static RexExpression configured(RexNode call, boolean legacy) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var config = TableConfig.getDefault();
    config.set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, config);
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    assertTrue((Boolean) emit.invoke(encoded, call), call.toString());
    return encoded;
  }
}
