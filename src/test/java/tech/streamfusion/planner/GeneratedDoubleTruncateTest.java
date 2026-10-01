package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GeneratedDoubleTruncateTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void generatedDefaultAndDynamicScalesPreserveBitsNullsAndFailures(boolean explicit)
      throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var valueType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DOUBLE), true);
    var scaleType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true);
    var value = rex.makeInputRef(valueType, 0);
    var scale = rex.makeInputRef(scaleType, 1);
    var call = (RexCall) rex.makeCall(valueType, FlinkSqlOperatorTable.TRUNCATE,
        explicit ? List.of(value, scale) : List.of(value));
    var arguments = explicit ? new LogicalType[] {new DoubleType(), new IntType()}
        : new LogicalType[] {new DoubleType()};
    var function = FlinkExpressionFunction.doubleTruncate(call, arguments,
        TableConfig.getDefault(), getClass().getClassLoader());
    function.open(new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return GeneratedDoubleTruncateTest.class.getClassLoader();
      }
    });
    try {
      for (Double input : new Double[] {null, -0.0, 0.0, 0.46, -0.46, 1.0, -1.0,
          Math.nextDown(1.0), 1e9, Math.nextUp(1e9), 1000.5, Math.nextDown(1000.5),
          Math.nextUp(1000.5), Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
          Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
        for (Integer position : new Integer[] {null, -6, -3, 0, 1, 3, 6,
            Integer.MIN_VALUE, Integer.MAX_VALUE}) {
          Integer effective = explicit ? position : Integer.valueOf(0);
          Object[] values = explicit ? new Object[] {input, position} : new Object[] {input};
          Double expected;
          try {
            expected = input == null || effective == null
                ? null : SqlFunctionUtils.struncate(input, effective);
          } catch (RuntimeException failure) {
            var actual = assertThrows(failure.getClass(), () -> function.eval(values));
            assertEquals(failure.getMessage(), actual.getMessage());
            continue;
          }
          Object actual = function.eval(values);
          if (expected == null) assertNull(actual);
          else assertEquals(Double.doubleToLongBits(expected),
              Double.doubleToLongBits((Double) actual));
        }
      }
    } finally {
      function.close();
    }
  }
}
