package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import tech.streamfusion.operator.NativeUdf;

/** Builds the production generated evaluator outside Criterion's timed boundary. */
public final class TruncateBenchmarkFunctions {
  private TruncateBenchmarkFunctions() {}

  public static int registerGenerated() throws Exception {
    return register(false);
  }

  public static int registerBorrowed() throws Exception {
    return register(true);
  }

  private static int register(boolean shortcut) throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var value = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DOUBLE), true);
    var scale = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true);
    var call = (RexCall) rex.makeCall(value, FlinkSqlOperatorTable.TRUNCATE,
        List.of(rex.makeInputRef(value, 0), rex.makeInputRef(scale, 1)));
    var arguments = new LogicalType[] {new DoubleType(), new IntType()};
    var loader = TruncateBenchmarkFunctions.class.getClassLoader();
    var function = shortcut
        ? FlinkExpressionFunction.doubleTruncate(call, arguments, TableConfig.getDefault(), loader)
        : new FlinkExpressionFunction(call, arguments, TableConfig.getDefault(), loader, false, false);
    function.open(new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return TruncateBenchmarkFunctions.class.getClassLoader();
      }
    });
    return NativeUdf.register(function,
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_DOUBLE, NativeUdf.TYPE_INT}, NativeUdf.TYPE_DOUBLE);
  }
}
