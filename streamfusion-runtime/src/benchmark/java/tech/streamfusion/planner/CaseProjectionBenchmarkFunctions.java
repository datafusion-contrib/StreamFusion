package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Stock generated SQL expressions for the prepared native casing benchmark. */
public final class CaseProjectionBenchmarkFunctions {
  private static FlinkExpressionFunction evaluator;
  private static RowData[] rows;
  private static volatile long consumed;

  private CaseProjectionBenchmarkFunctions() {}

  public static int prepare(boolean upper, boolean unicode, boolean mixed, int width, int count)
      throws Exception {
    var factory = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(factory);
    var text = factory.createTypeWithNullability(
        factory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    var call = rex.makeCall(text, upper ? SqlStdOperatorTable.UPPER : SqlStdOperatorTable.LOWER,
        List.of(rex.makeInputRef(text, 0)));
    evaluator = FlinkExpressionGenerator.generate(
        call, new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH)},
        TableConfig.getDefault(), CaseProjectionBenchmarkFunctions.class.getClassLoader(), false);
    evaluator.open(new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return CaseProjectionBenchmarkFunctions.class.getClassLoader();
      }
    });
    String unit = unicode ? "aßIΣ中🙂" : "aBI!x";
    int unitBytes = unicode ? 13 : 5;
    String body = unit.repeat(Math.max(1, width / unitBytes));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      String value = body + (i % 16);
      rows[i] = GenericRowData.of(i % 11 == 0 ? null : mixed && i % 2 == 0
          ? BinaryStringData.fromString(value)
          : BinaryStringData.fromBytes(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }
    return NativeUdf.register(evaluator, FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_INTERNAL}, NativeUdf.TYPE_STRING);
  }

  public static long[] measure(int loops) throws Exception {
    long digest = 0;
    long start = System.nanoTime();
    for (int i = 0; i < loops; i++) {
      for (RowData row : rows) {
        var value = (BinaryStringData) evaluator.evalRow(row);
        digest += value == null ? -1 : value.toBytes().length;
      }
    }
    consumed = digest;
    return new long[] {System.nanoTime() - start, digest};
  }
}
