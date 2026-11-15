package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Prepared released-Flink SQL TRIM and optional generated-callback diagnostic. */
public final class SqlTrimBenchmarkFunctions {
  private static final LogicalType[] TYPES = {
    new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH)
  };
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static volatile long consumed;

  private SqlTrimBenchmarkFunctions() {}

  private static FlinkExpressionFunction function(int direction) throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var text = types.createTypeWithNullability(
        types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    var flag = new SqlTrimFunction.Flag[] {
      SqlTrimFunction.Flag.BOTH, SqlTrimFunction.Flag.LEADING, SqlTrimFunction.Flag.TRAILING
    }[direction];
    var call = rex.makeCall(text, FlinkSqlOperatorTable.TRIM,
        List.of(rex.makeFlag(flag), rex.makeInputRef(text, 0), rex.makeInputRef(text, 1)));
    var result = FlinkExpressionGenerator.generate(call, TYPES, TableConfig.getDefault(),
        SqlTrimBenchmarkFunctions.class.getClassLoader(), false);
    result.open(new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() {
        return SqlTrimBenchmarkFunctions.class.getClassLoader();
      }
    });
    return result;
  }

  public static int register(int direction) throws Exception {
    return NativeUdf.register(function(direction),
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_INTERNAL, NativeUdf.TYPE_INTERNAL}, NativeUdf.TYPE_STRING);
  }

  public static void prepare(int direction, int width, boolean cached, int count, String corpus)
      throws Exception {
    if (stock != null) stock.close();
    stock = function(direction);
    var serializer = new RowDataSerializer(RowType.of(TYPES));
    rows = new RowData[count];
    String unit = corpus.equals("unicode") ? "\u4e2d" : "X";
    String middle = unit.repeat(Math.max(1, width / unit.getBytes(java.nio.charset.StandardCharsets.UTF_8).length));
    for (int i = 0; i < count; i++) {
      String set = new String[] {" ab", "ab", "\t ", "[]+"}[i % 4];
      RowData row = GenericRowData.of(
          i % 11 == 0 ? null : StringData.fromString(set),
          i % 7 == 0 ? null : StringData.fromString(set + middle + set));
      rows[i] = cached ? row : serializer.toBinaryRow(row).copy();
    }
  }

  public static long[] measure(int loops) throws Exception {
    long checksum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData row : rows) {
        StringData value = (StringData) stock.evalRow(row);
        checksum += value == null ? -1 : value.toBytes().length;
      }
    }
    consumed = checksum;
    return new long[] {System.nanoTime() - start, checksum};
  }
}
