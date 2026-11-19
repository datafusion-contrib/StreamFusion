package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Prepared released-Flink controls for the integer_split Rust benchmark. */
public final class IntegerSplitBenchmarkFunctions {
  private static final LogicalType[] TYPES = {
    new VarCharType(VarCharType.MAX_LENGTH), new IntType(), new IntType()
  };
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static volatile long consumed;

  private IntegerSplitBenchmarkFunctions() {}

  private static FlinkExpressionFunction function() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var text =
        types.createTypeWithNullability(
            types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    var number = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true);
    var call =
        rex.makeCall(
            text,
            FlinkSqlOperatorTable.SPLIT_INDEX,
            List.of(
                rex.makeInputRef(text, 0),
                rex.makeInputRef(number, 1),
                rex.makeInputRef(number, 2)));
    var result =
        FlinkExpressionGenerator.generate(
            call,
            TYPES,
            TableConfig.getDefault(),
            IntegerSplitBenchmarkFunctions.class.getClassLoader(),
            false);
    result.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return IntegerSplitBenchmarkFunctions.class.getClassLoader();
          }
        });
    return result;
  }

  public static int register() throws Exception {
    return NativeUdf.register(
        function(),
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_STRING, NativeUdf.TYPE_INT, NativeUdf.TYPE_INT},
        NativeUdf.TYPE_STRING);
  }

  public static void prepare(String corpus, int width, boolean cached, int count, boolean mixed)
      throws Exception {
    if (stock != null) stock.close();
    stock = function();
    var serializer = new RowDataSerializer(RowType.of(TYPES));
    String unit =
        corpus.equals("cjk") ? "\u4e2d\u6587" : corpus.equals("emoji") ? "\ud83d\ude00" : "ab";
    int repeats =
        Math.max(1, width / unit.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      char delimiter = new char[] {',', '|', '\u00e9'}[i % 3];
      String value =
          "key"
              + i % 16
              + delimiter
              + unit.repeat(repeats)
              + delimiter
              + "tail"
              + i % 16
              + delimiter;
      int code = mixed && i % 17 == 0 ? 256 : delimiter;
      int index = mixed ? i % 7 - 1 : 1;
      RowData row =
          GenericRowData.of(
              i % 7 == 0 ? null : StringData.fromString(value),
              i % 11 == 0 ? null : code,
              i % 13 == 0 ? null : index);
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
