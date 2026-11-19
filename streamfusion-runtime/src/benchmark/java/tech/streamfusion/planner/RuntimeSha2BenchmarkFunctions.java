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

/** Prepared released-Flink controls for the runtime_sha2 Rust benchmark. */
public final class RuntimeSha2BenchmarkFunctions {
  private static final LogicalType[] TYPES = {
    new VarCharType(VarCharType.MAX_LENGTH), new IntType()
  };
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static volatile long consumed;

  private RuntimeSha2BenchmarkFunctions() {}

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
            FlinkSqlOperatorTable.SHA2,
            List.of(rex.makeInputRef(text, 0), rex.makeInputRef(number, 1)));
    var result =
        FlinkExpressionGenerator.generate(
            call,
            TYPES,
            TableConfig.getDefault(),
            RuntimeSha2BenchmarkFunctions.class.getClassLoader(),
            false);
    result.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return RuntimeSha2BenchmarkFunctions.class.getClassLoader();
          }
        });
    return result;
  }

  public static int register() throws Exception {
    return NativeUdf.register(
        function(),
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_STRING, NativeUdf.TYPE_INT},
        NativeUdf.TYPE_STRING);
  }

  public static void prepare(String corpus, int width, boolean cached, int count) throws Exception {
    if (stock != null) stock.close();
    stock = function();
    var serializer = new RowDataSerializer(RowType.of(TYPES));
    String unit =
        corpus.equals("cjk") ? "\u4e2d\u6587" : corpus.equals("emoji") ? "\ud83d\ude00" : "ab";
    int repeats =
        Math.max(1, width / unit.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      RowData row =
          GenericRowData.of(
              i % 7 == 0 ? null : StringData.fromString("key" + i % 16 + unit.repeat(repeats)),
              i % 11 == 0 ? null : new int[] {224, 256, 384, 512}[i % 4]);
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
