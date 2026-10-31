package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Released generated RowData control and actual planner binding for native classification. */
public final class StringClassificationBenchmarkFunctions {
  private static final LogicalType[] TYPES = {new VarCharType(VarCharType.MAX_LENGTH)};
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding binding;
  private static RowData[] rows;
  private static volatile long consumed;

  private StringClassificationBenchmarkFunctions() {}

  private static RexCall call() {
    var types = new FlinkTypeFactory(StringClassificationBenchmarkFunctions.class.getClassLoader(),
        FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var input = types.createTypeWithNullability(
        types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    return (RexCall) rex.makeCall(types.createSqlType(SqlTypeName.BOOLEAN),
        FlinkSqlOperatorTable.IS_ALPHA, List.of(rex.makeInputRef(input, 0)));
  }

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() {
        return StringClassificationBenchmarkFunctions.class.getClassLoader();
      }
    };
  }

  public static long[] register() throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, TableConfig.getDefault());
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    if (!(Boolean) emit.invoke(encoded, call()) || encoded.kinds().length != 2)
      throw new IllegalStateException("Expected one classification call over a runtime input");
    boolean previous = Boolean.getBoolean("sf.classification.previous");
    if (encoded.kinds()[0] != (previous ? 17 : 6)
        || !previous && encoded.payload()[0] != 238)
      throw new IllegalStateException("Unexpected classification route");
    binding = encoded.udfBinding();
    long[] constants = binding.bind(encoded.longs(), context());
    long[] result = new long[constants.length + 2];
    result[0] = encoded.kinds()[0];
    result[1] = encoded.payload()[0];
    System.arraycopy(constants, 0, result, 2, constants.length);
    return result;
  }

  public static String value(String corpus, int width, int row) {
    String unit = corpus.equals("bmp") || corpus.equals("unicode") ? "\u4e2d\u6587\u00e9\u03a9" : "abcXYZ";
    String body = unit.repeat((width + unit.length() - 1) / unit.length()).substring(0, width);
    if (corpus.equals("supplementary")) return "a\ud801\udc00" + body;
    if (corpus.equals("mixed") || corpus.equals("unicode")) {
      if (row % 17 == 0) return "";
      if (row % 3 == 0) return body + (corpus.equals("unicode") ? "\u0301" : "7");
    }
    return (char) ('a' + row % 26) + body;
  }

  public static void prepare(String corpus, int width, boolean nullable, boolean binary, int count)
      throws Exception {
    stock = FlinkExpressionGenerator.generate(call(), TYPES, TableConfig.getDefault(),
        StringClassificationBenchmarkFunctions.class.getClassLoader(), false);
    stock.open(context());
    var serializer = new RowDataSerializer(RowType.of(TYPES));
    rows = new RowData[count];
    for (int row = 0; row < count; row++) {
      RowData value = GenericRowData.of(nullable && row % 7 == 0 ? null
          : StringData.fromString(value(corpus, width, row)));
      rows[row] = binary ? serializer.toBinaryRow(value).copy() : value;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long checksum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData row : rows) checksum += Boolean.TRUE.equals(stock.evalRow(row)) ? 1 : 0;
    }
    consumed = checksum;
    return new long[] {System.nanoTime() - start, checksum};
  }

  public static void close() throws Exception {
    if (stock != null) stock.close();
    if (binding != null) binding.unbind();
  }
}
