package tech.streamfusion.planner;

import java.util.Base64;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import tech.streamfusion.operator.NativeUdf;

/** Released generated RowData and preceding native-Calc controls for integral SIGN. */
public final class IntegralSignBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding previousBinding;
  private static RowData[] rows;
  private static String element;
  private static String schema;
  private static volatile long consumed;

  private IntegralSignBenchmarkFunctions() {}

  private static FlinkExpressionFunction function(String type) {
    Thread.currentThread()
        .setContextClassLoader(IntegralSignBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE inputs(v " + type + ") WITH ('connector'='datagen')");
    var query = ((TableImpl) table.sqlQuery("SELECT SIGN(v) FROM inputs")).getQueryOperation();
    var project = (Project) ((PlannerQueryOperation) query).getCalciteTree();
    LogicalType valueType =
        LogicalTypeParser.parse(type, IntegralSignBenchmarkFunctions.class.getClassLoader());
    return FlinkExpressionGenerator.generate(
        project.getProjects(),
        null,
        new LogicalType[] {valueType},
        RowType.of(valueType),
        table.getConfig(),
        IntegralSignBenchmarkFunctions.class.getClassLoader());
  }

  private static NativeUdf.Binding binding(FlinkExpressionFunction function) throws Exception {
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_INTERNAL},
            NativeUdf.TYPE_ROW);
    return new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
  }

  private static long open(NativeUdf.Binding binding) {
    return binding
        .bind(
            new long[] {0},
            new FunctionContext(null) {
              @Override
              public ClassLoader getUserCodeClassLoader() {
                return IntegralSignBenchmarkFunctions.class.getClassLoader();
              }
            })[0];
  }

  public static int register(String type) throws Exception {
    var function = function(type);
    schema = Base64.getEncoder().encodeToString(function.arrowResultSchema().serializeAsMessage());
    previousBinding = binding(function);
    return (int) open(previousBinding);
  }

  public static String schema() {
    return schema;
  }

  public static void prepare(String type, boolean nullable, boolean binary, int count)
      throws Exception {
    if (stockBinding != null) stockBinding.unbind();
    element = type;
    stock = function(type);
    stockBinding = binding(stock);
    open(stockBinding);
    var serializer =
        new RowDataSerializer(
            RowType.of(
                LogicalTypeParser.parse(
                    type, IntegralSignBenchmarkFunctions.class.getClassLoader())));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      Object value;
      long v = i * 7919L;
      switch (type) {
        case "INT":
          value = i % 17 == 0 ? Integer.MIN_VALUE : (int) ((i % 2 == 0 ? -1L : 1L) * v);
          break;
        case "BIGINT":
          value = i % 17 == 0 ? Long.MIN_VALUE : (i % 2 == 0 ? -1L : 1L) * (9007199254740993L + v);
          break;
        default:
          throw new IllegalArgumentException(type);
      }
      RowData input = GenericRowData.of(nullable && i % 7 == 0 ? null : value);
      rows[i] = binary ? serializer.toBinaryRow(input).copy() : input;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData input : rows) {
        RowData result = (RowData) stock.evalRow(input);
        if (result.isNullAt(0)) sum--;
        else
          switch (element) {
            case "INT":
              sum += result.getInt(0);
              break;
            case "BIGINT":
              sum += result.getLong(0);
              break;
            default:
              throw new IllegalArgumentException(element);
          }
      }
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() {
    if (stockBinding != null) stockBinding.unbind();
    if (previousBinding != null) previousBinding.unbind();
    stockBinding = null;
    previousBinding = null;
    stock = null;
    rows = null;
  }
}
