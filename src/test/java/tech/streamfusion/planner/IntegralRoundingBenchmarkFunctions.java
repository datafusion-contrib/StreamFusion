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

/** Released generated RowData and preceding native-Calc controls for integer identity rounding. */
public final class IntegralRoundingBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding previousBinding;
  private static RowData[] rows;
  private static String element;
  private static String schema;
  private static int resultCode;
  private static volatile long consumed;

  private IntegralRoundingBenchmarkFunctions() {}

  private static Project project(String type) {
    Thread.currentThread()
        .setContextClassLoader(IntegralRoundingBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE inputs(v " + type + ") WITH ('connector'='datagen')");
    var query =
        ((TableImpl)
                table.sqlQuery(
                    "SELECT "
                        + System.getProperty("sf.integral.rounding.function", "FLOOR")
                        + "(v"
                        + (Integer.getInteger("sf.integral.rounding.position") == null
                            ? ""
                            : "," + Integer.getInteger("sf.integral.rounding.position"))
                        + ") FROM inputs"))
            .getQueryOperation();
    return (Project) ((PlannerQueryOperation) query).getCalciteTree();
  }

  private static FlinkExpressionFunction function(String type) {
    var project = project(type);
    LogicalType valueType =
        LogicalTypeParser.parse(type, IntegralRoundingBenchmarkFunctions.class.getClassLoader());
    return FlinkExpressionGenerator.generate(
        project.getProjects(),
        null,
        new LogicalType[] {valueType},
        RowType.of(valueType),
        org.apache.flink.table.api.TableConfig.getDefault(),
        IntegralRoundingBenchmarkFunctions.class.getClassLoader());
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
                return IntegralRoundingBenchmarkFunctions.class.getClassLoader();
              }
            })[0];
  }

  public static int register(String type) throws Exception {
    var project = project(type);
    var encoded = RexExpression.encodeProjections(project.getProjects(), java.util.List.of("v"));
    if (Integer.getInteger("sf.integral.rounding.position") != null) {
      if (encoded == null
          || encoded.kinds()[0] != 6
          || encoded.payload()[0] != 230
          || !encoded.jvmFunctionCalls().isEmpty()) {
        throw new IllegalStateException("The candidate must use native integer ROUND positions");
      }
      LogicalType valueType =
          LogicalTypeParser.parse(type, IntegralRoundingBenchmarkFunctions.class.getClassLoader());
      var function =
          FlinkExpressionGenerator.generate(
              project.getProjects().get(0),
              new LogicalType[] {valueType},
              org.apache.flink.table.api.TableConfig.getDefault(),
              IntegralRoundingBenchmarkFunctions.class.getClassLoader());
      switch (type) {
        case "TINYINT":
          resultCode = NativeUdf.TYPE_BYTE;
          break;
        case "SMALLINT":
          resultCode = NativeUdf.TYPE_SHORT;
          break;
        case "INT":
          resultCode = NativeUdf.TYPE_INT;
          break;
        case "BIGINT":
          resultCode = NativeUdf.TYPE_LONG;
          break;
        default:
          throw new IllegalArgumentException(type);
      }
      var descriptor =
          NativeUdf.Descriptor.forFunction(
              function,
              FlinkExpressionFunction.class.getMethod("eval", Object[].class),
              new int[] {resultCode},
              resultCode);
      previousBinding =
          new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
      return (int) open(previousBinding);
    }
    if (encoded == null
        || !java.util.Arrays.equals(encoded.kinds(), new int[] {0})
        || !java.util.Arrays.equals(encoded.payload(), new int[] {0})
        || !encoded.jvmFunctionCalls().isEmpty()) {
      throw new IllegalStateException(
          "The candidate must encode verified integer rounding as input");
    }
    var function = function(type);
    schema = Base64.getEncoder().encodeToString(function.arrowResultSchema().serializeAsMessage());
    previousBinding = binding(function);
    return (int) open(previousBinding);
  }

  public static String schema() {
    return schema;
  }

  public static int resultCode() {
    return resultCode;
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
                    type, IntegralRoundingBenchmarkFunctions.class.getClassLoader())));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      Object value;
      long v = i * 7919L;
      switch (type) {
        case "TINYINT":
          value = i % 17 == 0 ? Byte.MIN_VALUE : (byte) v;
          break;
        case "SMALLINT":
          value = i % 17 == 0 ? Short.MIN_VALUE : (short) v;
          break;
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
            case "TINYINT":
              sum += result.getByte(0);
              break;
            case "SMALLINT":
              sum += result.getShort(0);
              break;
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
