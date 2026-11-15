package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.NativeUdf;

/** Released generated RowData and preceding native-Calc controls for decimal SIGN work. */
public final class DecimalSignBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding previousBinding;
  private static RowData[] rows;
  private static int precision;
  private static int scale;
  private static int resultCode;
  private static volatile long consumed;

  private DecimalSignBenchmarkFunctions() {}

  private static Project project(int precision, int scale) {
    Thread.currentThread()
        .setContextClassLoader(DecimalSignBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql(
        "CREATE TABLE inputs(v DECIMAL("
            + precision
            + ","
            + scale
            + ")) WITH ('connector'='datagen')");
    String expression = "SIGN(v)";
    var query =
        ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM inputs")).getQueryOperation();
    return (Project) ((PlannerQueryOperation) query).getCalciteTree();
  }

  private static FlinkExpressionFunction function(int precision, int scale) {
    var project = project(precision, scale);
    var type = new DecimalType(precision, scale);
    return FlinkExpressionGenerator.generate(
        project.getProjects(),
        null,
        new LogicalType[] {type},
        RowType.of(type),
        org.apache.flink.table.api.TableConfig.getDefault(),
        DecimalSignBenchmarkFunctions.class.getClassLoader());
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
                return DecimalSignBenchmarkFunctions.class.getClassLoader();
              }
            })[0];
  }

  public static int register(int precision, int scale) throws Exception {
    var project = project(precision, scale);
    var encoded = RexExpression.encodeProjections(project.getProjects(), java.util.List.of("v"));
    if (encoded == null
        || encoded.kinds()[0] != 6
        || encoded.payload()[0] != 65
        || !encoded.jvmFunctionCalls().isEmpty()) {
      throw new IllegalStateException("The candidate must use the decimal SIGN kernel");
    }
    var function =
        FlinkExpressionGenerator.generate(
            project.getProjects().get(0),
            new LogicalType[] {new DecimalType(precision, scale)},
            org.apache.flink.table.api.TableConfig.getDefault(),
            DecimalSignBenchmarkFunctions.class.getClassLoader());
    resultCode = NativeUdf.decimalType(precision, scale);
    // Match the preceding scalar evaluator, including both decimal marshalling boundaries.
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {resultCode},
            resultCode);
    previousBinding = new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
    return (int) open(previousBinding);
  }

  public static int resultCode() {
    return resultCode;
  }

  public static void prepare(
      int inputPrecision, int inputScale, boolean nullable, boolean binary, int count)
      throws Exception {
    if (stockBinding != null) stockBinding.unbind();
    precision = inputPrecision;
    scale = inputScale;
    stock = function(precision, scale);
    stockBinding = binding(stock);
    open(stockBinding);
    var serializer = new RowDataSerializer(RowType.of(new DecimalType(precision, scale)));
    BigInteger limit = BigInteger.TEN.pow(precision);
    BigInteger max = limit.subtract(BigInteger.ONE);
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      BigInteger value =
          i % 11 == 0
              ? BigInteger.ZERO
              : i % 17 == 0
                  ? max
                  : BigInteger.valueOf((precision > 18 ? 9007199254740993L : 0) + i * 7919L + 1)
                      .remainder(limit);
      if (i % 2 == 0) value = value.negate();
      Object decimal =
          nullable && i % 7 == 0
              ? null
              : DecimalData.fromBigDecimal(new BigDecimal(value, scale), precision, scale);
      RowData input = GenericRowData.of(decimal);
      rows[i] = binary ? serializer.toBinaryRow(input).copy() : input;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData input : rows) {
        RowData result = (RowData) stock.evalRow(input);
        sum +=
            result.isNullAt(0)
                ? -1
                : DecimalDataUtils.signum(result.getDecimal(0, precision, scale));
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
