package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Prepared released-Flink RowData and preceding scalar-callback controls. */
public final class FixedBinaryBase64BenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding previousBinding;
  private static RowData[] rows;
  private static volatile long consumed;

  private FixedBinaryBase64BenchmarkFunctions() {}

  private static Project project(int width) {
    Thread.currentThread()
        .setContextClassLoader(FixedBinaryBase64BenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE inputs(v BINARY(" + width + ")) WITH ('connector'='datagen')");
    var query = ((TableImpl) table.sqlQuery("SELECT TO_BASE64(v) FROM inputs")).getQueryOperation();
    return (Project) ((PlannerQueryOperation) query).getCalciteTree();
  }

  private static long open(NativeUdf.Binding binding) {
    return binding
        .bind(
            new long[] {0},
            new FunctionContext(null) {
              @Override
              public ClassLoader getUserCodeClassLoader() {
                return FixedBinaryBase64BenchmarkFunctions.class.getClassLoader();
              }
            })[0];
  }

  public static int register(int width) throws Exception {
    var project = project(width);
    var encoded = RexExpression.encodeProjections(project.getProjects(), List.of("v"));
    if (encoded == null
        || encoded.kinds()[0] != 6
        || encoded.payload()[0] != 151
        || !encoded.jvmFunctionCalls().isEmpty()) {
      throw new IllegalStateException("Expected the native binary TO_BASE64 encoder");
    }
    var function =
        FlinkExpressionGenerator.generate(
            project.getProjects().get(0),
            new LogicalType[] {new BinaryType(width)},
            TableConfig.getDefault(),
            FixedBinaryBase64BenchmarkFunctions.class.getClassLoader());
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_BINARY},
            NativeUdf.TYPE_STRING);
    previousBinding = new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
    return (int) open(previousBinding);
  }

  public static void prepare(int width, boolean nullable, boolean binary, int count)
      throws Exception {
    var project = project(width);
    stock =
        FlinkExpressionGenerator.generate(
            project.getProjects(),
            null,
            new LogicalType[] {new BinaryType(width)},
            RowType.of(new VarCharType(VarCharType.MAX_LENGTH)),
            TableConfig.getDefault(),
            FixedBinaryBase64BenchmarkFunctions.class.getClassLoader());
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            stock,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_INTERNAL},
            NativeUdf.TYPE_ROW);
    stockBinding = new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
    open(stockBinding);
    var serializer = new RowDataSerializer(RowType.of(new BinaryType(width)));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      byte[] bytes = new byte[width];
      for (int j = 0; j < width; j++) bytes[j] = (byte) (i * 131 + j * 127);
      RowData input = GenericRowData.of(nullable && i % 7 == 0 ? null : bytes);
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
        else {
          String text = result.getString(0).toString();
          sum += text.length() + text.charAt(0) + text.charAt(text.length() - 1);
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
