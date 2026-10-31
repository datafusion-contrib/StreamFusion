package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
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
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.operator.NativeUdf;

/** Actual released generated expressions and independently selected production encoders. */
public final class BinaryBoundaryBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding binding;
  private static RowData[] rows;
  private static volatile long consumed;
  private static RexNode call;

  private BinaryBoundaryBenchmarkFunctions() {}

  public static boolean suffix() {
    return Boolean.getBoolean("sf.binary.boundary.suffix");
  }

  public static String functionName() {
    return suffix() ? "ENDSWITH" : "STARTSWITH";
  }

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() {
        return BinaryBoundaryBenchmarkFunctions.class.getClassLoader();
      }
    };
  }

  public static long[] register(int width, boolean fixed) throws Exception {
    Thread.currentThread().setContextClassLoader(BinaryBoundaryBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    String b = fixed ? "BINARY(" + width + ")" : "BYTES";
    String p = fixed ? "BINARY(4)" : "BYTES";
    table.executeSql("CREATE TABLE inputs(b " + b + ",p " + p + ") WITH ('connector'='datagen')");
    var query = ((TableImpl) table.sqlQuery("SELECT " + functionName() + "(b,p) FROM inputs")).getQueryOperation();
    call = ((Project) ((PlannerQueryOperation) query).getCalciteTree()).getProjects().get(0);
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("v"));
    boolean previous = Boolean.getBoolean("sf.binary.boundary.previous");
    if (encoded == null || encoded.kinds().length != 3
        || encoded.kinds()[0] != (previous ? 17 : 6)
        || encoded.childCounts()[0] != 2 || !previous && encoded.payload()[0] != (suffix() ? 236 : 235))
      throw new IllegalStateException("Unexpected binary prefix/suffix route");
    binding = encoded.udfBinding();
    long[] constants = binding.bind(encoded.longs(), context());
    long[] result = new long[constants.length + 2];
    result[0] = encoded.kinds()[0];
    result[1] = encoded.payload()[0];
    System.arraycopy(constants, 0, result, 2, constants.length);
    return result;
  }

  public static void prepare(int width, boolean fixed, boolean nullable, boolean binary, int count)
      throws Exception {
    LogicalType[] types = fixed ? new LogicalType[] {new BinaryType(width), new BinaryType(4)}
        : new LogicalType[] {new VarBinaryType(VarBinaryType.MAX_LENGTH), new VarBinaryType(VarBinaryType.MAX_LENGTH)};
    stock = FlinkExpressionGenerator.generate(call, types, TableConfig.getDefault(),
        BinaryBoundaryBenchmarkFunctions.class.getClassLoader(), false);
    stock.open(context());
    var serializer = new RowDataSerializer(RowType.of(types));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      byte[] b = new byte[width];
      for (int j = 0; j < width; j++) b[j] = (byte) (i * 131 + j * 127);
      int length = fixed ? 4 : i % 5 == 0 ? 0 : i % 7 == 0 ? width + 1 : Math.min(width, 4);
      int start = suffix() ? Math.max(0, b.length - length) : 0;
      byte[] p = Arrays.copyOfRange(b, start, start + length);
      if (i % 3 == 0 && p.length > 0) p[0] ^= 1;
      RowData row = GenericRowData.of(nullable && i % 7 == 0 ? null : b,
          nullable && i % 11 == 0 ? null : p);
      rows[i] = binary ? serializer.toBinaryRow(row).copy() : row;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) for (RowData row : rows) {
      var value = (Boolean) stock.evalRow(row);
      sum += value == null ? -1 : value ? 1 : 0;
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() throws Exception {
    if (stock != null) stock.close();
    if (binding != null) binding.unbind();
  }
}
