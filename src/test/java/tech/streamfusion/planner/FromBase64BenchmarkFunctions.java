package tech.streamfusion.planner;

import java.util.Base64;
import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.operator.NativeUdf;

/** Runs the encoder on the supplied classpath, so previous controls use the actual old binding. */
public final class FromBase64BenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding encodedBinding;
  private static RowData[] rows;
  private static volatile long consumed;

  private FromBase64BenchmarkFunctions() {}

  private static LogicalType inputType(boolean binary) {
    return binary
        ? new VarBinaryType(VarBinaryType.MAX_LENGTH)
        : new VarCharType(VarCharType.MAX_LENGTH);
  }

  private static Project project(boolean binary) {
    Thread.currentThread()
        .setContextClassLoader(FromBase64BenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql(
        "CREATE TABLE inputs(v "
            + (binary ? "BYTES" : "STRING")
            + ") WITH ('connector'='datagen')");
    var query =
        ((TableImpl) table.sqlQuery("SELECT FROM_BASE64(v) FROM inputs")).getQueryOperation();
    return (Project) ((PlannerQueryOperation) query).getCalciteTree();
  }

  private static long[] open(NativeUdf.Binding binding, long[] constants) {
    return binding.bind(
        constants,
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return FromBase64BenchmarkFunctions.class.getClassLoader();
          }
        });
  }

  public static long[] register(boolean binary) {
    var project = project(binary);
    var encoded = RexExpression.encodeProjections(project.getProjects(), List.of("v"));
    if (encoded == null || encoded.kinds().length != 2 || encoded.childCounts()[0] != 1)
      throw new IllegalStateException("Expected a direct FROM_BASE64 projection");
    encodedBinding = encoded.udfBinding();
    long[] constants = open(encodedBinding, encoded.longs());
    long[] result = new long[constants.length + 2];
    result[0] = encoded.kinds()[0];
    result[1] = encoded.payload()[0];
    System.arraycopy(constants, 0, result, 2, constants.length);
    return result;
  }

  public static void prepare(
      int width, boolean nullable, boolean binary, boolean binaryRow, int count) throws Exception {
    var project = project(binary);
    var type = inputType(binary);
    stock =
        FlinkExpressionGenerator.generate(
            project.getProjects(),
            null,
            new LogicalType[] {type},
            RowType.of(new VarCharType(VarCharType.MAX_LENGTH)),
            TableConfig.getDefault(),
            FromBase64BenchmarkFunctions.class.getClassLoader());
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            stock,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_INTERNAL},
            NativeUdf.TYPE_ROW);
    stockBinding = new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
    open(stockBinding, new long[] {0});
    var serializer = new RowDataSerializer(RowType.of(type));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      byte[] bytes = new byte[width];
      for (int j = 0; j < width; j++) bytes[j] = (byte) (i * 131 + j * 127);
      byte[] encoded = Base64.getEncoder().encode(bytes);
      Object value = binary ? encoded : BinaryStringData.fromBytes(encoded);
      RowData row = GenericRowData.of(nullable && i % 7 == 0 ? null : value);
      rows[i] = binaryRow ? serializer.toBinaryRow(row).copy() : row;
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
          var bytes = (BinaryStringData) result.getString(0);
          int length = bytes.getSizeInBytes();
          sum += length;
          if (length > 0) sum += (bytes.byteAt(0) & 255) + (bytes.byteAt(length - 1) & 255);
        }
      }
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() {
    if (stockBinding != null) stockBinding.unbind();
    if (encodedBinding != null) encodedBinding.unbind();
    stockBinding = null;
    encodedBinding = null;
    stock = null;
    rows = null;
  }

  /** Emits a deterministic released-Flink oracle for the Rust example's --oracle mode. */
  public static void main(String[] args) {
    byte[] alphabet = {'A', 'B', '/', '+', '=', '!', ' ', (byte) 255};
    emitOracle(new byte[0]);
    for (int length = 1; length <= 5; length++) {
      for (int index = 0; index < (1 << (3 * length)); index++) {
        byte[] bytes = new byte[length];
        int value = index;
        for (int position = 0; position < length; position++, value >>>= 3)
          bytes[position] = alphabet[value & 7];
        emitOracle(bytes);
      }
    }
    var random = new java.util.Random(498);
    for (int index = 0; index < 3000; index++) {
      byte[] bytes = new byte[random.nextInt(512)];
      random.nextBytes(bytes);
      emitOracle(bytes);
      byte[] encoded = Base64.getEncoder().encode(bytes);
      emitOracle(encoded);
      if (encoded.length > 0) {
        encoded[random.nextInt(encoded.length)] = (byte) random.nextInt(256);
        emitOracle(encoded);
      }
    }
  }

  private static void emitOracle(byte[] input) {
    String outcome;
    try {
      byte[] output =
          org.apache.flink.table.runtime.functions.SqlFunctionUtils.fromBase64(input).toBytes();
      byte[] characterOutput =
          org.apache.flink.table.runtime.functions.SqlFunctionUtils.fromBase64(
                  BinaryStringData.fromBytes(input))
              .toBytes();
      if (!java.util.Arrays.equals(output, characterOutput))
        throw new AssertionError("Overload mismatch");
      outcome = "ok:" + hex(output);
    } catch (IllegalArgumentException error) {
      outcome = "err:" + error.getMessage();
    }
    System.out.println(hex(input) + "\t" + outcome);
  }

  private static String hex(byte[] input) {
    char[] result = new char[input.length * 2];
    for (int index = 0; index < input.length; index++) {
      result[index * 2] = Character.forDigit((input[index] & 255) >>> 4, 16);
      result[index * 2 + 1] = Character.forDigit(input[index] & 15, 16);
    }
    return new String(result);
  }
}
