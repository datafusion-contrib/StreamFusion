package tech.streamfusion.planner;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;

/** Released generated casts over prepared RowData; the old encoder must reject the native route. */
public final class BinaryCharacterBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static RexNode call;
  private static RowData[] rows;
  private static volatile long consumed;

  private BinaryCharacterBenchmarkFunctions() {}

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() {
        return BinaryCharacterBenchmarkFunctions.class.getClassLoader();
      }
    };
  }

  public static void register(int width, boolean fixed) throws Exception {
    Thread.currentThread().setContextClassLoader(BinaryCharacterBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE inputs(b " + (fixed ? "BINARY(" + width + ")" : "BYTES")
        + ") WITH ('connector'='datagen')");
    var query = ((TableImpl) table.sqlQuery("SELECT CAST(b AS STRING) FROM inputs")).getQueryOperation();
    call = ((Project) ((PlannerQueryOperation) query).getCalciteTree()).getProjects().get(0);
    var encoded = RexExpression.encodeProjections(List.of(call), List.of("s"));
    if (Boolean.getBoolean("sf.binary.character.previous")) {
      if (encoded != null) throw new IllegalStateException("Previous binary cast must fall back");
    } else if (encoded == null || encoded.kinds()[0] != 6 || encoded.payload()[0] != 237
        || !encoded.jvmFunctionCalls().isEmpty()) {
      throw new IllegalStateException("Candidate binary cast must use the native UTF-8 decoder");
    }
  }

  public static byte[] bytes(int width, int mode, int row) {
    byte[] bytes = new byte[width];
    for (int j = 0; j < width; j++) bytes[j] = (byte) ('a' + (row + j) % 26);
    if (mode == 1) {
      byte[] unit = {(byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80,
          (byte) 0xe4, (byte) 0xb8, (byte) 0xad, 97};
      for (int j = 0; j + unit.length <= width; j += unit.length)
        System.arraycopy(unit, 0, bytes, j, unit.length);
    } else if (mode == 2 || mode == 3 && row % 7 == 0) {
      bytes[width - 3] = (byte) 0xed;
      bytes[width - 2] = (byte) 0xa0;
      bytes[width - 1] = (byte) 0x80;
    } else if (mode == 4) {
      for (int j = 0; j < width; j++) bytes[j] = (byte) (row * 131 + j * 127);
    }
    return bytes;
  }

  public static void prepare(int width, int mode, boolean fixed, boolean nullable, boolean binary, int count)
      throws Exception {
    LogicalType[] types = {fixed ? new BinaryType(width) : new VarBinaryType(VarBinaryType.MAX_LENGTH)};
    stock = FlinkExpressionGenerator.generate(call, types, TableConfig.getDefault(),
        BinaryCharacterBenchmarkFunctions.class.getClassLoader(), false);
    stock.open(context());
    var serializer = new RowDataSerializer(RowType.of(types));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      RowData row = GenericRowData.of(nullable && i % 7 == 0 ? null : bytes(width, mode, i));
      rows[i] = binary ? serializer.toBinaryRow(row).copy() : row;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) for (RowData row : rows) {
      var value = (StringData) stock.evalRow(row);
      String text = value == null ? null : value.toString();
      sum += text == null ? -1 : text.isEmpty() ? 0 : text.charAt(0) + 1;
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() throws Exception {
    if (stock != null) stock.close();
  }

  private static String hex(byte[] bytes) {
    var out = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) out.append(Character.forDigit((b & 255) >>> 4, 16))
        .append(Character.forDigit(b & 15, 16));
    return out.toString();
  }

  private static void oracle(byte[] bytes) throws Exception {
    var decoded = (StringData) stock.evalRow(GenericRowData.of(bytes));
    System.out.println(hex(bytes) + "," + hex(decoded.toString().getBytes(StandardCharsets.UTF_8)));
  }

  public static void main(String[] args) throws Exception {
    register(8, false);
    prepare(8, 0, false, false, false, 1);
    oracle(new byte[0]);
    for (int a = 0; a < 256; a++) {
      oracle(new byte[] {(byte) a});
      for (int b = 0; b < 256; b++) oracle(new byte[] {(byte) a, (byte) b});
    }
    for (int second = 0xa0; second <= 0xbf; second++) {
      oracle(new byte[] {(byte) 0xed, (byte) second});
      for (int third = 0; third < 256; third++) {
        oracle(new byte[] {(byte) 0xed, (byte) second, (byte) third});
        oracle(new byte[] {97, (byte) 0xed, (byte) second, (byte) third, 98});
      }
    }
    var random = new java.util.Random(493121);
    for (int i = 0; i < 8192; i++) {
      byte[] bytes = new byte[3 + i % 67];
      random.nextBytes(bytes);
      oracle(bytes);
    }
    for (byte[] bytes : new byte[][] {{(byte) 0xed, (byte) 0xa0, (byte) 0x80},
        {(byte) 0xf0, (byte) 0x90, (byte) 0x80}, {(byte) 0xf4, (byte) 0x90, (byte) 0x80, (byte) 0x80}})
      oracle(bytes);
    close();
  }
}
