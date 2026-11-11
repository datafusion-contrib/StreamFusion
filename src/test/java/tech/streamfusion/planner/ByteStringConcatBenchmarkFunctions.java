package tech.streamfusion.planner;

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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

/** Released Flink generated CONCAT over prebuilt rows, paired with the Rust-side Calc timer. */
public final class ByteStringConcatBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static volatile long consumed;

  private ByteStringConcatBenchmarkFunctions() {}

  public static void prepare(
      int width, boolean unicode, boolean nullable, int invalidEvery, int count) throws Exception {
    var loader = ByteStringConcatBenchmarkFunctions.class.getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE src(s STRING) WITH ('connector'='datagen')");
    var operation =
        ((TableImpl) table.sqlQuery("SELECT CONCAT(s, '!') FROM src")).getQueryOperation();
    var project = (Project) ((PlannerQueryOperation) operation).getCalciteTree();
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    stock =
        FlinkExpressionGenerator.generate(
            project.getProjects(),
            null,
            new LogicalType[] {type},
            RowType.of(type),
            TableConfig.getDefault(),
            loader);
    stock.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return loader;
          }
        });
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      byte[] bytes =
          (unicode ? "\u4e2d" : "a")
              .repeat(width)
              .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      if (invalidEvery > 0 && i % invalidEvery == 0 && bytes.length > 0) bytes[0] = (byte) 255;
      rows[i] =
          GenericRowData.of(nullable && i % 7 == 0 ? null : BinaryStringData.fromBytes(bytes));
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
          var value = (BinaryStringData) result.getString(0);
          int length = value.getSizeInBytes();
          sum += length + (value.byteAt(0) & 255) + (value.byteAt(length - 1) & 255);
        }
      }
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() throws Exception {
    stock.close();
    stock = null;
    rows = null;
  }
}
