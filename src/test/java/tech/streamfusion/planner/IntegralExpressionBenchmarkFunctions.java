package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import tech.streamfusion.operator.NativeUdf;

/** Actual released SQL/encoder controls for prepared scalar expressions. */
public final class IntegralExpressionBenchmarkFunctions {
  private static NativeUdf.Binding binding;
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static String type;
  private static boolean booleanOutput;
  private static boolean stringOutput;
  private static volatile long consumed;

  private IntegralExpressionBenchmarkFunctions() {}

  public static String register(String element, boolean nullable, boolean binary, int count)
      throws Exception {
    Thread.currentThread().setContextClassLoader(IntegralExpressionBenchmarkFunctions.class.getClassLoader());
    type = element;
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    if (type.equals("BOOLEAN"))
      table.getConfig().set("table.exec.legacy-cast-behaviour",
          Boolean.getBoolean("sf.integral.legacy") ? "ENABLED" : "DISABLED");
    String declaration = type + (nullable ? "" : " NOT NULL");
    table.executeSql("CREATE TABLE inputs(v " + declaration + ") WITH ('connector'='datagen')");
    String expression = System.getProperty("sf.integral.expression", "ABS(v+CAST(1 AS $TYPE))")
        .replace("$TYPE", type);
    var query = ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM inputs")).getQueryOperation();
    var project = (Project) ((PlannerQueryOperation) query).getCalciteTree();
    LogicalType inputType = LogicalTypeParser.parse(declaration, getLoader());
    LogicalType outputType = FlinkTypeFactory.toLogicalType(project.getProjects().get(0).getType());
    booleanOutput = outputType.getTypeRoot() == LogicalTypeRoot.BOOLEAN;
    stringOutput = outputType.getTypeRoot() == LogicalTypeRoot.VARCHAR
        || outputType.getTypeRoot() == LogicalTypeRoot.CHAR;
    stock = FlinkExpressionGenerator.generate(project.getProjects(), null,
        new LogicalType[] {inputType}, RowType.of(outputType), table.getConfig(), getLoader());
    var context = new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() { return getLoader(); }
    };
    stock.open(context);
    var encoded = type.equals("BOOLEAN") ? null
        : RexExpression.encodeProjections(project.getProjects(), List.of("v"));
    if (type.equals("BOOLEAN")) {
      var constructor = RexExpression.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      encoded = constructor.newInstance();
      var configure = RexExpression.class.getDeclaredMethod("configure", org.apache.flink.table.api.TableConfig.class);
      configure.setAccessible(true);
      configure.invoke(encoded, table.getConfig());
      var emit = RexExpression.class.getDeclaredMethod("emit", org.apache.calcite.rex.RexNode.class);
      emit.setAccessible(true);
      if (!(Boolean) emit.invoke(encoded, project.getProjects().get(0)))
        throw new IllegalStateException("Expected an admitted Boolean expression");
    }
    if (encoded == null)
      throw new IllegalStateException("Expected an admitted integer expression");
    boolean previous = Boolean.getBoolean("sf.integral.previous");
    if (previous == encoded.jvmFunctionCalls().isEmpty())
      throw new IllegalStateException("Unexpected encoder route: " + encoded.jvmFunctionCalls());
    if (!previous && encoded.nativeFunctionCalls().isEmpty())
      throw new IllegalStateException("Expected the intended native function before constant folding");
    binding = encoded.udfBinding();
    long[] constants = binding.bind(encoded.longs(), context);
    var serializer = new RowDataSerializer(RowType.of(inputType));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      RowData row = GenericRowData.of(nullable && i % 7 == 0 ? null : value(type, i));
      rows[i] = binary ? serializer.toBinaryRow(row).copy() : row;
    }
    return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(Map.of(
        "kinds", encoded.kinds(), "payload", encoded.payload(),
        "child_counts", encoded.childCounts(), "longs", constants,
        "doubles", encoded.doubles(), "strings", encoded.strings(),
        "boolean_output", booleanOutput, "string_output", stringOutput));
  }

  private static ClassLoader getLoader() {
    return IntegralExpressionBenchmarkFunctions.class.getClassLoader();
  }

  private static Object value(String type, int row) {
    long v = row * 7919L;
    switch (type) {
      case "BOOLEAN": return row % 2 == 0;
      case "TINYINT": return row % 17 == 0 ? Byte.MIN_VALUE : (byte) v;
      case "SMALLINT": return row % 17 == 0 ? Short.MIN_VALUE : (short) v;
      case "INT": return row % 17 == 0 ? Integer.MIN_VALUE : (int) ((row % 2 == 0 ? -1L : 1L) * v);
      case "BIGINT": return row % 17 == 0 ? Long.MIN_VALUE
          : (row % 2 == 0 ? -1L : 1L) * (9007199254740993L + v);
      default:
        if (!type.equals("DECIMAL(18,2)") && !type.equals("DECIMAL(38,18)"))
          throw new IllegalArgumentException(type);
        int precision = type.equals("DECIMAL(18,2)") ? 18 : 38;
        int scale = type.equals("DECIMAL(18,2)") ? 2 : 18;
        BigInteger maximum = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        BigInteger unscaled = row % 17 == 0 ? maximum : row % 17 == 1 ? maximum.negate()
            : BigInteger.valueOf((row % 2 == 0 ? -1L : 1L) * (9007199254740993L + v));
        return DecimalData.fromBigDecimal(new BigDecimal(unscaled, scale), precision, scale);
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData input : rows) {
        RowData result = (RowData) stock.evalRow(input);
        if (result.isNullAt(0)) sum--;
        else if (booleanOutput) sum += result.getBoolean(0) ? 1 : 0;
        else if (stringOutput) {
          var value = (BinaryStringData) result.getString(0);
          int length = value.getSizeInBytes();
          sum += length + (length == 0 ? 0 : value.byteAt(0) + value.byteAt(length - 1));
        }
        else switch (type) {
          case "TINYINT": sum += result.getByte(0); break;
          case "SMALLINT": sum += result.getShort(0); break;
          case "INT": sum += result.getInt(0); break;
          case "BIGINT": sum += result.getLong(0); break;
          default: throw new IllegalArgumentException(type);
        }
      }
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() throws Exception {
    if (binding != null) binding.unbind();
    if (stock != null) stock.close();
    binding = null;
    stock = null;
    rows = null;
  }
}
