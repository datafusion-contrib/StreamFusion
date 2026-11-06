package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Locale;
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
  private static LogicalTypeRoot outputRoot;
  private static boolean booleanOutput;
  private static boolean longOutput;
  private static boolean stringOutput;
  private static volatile long consumed;
  private static final String JSON_SCALAR_LONG_TEXT = "0123456789".repeat(64);
  private static final long[] TIMESTAMP_EPOCHS = {Long.MIN_VALUE, Long.MAX_VALUE,
      -62135596800000L, -12219292800000L, -2208988800000L, -86400001, -1, 0, 1,
      951827696123L, 1706659200000L, 1710037800000L, 253402300799999L};
  private static final long[] EPOCH_MILLIS = {Long.MIN_VALUE, Long.MAX_VALUE,
      -62167219200001L, -62167219200000L, -62167219199999L,
      253402300799999L, 253402300800000L, -62135596800000L, -12219292800000L,
      -86400001, -1, 0, 1, 951827696123L, 1706659200000L, 1710037800000L};
  private static final long[] EPOCH_SECONDS = {Long.MIN_VALUE, Long.MAX_VALUE,
      Long.MIN_VALUE / 1000 - 1, Long.MAX_VALUE / 1000 + 1,
      -62167219201L, -62167219200L, -62167219199L, 253402300798L,
      253402300799L, 253402300800L, -62135596800L, -12219292800L,
      -86401, -1, 0, 1, 951827696, 1706659200, 1710037800};

  private IntegralExpressionBenchmarkFunctions() {}

  public static String register(String element, boolean nullable, boolean binary, int count)
      throws Exception {
    Thread.currentThread().setContextClassLoader(IntegralExpressionBenchmarkFunctions.class.getClassLoader());
    type = element;
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    boolean timestampInput = type.startsWith("TIMESTAMP_LTZ(") || epochMillisInput() || epochSecondsInput();
    if (timestampInput) table.getConfig().setLocalTimeZone(
        java.time.ZoneId.of(System.getProperty("sf.integral.zone", "UTC")));
    table.getConfig().set("table.exec.legacy-cast-behaviour",
        Boolean.getBoolean("sf.integral.legacy") ? "ENABLED" : "DISABLED");
    String declaration = ("plain-timestamp".equals(System.getenv("SF_INTEGRAL_CORPUS"))
        ? type.replace("TIMESTAMP_LTZ(", "TIMESTAMP(") : type) + (nullable ? "" : " NOT NULL");
    String secondary = System.getProperty("sf.integral.secondaryType", "");
    if (!secondary.isEmpty() && !List.of("INT", "BIGINT").contains(secondary))
      throw new IllegalArgumentException("Unsupported secondary input " + secondary);
    String secondaryDeclaration = secondary + (nullable ? "" : " NOT NULL");
    table.executeSql("CREATE TABLE inputs(v " + declaration
        + (secondary.isEmpty() ? "" : ",n " + secondaryDeclaration)
        + ") WITH ('connector'='datagen')");
    String expression = System.getProperty("sf.integral.expression", "ABS(v+CAST(1 AS $TYPE))")
        .replace("$TYPE", type);
    var query = ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM inputs")).getQueryOperation();
    var project = (Project) ((PlannerQueryOperation) query).getCalciteTree();
    LogicalType inputType = LogicalTypeParser.parse(declaration, getLoader());
    LogicalType[] inputTypes = secondary.isEmpty() ? new LogicalType[] {inputType}
        : new LogicalType[] {inputType, LogicalTypeParser.parse(secondaryDeclaration, getLoader())};
    LogicalType outputType = FlinkTypeFactory.toLogicalType(project.getProjects().get(0).getType());
    outputRoot = outputType.getTypeRoot();
    booleanOutput = outputType.getTypeRoot() == LogicalTypeRoot.BOOLEAN;
    longOutput = outputType.getTypeRoot() == LogicalTypeRoot.BIGINT;
    stringOutput = outputType.getTypeRoot() == LogicalTypeRoot.VARCHAR
        || outputType.getTypeRoot() == LogicalTypeRoot.CHAR;
    stock = FlinkExpressionGenerator.generate(project.getProjects(), null,
        inputTypes, RowType.of(outputType), table.getConfig(), getLoader());
    var context = new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() { return getLoader(); }
    };
    stock.open(context);
    boolean previous = Boolean.getBoolean("sf.integral.previous");
    boolean fallbackControl = Boolean.getBoolean("sf.integral.fallbackControl");
    if (fallbackControl && !previous)
      throw new IllegalStateException("A fallback control requires the preceding encoder");
    String fallbackReason = null;
    boolean wholeCalc = Boolean.getBoolean("sf.integral.wholeCalc");
    if (wholeCalc && fallbackControl)
      throw new IllegalArgumentException("A whole-Calc control must use its production encoder");
    var encoded = fallbackControl ? null : wholeCalc
        ? RexExpression.encodeCalc(org.apache.calcite.rel.logical.LogicalCalc.create(
            project.getInput(), org.apache.calcite.rex.RexProgram.create(
                project.getInput().getRowType(), project.getProjects(), null, project.getRowType(),
                project.getCluster().getRexBuilder())))
        : RexExpression.encode(project.getProjects().get(0), project);
    if (fallbackControl) {
      var constructor = RexExpression.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      encoded = constructor.newInstance();
      var configure = RexExpression.class.getDeclaredMethod("configure", org.apache.flink.table.api.TableConfig.class);
      configure.setAccessible(true);
      configure.invoke(encoded, table.getConfig());
      var emit = RexExpression.class.getDeclaredMethod("emit", org.apache.calcite.rex.RexNode.class);
      emit.setAccessible(true);
      if (!(Boolean) emit.invoke(encoded, project.getProjects().get(0))) {
        var reason = RexExpression.class.getDeclaredField("reason");
        reason.setAccessible(true);
        fallbackReason = (String) reason.get(encoded);
        String expectedReason = System.getProperty("sf.integral.fallbackReason", "");
        if (fallbackReason == null || (expectedReason.isEmpty()
            ? !fallbackReason.startsWith("unsupported TRY_CAST DECIMAL")
            : !fallbackReason.equals(expectedReason)))
          throw new IllegalStateException("Unexpected preceding fallback: " + fallbackReason);
        encoded = null;
      } else {
        throw new IllegalStateException("Expected the preceding encoder to reject this signature");
      }
    }
    if (encoded == null && !fallbackControl)
      throw new IllegalStateException("Expected an admitted integer expression");
    var serializer = new RowDataSerializer(RowType.of(inputTypes));
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      Object primary = nullable && i % 7 == 0 ? null : value(type, i);
      RowData row = secondary.isEmpty() ? GenericRowData.of(primary)
          : GenericRowData.of(primary, nullable && i % 11 == 0 ? null : value(secondary, i + 3));
      rows[i] = binary ? serializer.toBinaryRow(row).copy() : row;
    }
    if (fallbackControl) {
      System.err.println("PREVIOUS_FALLBACK," + fallbackReason);
      return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
          Map.of("fallback", true, "reason", fallbackReason));
    }
    if (previous == encoded.jvmFunctionCalls().isEmpty())
      throw new IllegalStateException("Unexpected encoder route: " + encoded.jvmFunctionCalls());
    if (!previous && encoded.nativeFunctionCalls().isEmpty())
      throw new IllegalStateException("Expected the intended native function before constant folding");
    String requiredFunction = System.getenv("SF_INTEGRAL_REQUIRED_FUNCTION");
    if (requiredFunction != null && !(previous ? encoded.jvmFunctionCalls()
        : encoded.nativeFunctionCalls()).contains(requiredFunction))
      throw new IllegalStateException("Missing intended function " + requiredFunction
          + ": native=" + encoded.nativeFunctionCalls() + ", jvm=" + encoded.jvmFunctionCalls());
    binding = encoded.udfBinding();
    long[] constants = binding.bind(encoded.longs(), context);
    return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(Map.of(
        "kinds", encoded.kinds(), "payload", encoded.payload(),
        "child_counts", encoded.childCounts(), "longs", constants,
        "doubles", encoded.doubles(), "strings", encoded.strings(),
        "boolean_output", booleanOutput, "string_output", stringOutput,
        "long_output", longOutput, "integer_output", outputType.getTypeRoot().name()));
  }

  /** A rejected native signature runs in Java on RowData in the actual preceding implementation. */
  public static void main(String[] args) throws Exception {
    String element = args[0];
    boolean nullable = Boolean.parseBoolean(args[1]);
    boolean binary = Boolean.parseBoolean(args[2]);
    int count = Integer.parseInt(args[3]);
    int loops = Integer.parseInt(args[4]);
    int warmups = Integer.parseInt(args[5]);
    int runs = Integer.parseInt(args[6]);
    try {
      var route = new com.fasterxml.jackson.databind.ObjectMapper()
          .readTree(register(element, nullable, binary, count));
      if (!route.path("fallback").asBoolean())
        throw new IllegalStateException("This control must verify stock fallback");
      long expected = measure(1)[1] * loops;
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) & 1;
          long[] measured = measure(loops);
          if (measured[1] != expected) throw new IllegalStateException("Fallback checksum differs");
          System.out.printf(Locale.ROOT, "integral_expression,%s,%s,%s,%d,%d,%d,%s,%s,%d,%d%n",
              element, nullable, binary, count, count * loops, trial, trial < warmups,
              engine == 0 ? "flink" : "previous", measured[0], measured[1]);
        }
      }
    } finally {
      close();
    }
  }

  private static ClassLoader getLoader() {
    return IntegralExpressionBenchmarkFunctions.class.getClassLoader();
  }

  private static Object value(String type, int row) {
    long v = row * 7919L;
    switch (type) {
      case "VARCHAR(4096)": return org.apache.flink.table.data.StringData.fromString(
          jsonScalarText(row));
      case "BOOLEAN": return row % 2 == 0;
      case "TINYINT": return row % 17 == 0 ? Byte.MIN_VALUE : (byte) v;
      case "SMALLINT": return row % 17 == 0 ? Short.MIN_VALUE : (short) v;
      case "INT": return row % 17 == 0 ? Integer.MIN_VALUE : (int) ((row % 2 == 0 ? -1L : 1L) * v);
      case "BIGINT": return epochSecondsInput() ? epochSeconds(row)
          : epochMillisInput() ? epochMillis(row) : row % 17 == 0 ? Long.MIN_VALUE
          : (row % 2 == 0 ? -1L : 1L) * (9007199254740993L + v);
      case "TIMESTAMP_LTZ(3)": return org.apache.flink.table.data.TimestampData.fromEpochMillis(
          temporalMillis(row));
      case "TIMESTAMP_LTZ(9)": return org.apache.flink.table.data.TimestampData.fromEpochMillis(
          temporalMillis(row), (row * 7919) % 1000000);
      default:
        if (!type.startsWith("DECIMAL(") || !type.endsWith(")"))
          throw new IllegalArgumentException(type);
        String[] decimal = type.substring(8, type.length() - 1).split(",");
        int precision = Integer.parseInt(decimal[0]);
        int scale = Integer.parseInt(decimal[1]);
        if ("decimal-epoch".equals(System.getenv("SF_INTEGRAL_CORPUS"))) {
          var values = decimalEpochCoefficients(precision, scale);
          return DecimalData.fromBigDecimal(new BigDecimal(values.get(row % values.size()), scale),
              precision, scale);
        }
        BigInteger maximum = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        boolean decimalTruth = "decimal-truth".equals(System.getenv("SF_INTEGRAL_CORPUS"));
        BigInteger unscaled = decimalTruth && row % 17 == 2 ? BigInteger.ZERO
            : row % 17 == 0 ? maximum : row % 17 == 1 ? maximum.negate()
            : BigInteger.valueOf((row % 2 == 0 ? -1L : 1L)
                * (precision == 4 ? v % 10000 : 9007199254740993L + v));
        return DecimalData.fromBigDecimal(new BigDecimal(unscaled, scale), precision, scale);
    }
  }

  public static List<BigInteger> decimalEpochCoefficients(int precision, int scale) {
    BigInteger limit = BigInteger.TEN.pow(precision);
    BigInteger factor = BigInteger.TEN.pow(scale);
    var values = new java.util.ArrayList<BigInteger>(List.of(
        BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE.negate(),
        limit.subtract(BigInteger.ONE), BigInteger.ONE.subtract(limit)));
    for (long integer : new long[] {0, -1, 1, -86401, 951827696, 1706659200,
        -62167219200L, 253402300799L, 253402300800L, -62167219200000L,
        253402300799999L, 253402300800000L, Long.MIN_VALUE, Long.MAX_VALUE}) {
      for (int wrap : new int[] {0, -1, 1}) {
        BigInteger base = BigInteger.valueOf(integer)
            .add(BigInteger.ONE.shiftLeft(64).multiply(BigInteger.valueOf(wrap))).multiply(factor);
        for (int fraction : new int[] {-1, 0, 1}) {
          BigInteger coefficient = base.add(BigInteger.valueOf(fraction));
          if (coefficient.abs().compareTo(limit) < 0) values.add(coefficient);
        }
      }
    }
    return values;
  }

  public static String jsonScalarText(int row) {
    switch (row % 8) {
      case 0: return "";
      case 1: return "plain";
      case 2: return "a/b\\c\"d";
      case 3: return "\0\t\r\n\b\f\u001f";
      case 4: return "\\uD800";
      case 5: return "\u4e2d\u6587\ud83d\ude00";
      case 6: return "{\"a\":1}";
      default: return JSON_SCALAR_LONG_TEXT;
    }
  }

  public static long temporalMillis(long row) {
    int index = (int) (row % TIMESTAMP_EPOCHS.length);
    return TIMESTAMP_EPOCHS[index] + (index < 2 ? 0 : row / TIMESTAMP_EPOCHS.length * 7919);
  }

  private static boolean epochMillisInput() {
    return Boolean.getBoolean("sf.integral.epochMillisInput")
        || "epoch-millis".equals(System.getenv("SF_INTEGRAL_CORPUS"));
  }

  public static long epochMillis(long row) {
    int index = (int) (row % EPOCH_MILLIS.length);
    return EPOCH_MILLIS[index] + (index < 7 ? 0 : row / EPOCH_MILLIS.length * 7919);
  }

  private static boolean epochSecondsInput() {
    return Boolean.getBoolean("sf.integral.epochSecondsInput")
        || "epoch-seconds".equals(System.getenv("SF_INTEGRAL_CORPUS"));
  }

  public static long epochSeconds(long row) {
    int index = (int) (row % EPOCH_SECONDS.length);
    return EPOCH_SECONDS[index] + (index < 10 ? 0 : row / EPOCH_SECONDS.length * 7919);
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData input : rows) {
        RowData result = (RowData) stock.evalRow(input);
        if (result.isNullAt(0)) sum--;
        else if (booleanOutput) sum += result.getBoolean(0) ? 1 : 0;
        else if (longOutput) sum += result.getLong(0);
        else if (stringOutput) {
          var value = (BinaryStringData) result.getString(0);
          int length = value.getSizeInBytes();
          sum += length + (length == 0 ? 0 : value.byteAt(0) + value.byteAt(length - 1));
        }
        else switch (outputRoot) {
          case TINYINT: sum += result.getByte(0); break;
          case SMALLINT: sum += result.getShort(0); break;
          case INTEGER: sum += result.getInt(0); break;
          case BIGINT: sum += result.getLong(0); break;
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            var timestamp = result.getTimestamp(0, 3);
            sum += timestamp.getMillisecond() + timestamp.getNanoOfMillisecond();
            break;
          default: throw new IllegalArgumentException(outputRoot.toString());
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
