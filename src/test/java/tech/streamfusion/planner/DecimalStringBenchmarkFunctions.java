package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.NativeUdf;

/** Prepared RowData oracle and actual encoder bindings for decimal formatting comparisons. */
public final class DecimalStringBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding binding;
  private static RowData[] rows;
  private static volatile long consumed;

  private DecimalStringBenchmarkFunctions() {}

  private static TableConfig config() {
    var config = TableConfig.getDefault();
    config.getConfiguration().setString("table.exec.legacy-cast-behaviour",
        Boolean.getBoolean("sf.decimal.string.legacy") ? "ENABLED" : "DISABLED");
    return config;
  }

  private static RexCall call(int precision, int scale) {
    var types = new FlinkTypeFactory(DecimalStringBenchmarkFunctions.class.getClassLoader(),
        FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    return (RexCall) rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
        Boolean.getBoolean("sf.decimal.string.tryCast")
            ? FlinkSqlOperatorTable.TRY_CAST : FlinkSqlOperatorTable.CAST,
        List.of(rex.makeInputRef(types.createSqlType(SqlTypeName.DECIMAL, precision, scale), 0)));
  }

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override public ClassLoader getUserCodeClassLoader() {
        return DecimalStringBenchmarkFunctions.class.getClassLoader();
      }
    };
  }

  public static long[] register(int precision, int scale) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, config());
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    boolean previous = Boolean.getBoolean("sf.decimal.string.previous");
    boolean admitted = (Boolean) emit.invoke(encoded, call(precision, scale));
    if (previous && Boolean.getBoolean("sf.decimal.string.tryCast")) {
      if (admitted) throw new IllegalStateException("Previous TRY_CAST must use stock fallback");
      return new long[] {-1, 0};
    }
    if (!admitted || encoded.kinds().length != 2)
      throw new IllegalStateException("Expected a direct decimal-to-STRING cast");
    if (encoded.kinds()[0] != (previous ? 17 : 11)
        || !previous && encoded.payload()[0] != 6)
      throw new IllegalStateException("Unexpected decimal cast route");
    binding = encoded.udfBinding();
    long[] constants = binding.bind(encoded.longs(), context());
    long[] result = new long[constants.length + 2];
    result[0] = encoded.kinds()[0];
    result[1] = encoded.payload()[0];
    System.arraycopy(constants, 0, result, 2, constants.length);
    return result;
  }

  public static void prepare(int precision, int scale, boolean nullable, boolean binary, int count)
      throws Exception {
    stock = FlinkExpressionGenerator.generate(call(precision, scale),
        new LogicalType[] {new DecimalType(precision, scale)}, config(),
        DecimalStringBenchmarkFunctions.class.getClassLoader(), false);
    stock.open(context());
    var serializer = new RowDataSerializer(RowType.of(new DecimalType(precision, scale)));
    var limit = BigInteger.TEN.pow(precision);
    var max = limit.subtract(BigInteger.ONE);
    rows = new RowData[count];
    for (int i = 0; i < count; i++) {
      BigInteger value = i % 17 == 0 ? max
          : BigInteger.valueOf((precision > 18 ? 9007199254740993L : 0) + i * 7919L + 1)
              .remainder(limit);
      if (i % 13 == 0) value = BigInteger.ZERO;
      if ((i & 1) == 0) value = value.negate();
      Object decimal = nullable && i % 7 == 0 ? null
          : DecimalData.fromBigDecimal(new BigDecimal(value, scale), precision, scale);
      RowData row = GenericRowData.of(decimal);
      rows[i] = binary ? serializer.toBinaryRow(row).copy() : row;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long sum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData row : rows) {
        var value = (StringData) stock.evalRow(row);
        sum += value == null ? -1 : value.toBytes().length;
      }
    }
    consumed = sum;
    return new long[] {System.nanoTime() - start, sum};
  }

  public static void close() throws Exception {
    if (stock != null) stock.close();
    if (binding != null) binding.unbind();
  }

  public static void main(String[] args) {
    var random = new java.util.Random(492);
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        var limit = BigInteger.TEN.pow(precision);
        var max = limit.subtract(BigInteger.ONE);
        var values = new java.util.ArrayList<BigInteger>(List.of(BigInteger.ZERO,
            BigInteger.ONE, BigInteger.ONE.negate(), max, max.negate()));
        for (int i = 0; i < 32; i++) {
          var value = new BigInteger(128, random).remainder(limit);
          values.add((i & 1) == 0 ? value.negate() : value);
        }
        for (var value : values) {
          var decimal = DecimalData.fromBigDecimal(new BigDecimal(value, scale), precision, scale);
          System.out.println(precision + "," + scale + "," + value + "," + decimal);
        }
      }
    }
  }
}
