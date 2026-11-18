package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.util.Base64;
import org.apache.calcite.rel.core.Project;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.NativeUdf;

/** Prepared RowData controls for the production runtime_array_sort Rust benchmark. */
public final class RuntimeArraySortBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static NativeUdf.Binding stockBinding;
  private static NativeUdf.Binding previousBinding;
  private static RowData[] rows;
  private static String element;
  private static String schema;
  private static volatile long consumed;

  private RuntimeArraySortBenchmarkFunctions() {}

  private static LogicalType elementType(String type) {
    return type.equals("int")
        ? new IntType()
        : type.equals("bigint") ? new BigIntType() : new DecimalType(38, 9);
  }

  private static FlinkExpressionFunction function(String type) throws Exception {
    Thread.currentThread()
        .setContextClassLoader(RuntimeArraySortBenchmarkFunctions.class.getClassLoader());
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    String sqlType = type.equals("decimal") ? "DECIMAL(38,9)" : type;
    table.executeSql(
        "CREATE TABLE inputs(a ARRAY<"
            + sqlType
            + ">, ascending_order BOOLEAN, null_first BOOLEAN) WITH ('connector'='datagen')");
    var query =
        ((TableImpl)
                table.sqlQuery(
                    "SELECT ARRAY_SORT(ARRAY_SLICE(a,1,31),ascending_order,null_first) FROM"
                        + " inputs"))
            .getQueryOperation();
    var project = (Project) ((PlannerQueryOperation) query).getCalciteTree();
    var types =
        new LogicalType[] {new ArrayType(elementType(type)), new BooleanType(), new BooleanType()};
    var function =
        FlinkExpressionGenerator.generate(
            project.getProjects(),
            null,
            types,
            RowType.of(new ArrayType(elementType(type))),
            table.getConfig(),
            RuntimeArraySortBenchmarkFunctions.class.getClassLoader());
    return function;
  }

  public static int register(String type) throws Exception {
    var function = function(type);
    schema = Base64.getEncoder().encodeToString(function.arrowResultSchema().serializeAsMessage());
    previousBinding = binding(function);
    return (int) open(previousBinding);
  }

  private static NativeUdf.Binding binding(FlinkExpressionFunction function) throws Exception {
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_INTERNAL, NativeUdf.TYPE_BOOLEAN, NativeUdf.TYPE_BOOLEAN},
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
                return RuntimeArraySortBenchmarkFunctions.class.getClassLoader();
              }
            })[0];
  }

  public static String schema() {
    return schema;
  }

  public static void prepare(String type, int width, boolean nullable, boolean binary, int count)
      throws Exception {
    if (stockBinding != null) stockBinding.unbind();
    element = type;
    stock = function(type);
    stockBinding = binding(stock);
    open(stockBinding);
    var types =
        new LogicalType[] {new ArrayType(elementType(type)), new BooleanType(), new BooleanType()};
    var serializer = new RowDataSerializer(RowType.of(types));
    rows = new RowData[count];
    for (int row = 0; row < count; row++) {
      ArrayData array = null;
      if (!nullable || row % 7 != 0) {
        Object[] values = new Object[width];
        for (int i = 0; i < width; i++) {
          if (nullable && i % 5 == 0) continue;
          int v = (row + i * 7) % 17 - 8;
          if (type.equals("int")) values[i] = v;
          else if (type.equals("bigint")) values[i] = 9007199254740993L + v;
          else
            values[i] =
                DecimalData.fromBigDecimal(
                    new BigDecimal("9007199254740993.000000001").add(BigDecimal.valueOf(v)), 38, 9);
        }
        array = new GenericArrayData(values);
      }
      RowData value =
          GenericRowData.of(
              array,
              nullable && row % 11 == 0 ? null : row % 2 == 0,
              nullable && row % 13 == 0 ? null : row % 4 < 2);
      rows[row] = binary ? serializer.toBinaryRow(value).copy() : value;
    }
  }

  public static long[] measure(int loops) throws Exception {
    long checksum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData input : rows) {
        RowData result = (RowData) stock.evalRow(input);
        if (result.isNullAt(0)) {
          checksum -= 7;
          continue;
        }
        ArrayData values = result.getArray(0);
        checksum += values.size();
        for (int i = 0; i < values.size(); i++) {
          if (values.isNullAt(i)) checksum--;
          else if (element.equals("int")) checksum += values.getInt(i);
          else if (element.equals("bigint")) checksum += values.getLong(i);
          else checksum += values.getDecimal(i, 38, 9).toBigDecimal().unscaledValue().longValue();
        }
      }
    }
    consumed = checksum;
    return new long[] {System.nanoTime() - start, checksum};
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
