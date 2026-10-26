package tech.streamfusion.planner;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.operator.NativeUdf;

/** Released generated expressions and prepared RowData for the Rust string_fusion driver. */
public final class StringFusionBenchmarkFunctions {
  private static FlinkExpressionFunction stock;
  private static RowData[] rows;
  private static volatile long consumed;

  private StringFusionBenchmarkFunctions() {}

  private static LogicalType[] arguments(int operation, boolean flattened) {
    List<LogicalType> types = new ArrayList<>();
    types.add(new VarCharType(VarCharType.MAX_LENGTH));
    types.add((operation - 20) % 3 == 2 ? new BigIntType() : new IntType());
    types.add(new VarCharType(VarCharType.MAX_LENGTH));
    if (flattened && (operation - 20) % 3 == 2) java.util.Collections.swap(types, 1, 2);
    return types.toArray(LogicalType[]::new);
  }

  private static FlinkExpressionFunction function(int operation, String target, boolean flattened)
      throws Exception {
    var factory = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(factory);
    var text =
        factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);
    boolean overlay = (operation - 20) % 3 == 2;
    var number =
        factory.createTypeWithNullability(
            factory.createSqlType(overlay ? SqlTypeName.BIGINT : SqlTypeName.INTEGER), true);
    RexNode source = rex.makeInputRef(text, 0);
    RexNode count = rex.makeInputRef(number, overlay && flattened ? 2 : 1);
    RexNode pattern = rex.makeInputRef(text, overlay && flattened ? 1 : 2);
    RexNode producer =
        rex.makeCall(
            text,
            overlay
                ? SqlStdOperatorTable.OVERLAY
                : (operation - 20) % 3 == 0
                    ? FlinkSqlOperatorTable.LPAD
                    : FlinkSqlOperatorTable.RPAD,
            overlay ? List.of(source, pattern, count) : List.of(source, count, pattern));
    RexNode literal = rex.makeCharLiteral(new NlsString(target, "UTF-8", SqlCollation.IMPLICIT));
    RexNode call =
        operation >= 26
            ? rex.makeCall(
                text,
                FlinkSqlOperatorTable.REPLACE,
                List.of(producer, rex.makeLiteral("?"), rex.makeLiteral("Z")))
            : rex.makeCall(
                operation < 23 ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS,
                List.of(producer, literal));
    var result =
        FlinkExpressionGenerator.generate(
            call,
            arguments(operation, flattened),
            TableConfig.getDefault(),
            StringFusionBenchmarkFunctions.class.getClassLoader(),
            false);
    result.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return StringFusionBenchmarkFunctions.class.getClassLoader();
          }
        });
    return result;
  }

  public static int register(int operation, String target) throws Exception {
    var function = function(operation, target, true);
    int[] codes = new int[] {16, 2, 16};
    if ((operation - 20) % 3 == 2) {
      codes[1] = NativeUdf.TYPE_INTERNAL;
      codes[2] = NativeUdf.TYPE_LONG;
    }
    return NativeUdf.register(
        function,
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        codes,
        operation >= 26 ? NativeUdf.TYPE_STRING : NativeUdf.TYPE_BOOLEAN);
  }

  public static void prepare(
      int operation, String target, String corpus, int width, boolean cached, int count)
      throws Exception {
    prepare(operation, target, corpus, width, cached, count, 0);
  }

  public static void prepare(
      int operation, String target, String corpus, int width, boolean cached, int count, int growth)
      throws Exception {
    prepare(operation, target, corpus, width, cached, count, growth, false);
  }

  public static void prepare(
      int operation,
      String target,
      String corpus,
      int width,
      boolean cached,
      int count,
      int growth,
      boolean matching)
      throws Exception {
    if (stock != null) stock.close();
    stock = function(operation, target, false);
    var serializer = new RowDataSerializer(RowType.of(arguments(operation, false)));
    String unit =
        corpus.equals("cjk") ? "\u4e2d\u6587?" : corpus.equals("emoji") ? "\ud83d\ude00?" : "ab?c";
    int repeats = Math.max(1, width / unit.getBytes(StandardCharsets.UTF_8).length);
    String base = "a?" + unit.repeat(repeats);
    String pad =
        growth > 0 && corpus.equals("cjk")
            ? "\u4e2d"
            : growth > 0 && corpus.equals("emoji") ? "\ud83d\ude00" : "X";
    rows = new RowData[count];
    boolean overlay = (operation - 20) % 3 == 2;
    for (int i = 0; i < count; i++) {
      String source = base + "|" + (matching ? 0 : i % 16);
      int number =
          growth > 0
              ? (i % 7 == 0 ? 0 : source.length()) + growth + (matching ? 0 : i % 3)
              : overlay ? 1 + i % 3 : 2 + unit.length() * (repeats / 2) + i % 3;
      Object typedNumber = overlay ? (Object) Long.valueOf(number) : Integer.valueOf(number);
      RowData row =
          GenericRowData.of(
              i % 7 == 0 ? null : StringData.fromString(source),
              typedNumber,
              StringData.fromString(pad));
      rows[i] = cached ? row : serializer.toBinaryRow(row).copy();
    }
  }

  public static long[] measure(int loops) throws Exception {
    long checksum = 0;
    long start = System.nanoTime();
    for (int loop = 0; loop < loops; loop++) {
      for (RowData row : rows) {
        Object result = stock.evalRow(row);
        checksum +=
            result == null
                ? -1
                : result instanceof Boolean
                    ? ((Boolean) result ? 1 : 0)
                    : ((StringData) result).toBytes().length;
      }
    }
    consumed = checksum;
    return new long[] {System.nanoTime() - start, checksum};
  }
}
