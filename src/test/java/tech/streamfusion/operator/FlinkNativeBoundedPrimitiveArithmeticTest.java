package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

/**
 * Released operator parity and ordinary checkpoint controls; stock state interchange is separate.
 */
class FlinkNativeBoundedPrimitiveArithmeticTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> modes() {
    var modes = new ArrayList<Arguments>();
    for (int code : new int[] {0, 2, 4, 5, 6, 1})
      for (int shape = 0; shape < 3; shape++)
        for (boolean rocks : new boolean[] {false, true})
          for (boolean distinct : new boolean[] {false, true})
            for (boolean nonfinite :
                (code == 1 || code == 6) ? new boolean[] {false, true} : new boolean[] {false})
              modes.add(Arguments.of(code, shape, rocks, distinct, nonfinite));
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void releasedRawSumsCountsAndAveragesMatchBeforeAndAfterOrdinaryCheckpoints(
      int code, int shape, boolean rocks, boolean distinct, boolean nonfinite) throws Exception {
    LogicalType type = type(code);
    RowType input = RowType.of(type, new BigIntType());
    RowType output = RowType.of(type, new BigIntType(), type, type, new BigIntType(), type);
    OperatorSubtaskState stockSnapshot, nativeSnapshot;
    List<List<String>> suffix = new ArrayList<>();
    Number high =
        code == 6
            ? 16_777_216L
            : code == 1
                ? 9_007_199_254_740_992L
                : code == 0
                    ? Long.MAX_VALUE
                    : code == 2 ? Integer.MAX_VALUE : code == 4 ? Short.MAX_VALUE : Byte.MAX_VALUE;
    Number[] values =
        nonfinite
            ? new Number[] {
              Double.POSITIVE_INFINITY, 1L, Double.NEGATIVE_INFINITY, null, null, null, 1L
            }
            : new Number[] {high, 1L, -high.longValue(), 1L, null, null, null, 1L};
    int prefixCount = nonfinite ? 6 : 3;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, shape, rocks, distinct, 2);
        var nativeOperator = nativeHarness(code, input, shape, rocks, distinct, 2)) {
      stock.setup(new RowDataSerializer(output));
      nativeOperator.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOperator.open();
      for (int i = 0; i < prefixCount; i++)
        assertEquals(
            stepStock(stock, row(code, values[i], 1000 + i), shape, output),
            stepNative(
                nativeOperator, row(code, values[i], 1000 + i), shape, input, allocator, output));
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOperator.snapshot(1, 1);
      for (int i = prefixCount; i < values.length; i++) {
        var expected = stepStock(stock, row(code, values[i], 1000 + i), shape, output);
        suffix.add(expected);
        assertEquals(
            expected,
            stepNative(
                nativeOperator, row(code, values[i], 1000 + i), shape, input, allocator, output));
      }
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, shape, rocks, distinct, 2);
        var nativeOperator = nativeHarness(code, input, shape, rocks, distinct, 2)) {
      stock.setup(new RowDataSerializer(output));
      nativeOperator.setup(new ArrowBatchSerializer());
      stock.initializeState(stockSnapshot);
      nativeOperator.initializeState(nativeSnapshot);
      stock.open();
      nativeOperator.open();
      for (int i = prefixCount; i < values.length; i++) {
        var expected = stepStock(stock, row(code, values[i], 1000 + i), shape, output);
        assertEquals(suffix.get(i - prefixCount), expected);
        assertEquals(
            expected,
            stepNative(
                nativeOperator, row(code, values[i], 1000 + i), shape, input, allocator, output));
      }
    }
  }

  static Stream<Arguments> rangeModes() {
    return Stream.of(1, 6)
        .flatMap(
            code ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .map(snapshot -> Arguments.of(code, rocks, snapshot))));
  }

  @ParameterizedTest
  @MethodSource("rangeModes")
  void floatingRetractionsKeepTheReleasedBackendMapOrder(
      int code, boolean rocks, boolean checkpoint) throws Exception {
    var type = type(code);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, new BigIntType(), type);
    OperatorSubtaskState stockSnapshot = null, nativeSnapshot = null;
    List<String> expected;
    Number high = code == 6 ? 16_777_216L : 9_007_199_254_740_992L;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 2, rocks, false, 32);
        var nativeOperator = nativeHarness(code, input, 2, rocks, false, 32)) {
      stock.setup(new RowDataSerializer(output));
      nativeOperator.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOperator.open();
      Number[] values = {1L, -high.longValue(), high};
      long[] times = {1000, 1008, 1016};
      for (int i = 0; i < 3; i++)
        assertEquals(
            stepStock(stock, row(code, values[i], times[i]), 2, output),
            stepNative(
                nativeOperator, row(code, values[i], times[i]), 2, input, allocator, output));
      if (checkpoint) {
        stockSnapshot = stock.snapshot(1, 1);
        nativeSnapshot = nativeOperator.snapshot(1, 1);
      }
      expected = stepStock(stock, row(code, 7L, 1064), 2, output);
      assertEquals(
          expected, stepNative(nativeOperator, row(code, 7L, 1064), 2, input, allocator, output));
      assertEquals(rocks ? "7.0" : "6.0", expected.get(0).split(":")[0]);
    }
    if (checkpoint)
      try (var allocator = new RootAllocator();
          var stock = stock(type, input, 2, rocks, false, 32);
          var nativeOperator = nativeHarness(code, input, 2, rocks, false, 32)) {
        stock.setup(new RowDataSerializer(output));
        nativeOperator.setup(new ArrowBatchSerializer());
        stock.initializeState(stockSnapshot);
        nativeOperator.initializeState(nativeSnapshot);
        stock.open();
        nativeOperator.open();
        assertEquals(expected, stepStock(stock, row(code, 7L, 1064), 2, output));
        assertEquals(
            expected, stepNative(nativeOperator, row(code, 7L, 1064), 2, input, allocator, output));
      }
  }

  static LogicalType type(int code) {
    switch (code) {
      case 0:
        return new BigIntType();
      case 2:
        return new IntType();
      case 4:
        return new SmallIntType();
      case 5:
        return new TinyIntType();
      case 6:
        return new FloatType();
      default:
        return new DoubleType();
    }
  }

  static RowData row(int code, Number number, long time) {
    Object value = null;
    if (number != null)
      switch (code) {
        case 0:
          value = number.longValue();
          break;
        case 2:
          value = number.intValue();
          break;
        case 4:
          value = number.shortValue();
          break;
        case 5:
          value = number.byteValue();
          break;
        case 6:
          value = number.floatValue();
          break;
        default:
          value = number.doubleValue();
      }
    return GenericRowData.of(value, time);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      LogicalType type, RowType input, int shape, boolean rocks, boolean distinct, long offset)
      throws Exception {
    var table =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.newInstance().inStreamingMode().build());
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var factory = planner.getTypeFactory();
    var calls = new ArrayList<AggregateCall>();
    var functions =
        List.of(
            SqlStdOperatorTable.SUM,
            SqlStdOperatorTable.SUM0,
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.AVG);
    for (int i = 0; i < functions.size(); i++)
      calls.add(
          AggregateCall.create(
              functions.get(i),
              distinct,
              List.of(0),
              -1,
              factory.createFieldTypeFromLogicalType(i == 2 ? new BigIntType() : type),
              "a" + i));
    boolean[] retract = {true, true, true, true};
    var info =
        AggregateUtil.transformToStreamAggregateInfoList(
            factory,
            RowType.of(type),
            JavaScalaConversionUtil.toScala(calls),
            retract,
            true,
            true,
            true);
    var builder =
        planner
            .createRelBuilder()
            .values(
                factory.buildRelNodeRowType(
                    JavaScalaConversionUtil.toScala(List.of("v")),
                    JavaScalaConversionUtil.toScala(List.of(type))));
    var generated =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(
                    table.getConfig(), Thread.currentThread().getContextClassLoader()),
                builder,
                JavaScalaConversionUtil.toScala(List.of(type)),
                false)
            .needAccumulate()
            .needRetract()
            .generateAggsHandler("PrimitiveBoundedOverContract", info);
    var state =
        Arrays.stream(info.getAccTypes()).map(t -> t.getLogicalType()).toArray(LogicalType[]::new);
    var fields = input.getChildren().toArray(new LogicalType[0]);
    KeyedProcessFunction<RowData, RowData, RowData> function =
        shape == 0
            ? new ProcTimeRowsBoundedPrecedingFunction<>(0, 0, generated, state, fields, offset + 1)
            : shape == 1
                ? new RowTimeRowsBoundedPrecedingFunction<>(
                    0, 0, generated, state, fields, offset + 1, 1)
                : new RowTimeRangeBoundedPrecedingFunction<>(generated, state, fields, offset, 1);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function),
            row -> serializer.toBinaryRow(GenericRowData.of(0L)).copy(),
            InternalTypeInfo.of(keyType));
    if (rocks) {
      Class<?> backend;
      try {
        backend = Class.forName("org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend");
      } catch (ClassNotFoundException older) {
        backend =
            Class.forName("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend");
      }
      harness.setStateBackend((StateBackend) backend.getConstructor().newInstance());
    }
    return harness;
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      int code, RowType input, int shape, boolean rocks, boolean distinct, long offset)
      throws Exception {
    int delta = distinct ? 100 : 0;
    var operator =
        new NativeOverAggregateOperator(
            1,
            new int[] {0, 0, 0, 0},
            new int[0],
            new int[] {code, code, code, code},
            new int[] {delta, delta + 22, delta + 3, delta + 4},
            shape == 2 ? 2 : 1,
            offset,
            shape == 0,
            new int[0],
            0,
            input,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(),
                  FlinkNativeBoundedPrimitiveArithmeticTest.class.getClassLoader()));
    return harness;
  }

  static List<String> stepStock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowData row,
      int shape,
      RowType output)
      throws Exception {
    if (shape == 0) harness.setProcessingTime(row.getLong(1));
    harness.getOutput().clear();
    harness.processElement(new StreamRecord<>(row));
    if (shape != 0) harness.processWatermark(new Watermark(row.getLong(1)));
    return values(harness.extractOutputValues(), output);
  }

  static List<String> stepNative(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowData row,
      int shape,
      RowType input,
      RootAllocator allocator,
      RowType output)
      throws Exception {
    if (shape == 0) harness.setProcessingTime(row.getLong(1));
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
    if (shape != 0) harness.processWatermark(new Watermark(row.getLong(1)));
    var result = new ArrayList<String>();
    for (var batch : harness.extractOutputValues()) {
      try (var root = batch.root()) {
        result.addAll(values(RowDataArrowConverter.read(root, output), output));
      }
    }
    harness.getOutput().clear();
    return result;
  }

  static List<String> values(List<RowData> rows, RowType output) {
    var values = new ArrayList<String>();
    for (var row : rows) {
      var fields = new ArrayList<String>();
      for (int i = 2; i < 6; i++)
        fields.add(
            String.valueOf(RowData.createFieldGetter(output.getTypeAt(i), i).getFieldOrNull(row)));
      values.add(String.join(":", fields));
    }
    return values;
  }
}
