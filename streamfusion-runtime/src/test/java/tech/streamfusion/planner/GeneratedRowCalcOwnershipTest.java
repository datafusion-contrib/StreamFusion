package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Base64;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeCalcOperator;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.operator.RowDataArrowConverter;

class GeneratedRowCalcOwnershipTest {
  private static FlinkExpressionFunction function() throws Exception {
    return function(false);
  }

  private static FlinkExpressionFunction function(boolean wide) throws Exception {
    var function = unopenedFunction(wide);
    function.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return GeneratedRowCalcOwnershipTest.class.getClassLoader();
          }
        });
    return function;
  }

  private static FlinkExpressionFunction unopenedFunction(boolean wide) {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var ref =
        rex.makeInputRef(
            types.createTypeWithNullability(
                types.createSqlType(wide ? SqlTypeName.BIGINT : SqlTypeName.INTEGER), true),
            0);
    LogicalType logical =
        wide ? new org.apache.flink.table.types.logical.BigIntType() : new IntType();
    return FlinkExpressionGenerator.generate(
        List.of(ref, ref),
        null,
        new LogicalType[] {logical},
        RowType.of(logical, logical),
        TableConfig.getDefault(),
        GeneratedRowCalcOwnershipTest.class.getClassLoader());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void alternatingChangelogBatchesKeepRoutingAndOnlyUseDirectEvaluationForInserts(boolean wide)
      throws Exception {
    var function = unopenedFunction(wide);
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {wide ? NativeUdf.TYPE_LONG : NativeUdf.TYPE_INT},
            NativeUdf.TYPE_ROW);
    var binding =
        new NativeUdf.Binding(
            new NativeUdf.Descriptor[] {descriptor}, new int[] {0}, new int[] {0});
    var calc =
        new NativeCalcOperator(
            new int[] {38, 0},
            new int[] {0, 0},
            new int[] {1, 0},
            new long[] {0, 0},
            new double[0],
            new String[] {
              Base64.getEncoder().encodeToString(function.arrowResultSchema().serializeAsMessage())
            },
            new int[] {0},
            -1,
            function.rowResultType().getFieldNames().toArray(new String[0]),
            binding);
    var counter = NativeCalcOperator.class.getDeclaredField("directPrimitiveRows");
    counter.setAccessible(true);
    try (var allocator = new RootAllocator();
        var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(calc)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      long directRows = 0;
      for (int batch = 0; batch < 4; batch++) {
        boolean changelog = batch % 2 == 1;
        var rows = new java.util.ArrayList<RowData>();
        for (int row = 0; row < 4; row++) {
          Object value;
          if (row == 0) value = null;
          else if (wide) value = Long.valueOf(Long.MAX_VALUE - row);
          else value = Integer.valueOf(1000 + row);
          rows.add(
              GenericRowData.ofKind(changelog ? RowKind.values()[row] : RowKind.INSERT, value));
        }
        int keyGroup = batch + 7;
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        rows, RowType.of(function.argumentTypes()), allocator, changelog),
                    keyGroup)));
        assertEquals(1, harness.getOutput().size());
        var output = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().poll()).getValue();
        assertEquals(keyGroup, output.keyGroup());
        try (var root = output.root()) {
          if (!changelog)
            assertEquals(
                0, allocator.getAllocatedMemory(), "direct outputs own independent buffers");
          var actual = RowDataArrowConverter.read(root, function.rowResultType());
          assertEquals(rows.size(), actual.size());
          for (int row = 0; row < rows.size(); row++) {
            assertEquals(rows.get(row).getRowKind(), actual.get(row).getRowKind());
            for (int column = 0; column < 2; column++) {
              assertEquals(rows.get(row).isNullAt(0), actual.get(row).isNullAt(column));
              if (row == 0) continue;
              if (wide) assertEquals(rows.get(row).getLong(0), actual.get(row).getLong(column));
              else assertEquals(rows.get(row).getInt(0), actual.get(row).getInt(column));
            }
          }
        }
        assertEquals(0, allocator.getAllocatedMemory(), "all input buffers are released");
        if (!changelog) directRows += rows.size();
        assertEquals(directRows, ((org.apache.flink.metrics.Counter) counter.get(calc)).getCount());
      }
    }
  }

  private static int register(FlinkExpressionFunction function) throws Exception {
    return NativeUdf.register(
        function,
        FlinkExpressionFunction.class.getMethod("eval", Object[].class),
        new int[] {
          function.argumentTypes()[0] instanceof IntType ? NativeUdf.TYPE_INT : NativeUdf.TYPE_LONG
        },
        NativeUdf.TYPE_ROW);
  }

  private static VectorSchemaRoot evaluate(int id, VectorSchemaRoot input) throws Exception {
    var method =
        NativeUdf.class.getDeclaredMethod(
            "evaluateRowCalc", int.class, VectorSchemaRoot.class, int[].class);
    method.setAccessible(true);
    try {
      return (VectorSchemaRoot) method.invoke(null, id, input, new int[] {0});
    } catch (InvocationTargetException failure) {
      if (failure.getCause() instanceof Exception) throw (Exception) failure.getCause();
      throw (Error) failure.getCause();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void slicedInputCanCloseBeforeReadingTheFlatResults(boolean wide) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var function = function(wide);
    int id = register(function);
    try {
      VectorSchemaRoot output;
      try (var input = input(10, wide);
          var slice = input.slice(6, 3)) {
        output = evaluate(id, slice);
      }
      try (output) {
        var reader = ArrowConversion.createArrowReader(output, function.rowResultType());
        assertEquals(3, output.getRowCount());
        if (wide) assertEquals(Long.MIN_VALUE + 6, reader.read(0).getLong(0));
        else assertEquals(1006, reader.read(0).getInt(0));
        assertTrue(reader.read(1).isNullAt(0));
        assertTrue(reader.read(1).isNullAt(1));
        if (wide) assertEquals(Long.MAX_VALUE - 8, reader.read(2).getLong(1));
        else assertEquals(1008, reader.read(2).getInt(1));
      }
      try (var empty = input(0, wide);
          var result = evaluate(id, empty)) {
        assertEquals(0, result.getRowCount());
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static VectorSchemaRoot input(int rows, boolean wide) {
    if (!wide) return GeneratedExpressionDispatchTest.input(rows);
    var vector = new org.apache.arrow.vector.BigIntVector("id", NativeAllocator.SHARED);
    vector.allocateNew(rows);
    for (int row = 0; row < rows; row++) {
      if (row % 7 == 0) vector.setNull(row);
      else vector.set(row, row < 8 ? Long.MIN_VALUE + row : Long.MAX_VALUE - row);
    }
    vector.setValueCount(rows);
    return new VectorSchemaRoot(List.of(vector));
  }

  @ParameterizedTest
  @ValueSource(strings = {"checked", "runtime", "error"})
  void partialOutputIsFreedWhenEvaluationFails(String kind) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var function = function();
    var field = FlinkExpressionFunction.class.getDeclaredField("evaluator");
    field.setAccessible(true);
    var original = (FlinkExpressionFunction.Evaluator) field.get(function);
    Throwable expected =
        kind.equals("checked")
            ? new java.io.IOException("checked")
            : kind.equals("runtime")
                ? new IllegalStateException("runtime")
                : new AssertionError("error");
    field.set(
        function,
        new FlinkExpressionFunction.Evaluator() {
          private int rows;

          @Override
          public Object eval(RowData input) throws Exception {
            if (++rows == 2) {
              if (expected instanceof Error) throw (Error) expected;
              throw (Exception) expected;
            }
            return original.eval(input);
          }

          @Override
          public void open(FunctionContext context) {}

          @Override
          public void close() throws Exception {
            original.close();
          }
        });
    int id = register(function);
    try (var input = GeneratedExpressionDispatchTest.input(3)) {
      Throwable actual = assertThrows(RuntimeException.class, () -> evaluate(id, input));
      assertSame(
          expected,
          actual instanceof RuntimeException && actual.getCause() != null
              ? actual.getCause()
              : actual);
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
