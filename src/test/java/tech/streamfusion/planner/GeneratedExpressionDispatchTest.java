package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeUdf;

class GeneratedExpressionDispatchTest {
  static FlinkExpressionFunction identity() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var function =
        new FlinkExpressionFunction(
            rex.makeInputRef(
                types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true), 0),
            new LogicalType[] {new IntType()},
            TableConfig.getDefault(),
            GeneratedExpressionDispatchTest.class.getClassLoader());
    function.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return GeneratedExpressionDispatchTest.class.getClassLoader();
          }
        });
    return function;
  }

  static int register(boolean direct) throws Exception {
    var generated = identity();
    ScalarFunction function = direct ? generated : new ReflectiveIdentity(generated);
    return NativeUdf.register(
        function,
        function.getClass().getMethod("eval", Object[].class),
        new int[] {NativeUdf.TYPE_INT},
        NativeUdf.TYPE_INT);
  }

  static VectorSchemaRoot input(int rows) {
    var vector = new IntVector("id", NativeAllocator.SHARED);
    vector.allocateNew(rows);
    for (int i = 0; i < rows; i++) {
      if (i % 7 == 0) vector.setNull(i);
      else vector.set(i, i + 1000);
    }
    vector.setValueCount(rows);
    return new VectorSchemaRoot(List.of(vector));
  }

  static VectorSchemaRoot invoke(int id, VectorSchemaRoot input) {
    try (var inArray = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var outArray = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, input, NativeAllocator.DICTIONARIES, inArray, inSchema);
      NativeUdf.invokeUdf(
          id,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      return Data.importVectorSchemaRoot(
          NativeAllocator.SHARED, outArray, outSchema, NativeAllocator.DICTIONARIES);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void slicesNullsAndOutputOwnershipMatchReflectiveDispatch(boolean direct) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id = register(direct);
    try {
      VectorSchemaRoot output;
      try (var input = input(10);
          var slice = input.slice(6, 3)) {
        output = invoke(id, slice);
      }
      try (output) {
        assertEquals(3, output.getRowCount());
        var result = (IntVector) output.getVector(0);
        assertEquals(1006, result.getObject(0));
        assertEquals(null, result.getObject(1));
        assertEquals(1008, result.getObject(2));
      }
      try (var input = input(0);
          var outputEmpty = invoke(id, input)) {
        assertEquals(0, outputEmpty.getRowCount());
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @ValueSource(strings = {"checked", "runtime", "error"})
  void failureKeepsItsCauseAndReleasesPartiallyWrittenResults(String kind) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var function = identity();
    var evaluator = FlinkExpressionFunction.class.getDeclaredField("evaluator");
    evaluator.setAccessible(true);
    var original = (FlinkExpressionFunction.Evaluator) evaluator.get(function);
    Throwable failure =
        switch (kind) {
          case "checked" -> new java.io.IOException("generated checked failure");
          case "runtime" -> new IllegalArgumentException("generated runtime failure");
          default -> new AssertionError("generated error");
        };
    evaluator.set(
        function,
        new FlinkExpressionFunction.Evaluator() {
          private int rows;

          @Override
          public Object eval(RowData row) throws Exception {
            if (++rows == 2) {
              if (failure instanceof Error error) throw error;
              throw (Exception) failure;
            }
            return original.eval(row);
          }

          @Override
          public void open(FunctionContext context) {}

          @Override
          public void close() throws Exception {
            original.close();
          }
        });
    int id =
        NativeUdf.register(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_INT},
            NativeUdf.TYPE_INT);
    try (var input = input(3)) {
      var error = assertThrows(RuntimeException.class, () -> invoke(id, input));
      org.junit.jupiter.api.Assertions.assertSame(
          failure, failure instanceof RuntimeException ? error : error.getCause());
    } finally {
      NativeUdf.unregister(id);
      var propagate =
          NativeUdf.class.getDeclaredMethod("propagateUpcallFailure", RuntimeException.class);
      propagate.setAccessible(true);
      var restored =
          (RuntimeException) propagate.invoke(null, new RuntimeException("native wrapper"));
      org.junit.jupiter.api.Assertions.assertSame(
          failure, failure instanceof RuntimeException ? restored : restored.getCause());
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  public static final class ReflectiveIdentity extends ScalarFunction {
    private final FlinkExpressionFunction generated;

    ReflectiveIdentity(FlinkExpressionFunction generated) {
      this.generated = generated;
    }

    public Object eval(Object... arguments) throws Exception {
      return generated.eval(arguments);
    }

    @Override
    public void close() throws Exception {
      generated.close();
    }
  }
}
