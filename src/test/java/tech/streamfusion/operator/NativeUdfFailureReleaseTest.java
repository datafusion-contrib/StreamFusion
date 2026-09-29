package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;

class NativeUdfFailureReleaseTest {
  public static class FailingFunction extends ScalarFunction {
    final IllegalArgumentException failure = new IllegalArgumentException("scalar callback failed");

    public Integer eval(Integer value) {
      throw failure;
    }
  }

  @Test
  void callbackFailureSurvivesReleaseOfExportedInput() throws Exception {
    var function = new FailingFunction();
    int id =
        NativeUdf.register(
            function,
            FailingFunction.class.getMethod("eval", Integer.class),
            new int[] {NativeUdf.TYPE_INT},
            NativeUdf.TYPE_INT);
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    long calc =
        Native.createCalcExpression(
            new int[] {17, 0},
            new int[] {0, 0},
            new int[] {1, 0},
            new long[] {id, NativeUdf.TYPE_INT},
            new double[0],
            new String[0],
            new int[] {0},
            -1,
            new String[] {"result"});
    try (var allocator = new RootAllocator()) {
      for (int i = 0; i < 32; i++) {
        try (var input = ArrowArray.allocateNew(allocator);
            var inputSchema = ArrowSchema.allocateNew(allocator);
            var output = ArrowArray.allocateNew(allocator);
            var outputSchema = ArrowSchema.allocateNew(allocator)) {
          var schema = new Schema(List.of(Field.nullable("value", new ArrowType.Int(32, true))));
          try (var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            ((IntVector) root.getVector(0)).set(0, i);
            root.setRowCount(1);
            Data.exportVectorSchemaRoot(
                allocator, root, NativeAllocator.DICTIONARIES, input, inputSchema);
          }
          // The exported input owns the buffers after the producer closes. Its release callback
          // must run safely even when the scalar callback throws on the same JNI thread.
          var failure =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      Native.calcExpression(
                          calc,
                          input.memoryAddress(),
                          inputSchema.memoryAddress(),
                          output.memoryAddress(),
                          outputSchema.memoryAddress()));
          assertSame(function.failure, failure);
          assertSame(function.failure, NativeUdf.propagateUpcallFailure(failure));
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    } finally {
      Native.closeCalcExpression(calc);
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
