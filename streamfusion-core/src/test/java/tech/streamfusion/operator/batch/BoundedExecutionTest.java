package tech.streamfusion.operator.batch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class BoundedExecutionTest {
  @Test
  void emptyInputFinishesOnceAndRejectsAndReleasesLaterInput() {
    var execution =
        new BoundedExecution(
            ignored -> {
              throw new AssertionError("empty input must not create a native task");
            },
            schema -> schema,
            () -> null,
            ignored -> {
              throw new AssertionError("empty input must not emit output");
            },
            null);
    execution.endInput();
    assertDoesNotThrow(execution::endInput);
    assertDoesNotThrow(execution::close);
    assertDoesNotThrow(execution::close);
    assertEquals(0, execution.statistics()[0]);
    try (var allocator = new RootAllocator(1024)) {
      var vector = new IntVector("value", allocator);
      vector.allocateNew(1);
      vector.setSafe(0, 7);
      vector.setValueCount(1);
      var buffer = vector.getDataBuffer();
      var input = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), 1);
      var failure = assertThrows(IllegalStateException.class, () -> execution.accept(input));
      assertEquals("bounded task input already ended", failure.getMessage());
      assertEquals(0, buffer.refCnt());
    }
  }
}
