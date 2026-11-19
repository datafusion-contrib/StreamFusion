package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class WatermarkExpressionTest {
  @Test
  void nativeCalendarKernelMatchesFlinkOnRandomAndExtremeMilliseconds() {
    Random random = new Random(731);
    long[] timestamps = new long[2048];
    for (int i = 0; i < timestamps.length; i++) {
      timestamps[i] = random.nextLong();
    }
    timestamps[0] = Long.MIN_VALUE;
    timestamps[1] = Long.MAX_VALUE;
    timestamps[2] = -1;
    timestamps[3] = 0;
    int[] months = {0, 1, 12, 13, Integer.MAX_VALUE, random.nextInt(Integer.MAX_VALUE)};
    try (var allocator = new RootAllocator();
        var vector = new BigIntVector("epoch", allocator)) {
      vector.allocateNew(timestamps.length + 1);
      for (int i = 0; i < timestamps.length; i++) {
        vector.set(i, timestamps[i]);
      }
      vector.setNull(timestamps.length);
      try (var root =
          new VectorSchemaRoot(
              List.of(vector.getField()), List.of(vector), timestamps.length + 1)) {
        root.setRowCount(timestamps.length + 1);
        for (int shift : months) {
          try (var evaluator = WatermarkExpression.subtractMonths(0, shift).open()) {
            // The second invocation uses the native handle's cached schema and compiled expression.
            for (int batch = 0; batch < 2; batch++) {
              try (var values = evaluator.evaluate(root)) {
                for (int i = 0; i < timestamps.length; i++) {
                  assertEquals(
                      DateTimeUtils.addMonths(timestamps[i], -shift),
                      values.getMillis(i),
                      "timestamp=" + timestamps[i] + ", months=" + shift);
                }
                assertTrue(values.isNull(timestamps.length));
              }
            }
          }
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void serializedComposedProjectionKeepsEvaluationOrderAndInputOwnership() throws Exception {
    // (rt - 1 MONTH) - 1 MONTH is not equivalent to rt - 2 MONTH at March's month end.
    var expression =
        new WatermarkExpression(
            new int[] {6, 6, 0, 7, 7},
            new int[] {
              WatermarkExpression.SUBTRACT_MONTHS, WatermarkExpression.SUBTRACT_MONTHS, 0, 0, 0
            },
            new int[] {2, 2, 0, 0, 0},
            new long[] {1},
            new double[0],
            new String[0],
            "two calendar subtractions");
    WatermarkExpression copy = InstantiationUtil.clone(expression);
    var remapped = copy.remapInput(0, 1);
    assertEquals(expression.digest(), copy.digest());
    assertNotEquals(expression.digest(), remapped.digest());
    try (var allocator = new RootAllocator();
        var unrelated = new BigIntVector("other", allocator);
        var timestamp = new BigIntVector("rt", allocator);
        var evaluator = remapped.open()) {
      unrelated.allocateNew(1);
      unrelated.set(0, 0);
      timestamp.allocateNew(1);
      long input = Instant.parse("2024-03-31T12:00:00Z").toEpochMilli();
      timestamp.set(0, input);
      try (var root = new VectorSchemaRoot(List.of(unrelated, timestamp))) {
        root.setRowCount(1);
        try (var values = evaluator.evaluate(root)) {
          assertEquals(Instant.parse("2024-01-29T12:00:00Z").toEpochMilli(), values.getMillis(0));
          assertEquals(input, timestamp.get(0));
        }
        assertEquals(input, timestamp.get(0));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void fixedProjectionBorrowsInputWithoutAllocatingCandidateBuffers() {
    try (var allocator = new RootAllocator();
        var vector = new BigIntVector("rt", allocator);
        var evaluator = WatermarkExpression.subtractMillis(0, 1).open()) {
      vector.allocateNew(2);
      vector.set(0, Long.MIN_VALUE);
      vector.setNull(1);
      try (var root = new VectorSchemaRoot(List.of(vector))) {
        root.setRowCount(2);
        long allocated = allocator.getAllocatedMemory();
        try (var values = evaluator.evaluate(root)) {
          assertEquals(Long.MAX_VALUE, values.getMillis(0));
          assertTrue(values.isNull(1));
          assertEquals(allocated, allocator.getAllocatedMemory());
        }
        assertEquals(Long.MIN_VALUE, vector.get(0));
      }
    }
  }
}
