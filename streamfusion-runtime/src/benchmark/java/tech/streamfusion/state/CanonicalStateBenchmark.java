package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Measures only production native-partition installation in the real Flink heap backend. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class CanonicalStateBenchmark {
  @Test
  void compare() throws Exception {
    var allocations = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    assertTrue(allocations.isThreadAllocatedMemorySupported());
    allocations.setThreadAllocatedMemoryEnabled(true);
    int warmups = Integer.parseInt(System.getenv().getOrDefault("SF_WARMUP", "2"));
    int runs = Integer.parseInt(System.getenv().getOrDefault("SF_RUNS", "5"));
    for (int bytes : new int[] {4096, 2 * 1024 * 1024, CanonicalNativeState.CHUNK_BYTES + 1}) {
      int groups = bytes > CanonicalNativeState.CHUNK_BYTES ? 16 : 128;
      byte[][] partitions =
          IntStream.range(0, groups)
              .mapToObj(i -> CanonicalNativeStateTest.partition(i, bytes))
              .toArray(byte[][]::new);
      var operator = new CanonicalNativeStateTest.Operator();
      try (var harness = CanonicalNativeStateTest.harness(operator)) {
        harness.open();
        for (int trial = -warmups; trial < runs; trial++) {
          long allocated = allocations.getThreadAllocatedBytes(Thread.currentThread().getId());
          long start = System.nanoTime();
          CanonicalNativeState.write(
              operator.backend(), partitions, CanonicalNativeStateTest.OPERATOR, 100);
          long nanos = System.nanoTime() - start;
          allocated =
              allocations.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocated;
          System.out.printf(
              java.util.Locale.ROOT,
              "CANONICAL_STATE"
                  + " {\"payloadBytes\":%d,\"groups\":%d,\"trial\":%d,\"nanoseconds\":%d,\"allocatedBytes\":%d}%n",
              bytes,
              groups,
              trial,
              nanos,
              allocated);
        }
        var restored =
            CanonicalNativeState.readAndClear(
                operator.backend(), CanonicalNativeStateTest.OPERATOR);
        assertEquals(groups, restored.partitions.size());
        for (int group = 0; group < groups; group++) {
          ByteBuffer expected = ByteBuffer.wrap(partitions[group], Integer.BYTES, bytes).slice();
          assertEquals(expected, ByteBuffer.wrap(restored.partitions.get(group)));
        }
      }
    }
  }
}
