package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ColumnarOutputFailureTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void failedReceiverReleasesOnlyTheUnclaimedOwnership(boolean claimsInput) throws Exception {
    var failure = new IllegalStateException("downstream rejected output");
    Output<StreamRecord<ArrowBatch>> output =
        (Output<StreamRecord<ArrowBatch>>)
            java.lang.reflect.Proxy.newProxyInstance(
                Output.class.getClassLoader(),
                new Class<?>[] {Output.class},
                (proxy, method, arguments) -> {
                  if (method.getName().equals("collect") && arguments.length == 1) {
                    if (claimsInput) {
                      var record = (StreamRecord<ArrowBatch>) arguments[0];
                      try (VectorSchemaRoot owned = record.getValue().root()) {
                        assertEquals(7, ((IntVector) owned.getVector(0)).get(0));
                      }
                    }
                    throw failure;
                  }
                  return null;
                });
    try (var allocator = new RootAllocator()) {
      IntVector vector = new IntVector("value", allocator);
      vector.allocateNew(1);
      vector.setSafe(0, 7);
      VectorSchemaRoot root = VectorSchemaRoot.of(vector);
      root.setRowCount(1);
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> {
                ColumnarRecordMetrics.emitOwned(
                    output, UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(), root);
              }));
      assertEquals(0, failure.getSuppressed().length, "claimed buffers must not close twice");
      assertEquals(0, allocator.getAllocatedMemory(), "cleanup must not wait for the Cleaner");
    }
  }
}
