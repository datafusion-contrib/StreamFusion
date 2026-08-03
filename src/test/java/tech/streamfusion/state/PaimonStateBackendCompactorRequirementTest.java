package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * This module carries no state-table compactor, and the backend has no compactor-less mode:
 * state tables always carry deletion vectors, which only the compactor's synchronous barrier
 * maintenance keeps correct, so backend creation must fail closed with a message naming the
 * required deployment. The full backend suites run in streamfusion-paimon-compactor, where the
 * compactor is on the classpath.
 */
class PaimonStateBackendCompactorRequirementTest {

  @Test
  void backendCreationFailsClosedWithoutACompactor() throws Exception {
    KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new StreamMap<>(value -> value), value -> value, Types.LONG);
    harness.setStateBackend(new PaimonStateBackend());
    Exception failure = assertThrows(Exception.class, harness::open);
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    assertTrue(
        messages.toString().contains("deploy streamfusion-paimon-compactor"),
        "the failure must name the required deployment, got: " + messages);
  }
}
