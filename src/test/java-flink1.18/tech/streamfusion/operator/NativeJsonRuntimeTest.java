package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.junit.jupiter.api.Test;

class NativeJsonRuntimeTest {
  @Test
  void olderJacksonSharesItsThreadLocalBuffer() {
    assertTrue(NativeJsonRuntime.available());
    assertTrue(NativeJsonRuntime.probe(JsonFactory::new, JsonFactory::new));
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory()
                    .disable(JsonFactory.Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING),
            JsonFactory::new));
  }

  @Test
  void missingRuntimeFailsClosed() {
    assertFalse(
        NativeJsonRuntime.probe(
            () -> {
              throw new NoClassDefFoundError("shaded Jackson");
            },
            JsonFactory::new));
  }
}
