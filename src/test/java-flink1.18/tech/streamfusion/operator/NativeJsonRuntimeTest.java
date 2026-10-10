package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.Version;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;
import org.junit.jupiter.api.Test;

class NativeJsonRuntimeTest {
  @Test
  void olderJacksonSharesItsThreadLocalBuffer() {
    var recycler = new JsonFactory()._getBufferRecycler();
    char[] buffer = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, 16000);
    recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, buffer);
    assertTrue(NativeJsonRuntime.available());
    assertTrue(NativeJsonRuntime.probe(JsonFactory::new, JsonFactory::new));
    var other = new JsonFactory()._getBufferRecycler();
    char[] returned = other.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
    try {
      assertSame(recycler, other);
      assertSame(buffer, returned);
    } finally {
      other.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, returned);
    }
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory()
                    .disable(JsonFactory.Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING),
            JsonFactory::new));
  }

  @Test
  void unverifiedJacksonVersionFailsClosed() {
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory() {
                  @Override
                  public Version version() {
                    return new Version(2, 13, 0, null, "test", "test");
                  }
                },
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
