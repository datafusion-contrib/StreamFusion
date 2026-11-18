package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.Version;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.JsonRecyclerPools;
import org.junit.jupiter.api.Test;

class NativeJsonRuntimeTest {
  @Test
  void supportedRuntimeReturnsTheExistingBufferToTheSharedPool() {
    var factory = new JsonFactory();
    var recycler = factory._getBufferRecycler();
    char[] buffer = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, 16000);
    recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, buffer);
    recycler.releaseToPool();
    assertTrue(NativeJsonRuntime.available());
    assertTrue(NativeJsonRuntime.probe(JsonFactory::new, JsonFactory::new));
    var other = new JsonFactory()._getBufferRecycler();
    char[] returned = other.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
    try {
      assertSame(buffer, returned);
      assertEquals(buffer.length, returned.length);
    } finally {
      other.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, returned);
      other.releaseToPool();
    }
  }

  @Test
  void unverifiedVersionsAndNonThreadLocalPoolsFailClosed() {
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory() {
                  @Override
                  public Version version() {
                    return new Version(2, 17, 0, null, "test", "test");
                  }
                },
            JsonFactory::new));
    assertFalse(
        NativeJsonRuntime.probe(
            () -> JsonFactory.builder().recyclerPool(JsonRecyclerPools.newBoundedPool(2)).build(),
            JsonFactory::new));
    assertFalse(
        NativeJsonRuntime.probe(
            JsonFactory::new,
            () ->
                JsonFactory.builder().recyclerPool(JsonRecyclerPools.nonRecyclingPool()).build()));
  }

  @Test
  void missingConstructionAndReleaseMethodsFailClosed() {
    assertFalse(
        NativeJsonRuntime.probe(
            () -> {
              throw new NoClassDefFoundError("shaded Jackson");
            },
            JsonFactory::new));
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory() {
                  @Override
                  public BufferRecycler _getBufferRecycler() {
                    throw new NoSuchMethodError("_getBufferRecycler");
                  }
                },
            JsonFactory::new));
    assertFalse(
        NativeJsonRuntime.probe(
            () ->
                new JsonFactory() {
                  @Override
                  public BufferRecycler _getBufferRecycler() {
                    return new BufferRecycler() {
                      @Override
                      public void releaseToPool() {
                        throw new NoSuchMethodError("releaseToPool");
                      }
                    };
                  }
                },
            JsonFactory::new));
  }
}
