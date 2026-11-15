package tech.streamfusion.operator;

import java.util.function.Supplier;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;
import tech.streamfusion.compat.JsonRuntimeCompat;

/**
 * Jackson input-buffer state shared with Flink's SQL/JSON parser on the task thread. Flink's
 * SqlJsonUtils also uses a default JsonFactory, so both factories access the same thread-local
 * pool.
 */
public final class NativeJsonRuntime {
  private static final class Factory {
    static final JsonFactory INSTANCE = new JsonFactory();
  }

  private static final class Availability {
    static final boolean SUPPORTED = probe(() -> Factory.INSTANCE, JsonFactory::new);
  }

  private final BufferRecycler recycler;
  private final char[] buffer;

  public NativeJsonRuntime() {
    this(Factory.INSTANCE);
  }

  private NativeJsonRuntime(JsonFactory factory) {
    recycler = factory._getBufferRecycler();
    buffer = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
  }

  /** Verify the shaded Jackson contract once per class loader, before admitting SQL/JSON. */
  public static boolean available() {
    try {
      return Availability.SUPPORTED;
    } catch (LinkageError | RuntimeException incompatibleJackson) {
      return false;
    }
  }

  static boolean probe(Supplier<JsonFactory> nativeFactory, Supplier<JsonFactory> hostFactory) {
    try {
      JsonFactory factory = nativeFactory.get();
      JsonFactory other = hostFactory.get();
      if (!verifiedFactory(factory) || !verifiedFactory(other)) {
        return false;
      }
      NativeJsonRuntime runtime = new NativeJsonRuntime(factory);
      runtime.release(runtime.bufferSize());
      NativeJsonRuntime shared = new NativeJsonRuntime(other);
      try {
        return shared.bufferSize() > 0
            && shared.recycler == runtime.recycler
            && shared.buffer == runtime.buffer;
      } finally {
        shared.release(shared.bufferSize());
      }
    } catch (LinkageError | RuntimeException incompatibleJackson) {
      return false;
    }
  }

  private static boolean verifiedFactory(JsonFactory factory) {
    return JsonRuntimeCompat.verifiedFactory(factory);
  }

  public boolean legacySemantics() {
    return JsonRuntimeCompat.LEGACY_SQL_JSON;
  }

  public int bufferSize() {
    return buffer.length;
  }

  public void release(int requiredSize) {
    // createParser(String) grows this buffer for inputs of at most 32768 UTF-16 units.
    // Larger inputs use a StringReader with the existing capacity, even across parser calls.
    recycler.releaseCharBuffer(
        BufferRecycler.CHAR_TOKEN_BUFFER,
        requiredSize > buffer.length ? new char[requiredSize] : buffer);
    JsonRuntimeCompat.releaseToPool(recycler);
  }
}
