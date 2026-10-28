package tech.streamfusion.starrocks;

import java.io.IOException;
import tech.streamfusion.NativeExtensionLoader;

/**
 * JNI entry points of the StarRocks Stream Load row encoder. HTTP and transactions stay in Java.
 */
public final class NativeStarRocks {

  static {
    NativeExtensionLoader.load(
        NativeStarRocks.class,
        "starrocks",
        NativeStarRocks::nativeBuildVersion,
        NativeStarRocks::liveNativeHandles);
  }

  private NativeStarRocks() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Live handles owned by this library, used by the shared leak sentinel. */
  public static native String liveNativeHandles();

  /** Whether the native StarRocks extension loaded successfully. */
  public static native boolean isLoaded();

  /**
   * Creates a task-local encoder. {@code jsonOrder} lists JSON keys in the connector's row-map
   * order as field indexes, with {@code -1} for {@code __op}; CSV ignores it.
   */
  static native long createEncoder(
      boolean json,
      byte[] columnSeparator,
      String[] logicalTypes,
      String[] fieldNames,
      int[] jsonOrder,
      boolean upsertDelete,
      boolean ignoreUpdateBefore)
      throws IOException;

  /** Imports one exported Arrow batch whose rows the following {@link #next} calls return. */
  static native void begin(long handle, long arrayAddress, long schemaAddress) throws IOException;

  /**
   * Encodes the current batch's next slab of about a megabyte: the connector's rows back to back
   * after its row-kind filtering, with each row's end offset in {@code ends} and the row count in
   * {@code rows[0]}. Returns null once every row has been handed over.
   */
  static native byte[] next(long handle, int[] ends, int[] rows) throws IOException;

  /** Releases the current batch, also when its rows were not all consumed. */
  static native void reset(long handle);

  /** Releases a task-local encoder. */
  static native void closeEncoder(long handle);
}
