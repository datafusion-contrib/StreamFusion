package tech.streamfusion.fluss;

import tech.streamfusion.NativeExtensionLoader;

/** Columnar Fluss key routing and statistics; connection management remains in the Java SDK. */
public final class NativeFluss {
  static {
    NativeExtensionLoader.load(
        NativeFluss.class,
        "fluss",
        NativeFluss::nativeBuildVersion,
        NativeFluss::liveNativeHandles);
  }

  private NativeFluss() {}

  public static void requireLoaded() {}

  private static native String nativeBuildVersion();

  public static native String liveNativeHandles();

  static native long splitByBucket(
      long array, long schema, int[] columns, int[] timestampPrecisions, int bucketCount);

  static native int nextBucketSlice(long handle, long array, long schema);

  static native void closeBucketSplit(long handle);

  static native int[] statistics(long array, long schema, int[] columns);
}
