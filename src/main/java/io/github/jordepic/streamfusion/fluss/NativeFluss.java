package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Fluss source. */
public final class NativeFluss {

  static {
    NativeExtensionLoader.load(
        NativeFluss.class, "fluss", NativeFluss::nativeBuildVersion);
  }

  private NativeFluss() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  public static native boolean isLoaded();

  public static native long openFlussReader(
      String[] configKeys,
      String[] configValues,
      String databaseName,
      String tableName,
      int[] projectedFields,
      int rowtimeIndex);

  public static native void assignFlussSplits(
      long handle,
      String[] splitIds,
      long[] tableIds,
      long[] partitionIds,
      long[] buckets,
      long[] startOffsets,
      long[] stoppingOffsets);

  public static native void unassignFlussSplits(
      long handle, long[] tableIds, long[] partitionIds, long[] buckets);

  public static native int pollFlussBatch(long handle, long timeoutMillis);

  public static native int drainFlussSplit(
      long handle, long[] splitMeta, String[] outSplitId, long outArrayAddress, long outSchemaAddress);

  public static native void closeFlussReader(long handle);
}
