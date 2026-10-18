package tech.streamfusion.iceberg;

import tech.streamfusion.NativeExtensionLoader;

/** JNI boundary for the optional Iceberg data-file implementation. */
final class NativeIceberg {
  static {
    NativeExtensionLoader.load(
        NativeIceberg.class,
        "iceberg",
        NativeIceberg::nativeBuildVersion,
        NativeIceberg::liveNativeHandles);
  }

  private NativeIceberg() {}

  static boolean isLoaded() {
    return true;
  }

  private static native String nativeBuildVersion();

  static native String liveNativeHandles();

  static native long createWriter(
      long schemaAddress,
      String icebergSchema,
      String settings,
      String location,
      Object output,
      byte[] transferBuffer);

  static native long retainWriteBatch(long writer, long arrayAddress);

  static native void releaseWriteBatch(long batch);

  static native long write(long handle, long batch, int[] selected, int count);

  static native long estimatedBytes(long handle);

  static native String finish(long handle);

  static native void closeWriter(long handle);

  static native long route(long array, long schema, int[] destinations);

  static native int nextRoute(long handle, long array, long schema);

  static native void closeRoute(long handle);

  static native long createReader(
      String task, Object files, int batchRows, long skip, long outputSchema);

  static native boolean nextRead(long handle, long array);

  static native void closeReader(long handle);
}
