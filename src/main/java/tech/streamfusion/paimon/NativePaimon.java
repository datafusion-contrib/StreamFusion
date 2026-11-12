package tech.streamfusion.paimon;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry points owned by the optional Paimon native library. */
public final class NativePaimon {
  static {
    NativeExtensionLoader.load(
        NativePaimon.class,
        "paimon",
        NativePaimon::nativeBuildVersion,
        NativePaimon::liveNativeHandles);
  }

  private NativePaimon() {}

  private static native String nativeBuildVersion();

  public static native String liveNativeHandles();

  static native long createReader(
      Object io, String descriptor, long schema, int rows, long budget, boolean kinds, Object memory);

  static native boolean readerNext(long handle, long array, long schema);

  static native long readerReservedBytes(long handle);

  static native void closeReader(long handle);
}
