package io.github.jordepic.streamfusion.format.raw;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native raw format implementation. */
public final class NativeRawFormat {

  static {
    NativeExtensionLoader.load(NativeRawFormat.class, "raw", NativeRawFormat::nativeBuildVersion);
  }

  private NativeRawFormat() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  /**
   * Address of this library's exported driver init ({@code streamfusion_format_driver_init}): a
   * connector calls it with the ABI version it speaks and the format fills the decode vtable or
   * refuses (the ADBC driver-init pattern).
   */
  public static native long driverInitAddress();

  static native long createDecoder(
      long schemaArrayAddress, long schemaAddress, String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
