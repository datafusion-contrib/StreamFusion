package tech.streamfusion.format.csv;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native CSV format implementation. */
public final class NativeCsvFormat {

  static {
    NativeExtensionLoader.load(NativeCsvFormat.class, "csv", NativeCsvFormat::nativeBuildVersion, NativeCsvFormat::liveNativeHandles);
  }

  private NativeCsvFormat() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Live handles owned by this library, used by the shared leak sentinel. */
  public static native String liveNativeHandles();

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  /**
   * Address of this library's exported driver init ({@code streamfusion_format_driver_init}): a
   * connector calls it with the ABI version it speaks and the format fills the decode vtable or
   * refuses (the ADBC driver-init pattern).
   */
  public static native long driverInitAddress();

  static native long createDecoder(
      long schemaArrayAddress, long schemaAddress, boolean skipParseErrors, String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
