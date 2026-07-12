package io.github.jordepic.streamfusion.format.json;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native JSON and JSON-CDC format implementation. */
public final class NativeJsonFormat {

  static {
    NativeExtensionLoader.load(NativeJsonFormat.class, "json");
  }

  private NativeJsonFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  /**
   * Address of this library's exported driver init ({@code streamfusion_format_driver_init}): a
   * connector calls it with the ABI version it speaks and the format fills the decode vtable or
   * refuses (the ADBC driver-init pattern).
   */
  public static native long driverInitAddress();

  static native long createDecoder(
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      boolean skipParseErrors,
      String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
