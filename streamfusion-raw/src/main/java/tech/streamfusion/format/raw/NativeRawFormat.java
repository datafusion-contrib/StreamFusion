package tech.streamfusion.format.raw;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native raw format implementation. */
public final class NativeRawFormat {

  static {
    NativeExtensionLoader.load(
        NativeRawFormat.class,
        "raw",
        NativeRawFormat::nativeBuildVersion,
        NativeRawFormat::liveNativeHandles);
  }

  private NativeRawFormat() {}

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
      long schemaArrayAddress, long schemaAddress, String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);

  public static native boolean encoderSupported(int format);

  public static native byte[] spellFloatingPoint(double[] doubles, float[] floats);

  private static native long createEncoder(
      int format, String options, String[] types, String[] names, int[] fields);

  private static native tech.streamfusion.format.EncodedMessages encodeBatch(
      long handle, long array, long schema);

  private static native void closeEncoder(long handle);

  public static tech.streamfusion.format.NativeMessageEncoder encoder(
      int format, String options, String[] types, String[] names, int[] fields) {
    return tech.streamfusion.format.NativeHandleMessageEncoder.open(
        () -> createEncoder(format, options, types, names, fields),
        NativeRawFormat::encodeBatch,
        NativeRawFormat::closeEncoder);
  }
}
