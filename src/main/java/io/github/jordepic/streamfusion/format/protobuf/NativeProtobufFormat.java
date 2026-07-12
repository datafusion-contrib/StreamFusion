package io.github.jordepic.streamfusion.format.protobuf;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native protobuf format implementation. */
public final class NativeProtobufFormat {

  static {
    NativeExtensionLoader.load(NativeProtobufFormat.class, "protobuf");
  }

  private NativeProtobufFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  /**
   * Address of this library's exported driver init ({@code streamfusion_format_driver_init}): a
   * connector calls it with the ABI version it speaks and the format fills the decode vtable or
   * refuses (the ADBC driver-init pattern).
   */
  public static native long driverInitAddress();

  static native long createDecoder(
      byte[] descriptor, String messageName, long schemaArrayAddress, long schemaAddress);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
