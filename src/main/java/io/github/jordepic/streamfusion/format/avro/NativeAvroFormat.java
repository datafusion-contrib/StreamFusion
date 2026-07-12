package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Avro format implementation. */
public final class NativeAvroFormat {

  static {
    NativeExtensionLoader.load(NativeAvroFormat.class, "avro");
  }

  private NativeAvroFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  /**
   * Address of this library's exported driver init ({@code streamfusion_format_driver_init}): a
   * connector calls it with the ABI version it speaks and the format fills the decode vtable or
   * refuses (the ADBC driver-init pattern).
   */
  public static native long driverInitAddress();

  public static native long createDecoder(boolean confluent, String writerSchema, String readerSchema);

  public static native void registerWriterSchema(long handle, int schemaId, String schema);

  public static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void closeDecoder(long handle);
}
