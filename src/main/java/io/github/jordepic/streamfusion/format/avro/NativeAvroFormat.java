package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Avro format implementation. */
public final class NativeAvroFormat {

  static {
    NativeExtensionLoader.load(
        NativeAvroFormat.class, "avro", NativeAvroFormat::nativeBuildVersion);
  }

  private NativeAvroFormat() {}

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

  /**
   * {@code schemaArrayAddress}/{@code schemaAddress} export the boundary Arrow schema (an empty
   * batch of the output row type); the native decode reconciles each arrow-avro batch onto it.
   */
  public static native long createDecoder(
      boolean confluent,
      String writerSchema,
      String readerSchema,
      long schemaArrayAddress,
      long schemaAddress);

  /**
   * The {@code debezium-avro-confluent} decoder: a registry-driven envelope decode against
   * {@code readerSchema} (the Debezium envelope derivation), fanned out to changelog rows on the
   * exported physical boundary schema plus {@code $row_kind$}.
   */
  public static native long createDebeziumDecoder(
      String readerSchema, long schemaArrayAddress, long schemaAddress);

  public static native void registerWriterSchema(long handle, int schemaId, String schema);

  public static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void closeDecoder(long handle);
}
