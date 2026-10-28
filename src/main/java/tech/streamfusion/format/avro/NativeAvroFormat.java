package tech.streamfusion.format.avro;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Avro format implementation. */
public final class NativeAvroFormat {

  static {
    NativeExtensionLoader.load(
        NativeAvroFormat.class, "avro", NativeAvroFormat::nativeBuildVersion, NativeAvroFormat::liveNativeHandles);
  }

  private NativeAvroFormat() {}

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

  /**
   * {@code schemaArrayAddress}/{@code schemaAddress} export the boundary Arrow schema (an empty
   * batch of the output row type); the native decode reconciles each arrow-avro batch onto it.
   * {@code formatOptions} are the table's encoded option lines: the connector metadata columns and
   * a keyed table's key composition.
   */
  public static native long createDecoder(
      boolean confluent,
      String writerSchema,
      String readerSchema,
      long schemaArrayAddress,
      long schemaAddress,
      String formatOptions);

  /**
   * The {@code debezium-avro-confluent} decoder: a registry-driven envelope decode against
   * {@code readerSchema} (the Debezium envelope derivation), fanned out to changelog rows on the
   * exported physical boundary schema plus {@code $row_kind$}.
   */
  public static native long createDebeziumDecoder(
      String readerSchema, long schemaArrayAddress, long schemaAddress, String formatOptions);

  public static native void registerWriterSchema(long handle, int schemaId, String schema);

  /** Registers a writer schema with a keyed table's Confluent-framed key decoder. */
  public static native void registerKeyWriterSchema(long handle, int schemaId, String schema);

  public static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  /** Decodes Kafka records with their connector metadata; see {@code NativeMessageDecoder}. */
  public static native void decodeRecordsInto(
      long handle,
      long slabAddress,
      long slabLength,
      long[] layout,
      int count,
      String topic,
      int partition,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void closeDecoder(long handle);
}
