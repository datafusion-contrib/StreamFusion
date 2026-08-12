package tech.streamfusion.kafka;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for native Kafka record serialization. Kafka I/O remains in Flink. */
public final class NativeKafka {

  static {
    NativeExtensionLoader.load(NativeKafka.class, "kafka", NativeKafka::nativeBuildVersion);
  }

  private NativeKafka() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Whether the native Kafka serialization extension loaded successfully. */
  public static native boolean isLoaded();

  /** Whether this build encodes the supplied {@code FormatCodes} wire code. */
  public static native boolean encodeFormatSupported(int format);

  /** Spells floating-point values with the native legacy-JDK compatibility implementation. */
  public static native byte[] spellFloatingPoint(double[] doubles, float[] floats);

  /** Serializes one Arrow batch into the final value byte arrays consumed by Flink's KafkaSink. */
  public static native byte[][] encodeKafkaBatch(
      long arrayAddress,
      long schemaAddress,
      int format,
      String formatOptions,
      String[] logicalTypes,
      String[] fieldNames);

  /** Serializes projected key/value bytes; null values represent upsert tombstones. */
  public static native byte[][][] encodeKafkaRecords(
      long arrayAddress,
      long schemaAddress,
      int format,
      String formatOptions,
      int keyFormat,
      String keyFormatOptions,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert);

  /** Serializes projected key/value bytes into two contiguous slabs and row metadata. */
  public static native NativeKafkaEncodedBatch encodeKafkaRecordBatch(
      long arrayAddress,
      long schemaAddress,
      int format,
      String formatOptions,
      int keyFormat,
      String keyFormatOptions,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert);

  /** Creates a task-local encoder plan, parsing formats and projections once at operator open. */
  public static native long createKafkaEncoder(
      int format,
      String formatOptions,
      int keyFormat,
      String keyFormatOptions,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert);

  /** Encodes a batch with a task-local plan created by {@link #createKafkaEncoder}. */
  public static native NativeKafkaEncodedBatch encodeKafkaRecordBatchWithHandle(
      long handle, long arrayAddress, long schemaAddress);

  /** Releases a task-local Kafka encoder plan. */
  public static native void closeKafkaEncoder(long handle);
}
