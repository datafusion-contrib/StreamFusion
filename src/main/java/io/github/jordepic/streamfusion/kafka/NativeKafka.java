package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Kafka source. */
public final class NativeKafka {

  static {
    NativeExtensionLoader.load(NativeKafka.class, "kafka");
  }

  private NativeKafka() {}

  public static native boolean featureBuilt();

  public static native long openKafkaConsumer(String[] configKeys, String[] configValues);

  /**
   * Attaches a format library's decode to a consumer through the driver-init handshake: the init at
   * {@code initAddress} is called with the ABI version this connector speaks and fills the decode
   * vtable, or refuses — in which case this returns false and the caller keeps the JVM-mediated
   * decode. Polls of an attached consumer emit typed batches, decoded on the fetch thread with no
   * JVM round trip. The decoder handle's Java owner must outlive the consumer.
   */
  public static native boolean attachKafkaDecoder(long handle, long initAddress, long decoderHandle);

  public static native void assignKafkaSplits(
      long handle,
      String[] topics,
      long[] partitions,
      long[] startOffsets,
      long[] stoppingOffsets);

  public static native void unassignKafkaSplits(long handle, String[] topics, long[] partitions);

  public static native int pollKafkaBatch(long handle, int maxRecords, long timeoutMillis);

  public static native int drainKafkaSplit(
      long handle, long[] splitMeta, String[] outTopic, long outArrayAddress, long outSchemaAddress);

  public static native void closeKafkaConsumer(long handle);

  public static native long benchmarkKafkaConsume(
      String brokers, String topic, long schemaArrayAddress, long schemaAddress, long maxMessages);

  public static native long benchmarkNativeConsume(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  public static native long benchmarkNativeConsumeSerial(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  public static native long benchmarkConsumeOnly(
      String[] configKeys, String[] configValues, String topic, long maxMessages);
}
