package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.NativeExtensionLoader;
import java.io.IOException;

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
  public static native boolean attachKafkaDecoder(
      long handle, long initAddress, long decoderHandle);

  public static native void assignKafkaSplits(
      long handle, String[] topics, long[] partitions, long[] startOffsets, long[] stoppingOffsets);

  public static native void unassignKafkaSplits(long handle, String[] topics, long[] partitions);

  public static native void commitKafkaOffsets(
      long handle, String[] topics, long[] partitions, long[] offsets) throws IOException;

  public static native void setKafkaSplitsPaused(
      long handle, String[] topics, long[] partitions, boolean paused) throws IOException;

  public static native int pollKafkaBatch(long handle, int maxRecords, long timeoutMillis);

  public static native void wakeKafkaConsumer(long handle);

  /** Serializes one Arrow batch directly into the final heap byte arrays KafkaProducer requires. */
  public static native byte[][] encodeKafkaJsonBatch(
      long arrayAddress,
      long schemaAddress,
      boolean ignoreNullFields,
      String timestampFormat,
      String[] logicalTypes,
      String[] fieldNames);

  /** Serializes projected key/value bytes together; null values are upsert tombstones. */
  public static native byte[][][] encodeKafkaJsonRecords(
      long arrayAddress,
      long schemaAddress,
      boolean ignoreNullFields,
      String timestampFormat,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert);

  public static native int drainKafkaSplit(
      long handle,
      long[] splitMeta,
      String[] outTopic,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void closeKafkaConsumer(long handle);

  /**
   * Opens a native transactional producer (the exactly-once hand-off data plane: it initializes,
   * begins, produces, and flushes a Kafka transaction, but never commits — the Java committer
   * finishes the transaction by presenting the identity this producer reports). The librdkafka
   * config is applied verbatim and must include {@code transactional.id} and {@code
   * statistics.interval.ms} (the only channel librdkafka exposes the producer id/epoch through).
   */
  public static native long openTransactionalKafkaProducer(
      int configVersion,
      String[] configKeys,
      String[] configValues,
      String transactionalId,
      long maxBlockMillis,
      int maxRequestSize);

  /**
   * Runs init_transactions and writes the broker-assigned {@code [producerId, epoch]} into {@code
   * outIdentity}.
   */
  public static native void initKafkaTransactions(
      long handle, long timeoutMillis, long[] outIdentity);

  public static native void beginKafkaTransaction(long handle);

  /** Produces one record into the open transaction; a null key produces an unkeyed record. */
  public static native void produceKafkaRecord(long handle, String topic, byte[] key, byte[] value);

  /**
   * Encodes one Arrow batch and enqueues all of its Kafka records without materializing JVM rows.
   */
  public static native long produceKafkaJsonBatch(
      long handle,
      String topic,
      long arrayAddress,
      long schemaAddress,
      boolean ignoreNullFields,
      String timestampFormat,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert);

  /**
   * Flushes the open transaction's records and writes the {@code [producerId, epoch]} it runs under
   * into {@code outIdentity}. Afterwards the transaction is fully materialized on the broker (still
   * ongoing) and survives {@link #closeKafkaProducer}.
   */
  public static native void flushKafkaProducer(long handle, long timeoutMillis, long[] outIdentity);

  public static native void abortKafkaTransaction(long handle, long timeoutMillis);

  /**
   * Destroys the producer without committing or aborting: an open flushed transaction stays ongoing
   * on the broker for the Java committer to finish.
   */
  public static native void closeKafkaProducer(long handle);

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
