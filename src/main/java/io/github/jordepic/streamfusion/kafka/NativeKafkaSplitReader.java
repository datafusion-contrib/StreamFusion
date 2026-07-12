package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.format.NativeBodyBatchDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.TopicPartition;

/**
 * The native side of one Flink subtask's Kafka reading: a single rdkafka consumer (multiplexing all the
 * subtask's partitions) wrapped behind the FLIP-27 {@link SplitReader} contract, so it slots into the
 * standard {@code SingleThreadMultiplexSourceReaderBase} machinery in place of Flink's
 * {@code KafkaPartitionSplitReader}. Splits handed over by the enumerator are assigned+seeked natively
 * ({@code assignKafkaSplits}); each {@link #fetch()} polls one cycle and turns the per-partition binary
 * body batches into per-split records so the reader updates each split's offset state independently.
 *
 * <p>The consumer feeds payloads directly into Arrow binary builders, so no {@code ConsumerRecord} or
 * JVM {@code byte[]} is materialized. When the planner supplies the table's format decoder, the body
 * batches are decoded here — still on this fetch thread, so the format work overlaps the task thread's
 * operators; a CPU profile of the decode-as-operator arrangement showed the decode (65% of the whole
 * job) serialized behind the island on the task thread while this thread idled. The connector stays
 * format-neutral: the decoder arrives through the format-provider SPI, never as a dependency.
 */
final class NativeKafkaSplitReader implements SplitReader<NativeKafkaRecord, KafkaPartitionSplit> {

  private final long handle;
  private final int maxRecords;
  private final long pollTimeoutMillis;
  private final NativeBodyBatchDecoder decoder;
  /** Whether the format's decode is attached natively — polls then emit typed batches directly. */
  private final boolean nativeDecode;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  // Bounded mode: concrete stopping offset per split, the last position seen, and splits already
  // reported finished. A split finishes once its next offset reaches its (concrete) stopping offset.
  private final Map<String, Long> stoppingOffsets = new HashMap<>();
  private final Map<String, Long> positions = new HashMap<>();
  private final Map<String, TopicPartition> partitionsById = new HashMap<>();
  private final Set<String> finished = new HashSet<>();

  NativeKafkaSplitReader(
      String[] configKeys,
      String[] configValues,
      int maxRecords,
      long pollTimeoutMillis,
      NativeMessageDecoderFactory decoderFactory,
      RowType decodedType) {
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.handle = NativeKafka.openKafkaConsumer(configKeys, configValues);
    try {
      this.decoder =
          decoderFactory == null
              ? null
              : new NativeBodyBatchDecoder(decoderFactory, decodedType, allocator);
    } catch (Exception e) {
      NativeKafka.closeKafkaConsumer(handle);
      throw new RuntimeException("failed to open native format decoder", e);
    }
    // A decoder that exposes its library's driver init is attached through the version handshake and
    // decodes inside the poll itself, while the payload bytes are cache-hot. Anything else — including
    // a format artifact whose init refuses this connector's ABI version — keeps the JVM-mediated
    // decode below, so version skew degrades in speed, never in correctness.
    this.nativeDecode = decoder != null && attach(decoder.decoder());
  }

  private boolean attach(NativeMessageDecoder decoder) {
    return decoder.driverInitAddress() != 0
        && decoder.decoderHandle() != 0
        && NativeKafka.attachKafkaDecoder(
            handle, decoder.driverInitAddress(), decoder.decoderHandle());
  }

  @Override
  public RecordsWithSplitIds<NativeKafkaRecord> fetch() {
    int pending = NativeKafka.pollKafkaBatch(handle, maxRecords, pollTimeoutMillis);
    RecordsBySplits.Builder<NativeKafkaRecord> builder = new RecordsBySplits.Builder<>();
    for (int i = 0; i < pending; i++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[3];
        String[] topic = new String[1];
        NativeKafka.drainKafkaSplit(
            handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        String splitId =
            KafkaPartitionSplit.toSplitId(new TopicPartition(topic[0], (int) meta[0]));
        positions.put(splitId, meta[1]);
        ArrowBatch batch = decoded(root);
        // A null batch (every document dropped, e.g. ignore-parse-errors) still carries its offset
        // advance so the split's checkpoint state moves past the consumed records.
        builder.add(splitId, new NativeKafkaRecord(batch, meta[1]));
      }
    }
    // Bounded mode: a split is done once its next offset reaches its stopping offset. (No data exists
    // past a latest-offset stop at run time, so the emitted batches never overshoot it.) Unassign each
    // finished partition natively so the consumer stops fetching/blocking on it (no bounded-tail stall).
    List<TopicPartition> justFinished = new java.util.ArrayList<>();
    for (Map.Entry<String, Long> stop : stoppingOffsets.entrySet()) {
      String splitId = stop.getKey();
      if (!finished.contains(splitId)
          && positions.getOrDefault(splitId, Long.MIN_VALUE) >= stop.getValue()) {
        builder.addFinishedSplit(splitId);
        finished.add(splitId);
        justFinished.add(partitionsById.get(splitId));
      }
    }
    if (!justFinished.isEmpty()) {
      String[] topics = new String[justFinished.size()];
      long[] partitions = new long[justFinished.size()];
      for (int i = 0; i < justFinished.size(); i++) {
        topics[i] = justFinished.get(i).topic();
        partitions[i] = justFinished.get(i).partition();
      }
      NativeKafka.unassignKafkaSplits(handle, topics, partitions);
    }
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
    List<KafkaPartitionSplit> splits = splitsChanges.splits();
    String[] topics = new String[splits.size()];
    long[] partitions = new long[splits.size()];
    long[] offsets = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      KafkaPartitionSplit split = splits.get(i);
      topics[i] = split.getTopic();
      partitions[i] = split.getPartition();
      offsets[i] = split.getStartingOffset();
      partitionsById.put(split.splitId(), split.getTopicPartition());
      split
          .getStoppingOffset()
          .filter(stop -> stop != KafkaPartitionSplit.NO_STOPPING_OFFSET)
          .ifPresent(stop -> stoppingOffsets.put(split.splitId(), stop));
    }
    NativeKafka.assignKafkaSplits(handle, topics, partitions, offsets);
  }

  /** Body batch → typed batch when this reader carries the format decode; null for an empty result. */
  private ArrowBatch decoded(VectorSchemaRoot root) {
    if (nativeDecode) {
      // Already decoded inside the poll; empty (every document dropped) still advances the offset.
      if (root.getRowCount() == 0) {
        root.close();
        return null;
      }
      return new ArrowBatch(root);
    }
    if (decoder == null) {
      return new ArrowBatch(root);
    }
    try {
      VectorSchemaRoot out = decoder.decode(root);
      if (out.getRowCount() == 0) {
        out.close();
        return null;
      }
      return new ArrowBatch(out);
    } catch (Exception e) {
      throw new RuntimeException("native format decode failed", e);
    }
  }

  @Override
  public void wakeUp() {
    // fetch() polls with a bounded timeout and returns promptly, so the fetcher loop is never blocked
    // for long; no interrupt of an in-flight native poll is needed.
  }

  @Override
  public void close() throws Exception {
    NativeKafka.closeKafkaConsumer(handle);
    if (decoder != null) {
      decoder.close();
    }
  }
}
