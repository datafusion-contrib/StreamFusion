package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.format.NativeBodyBatchDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.BoundedSplitTracker;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
import io.github.jordepic.streamfusion.operator.NativeSourceRecord;
import io.github.jordepic.streamfusion.operator.NativeSourceWatermarks;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
final class NativeKafkaSplitReader implements SplitReader<NativeSourceRecord, KafkaPartitionSplit> {

  private final long handle;
  private final int maxRecords;
  private final long pollTimeoutMillis;
  // Rowtime column in the decoded batch for a watermarked table, or -1: each emitted batch carries
  // its max rowtime as the record timestamp, feeding the source operator's per-split watermarks.
  private final int rowtimeIndex;
  private final NativeBodyBatchDecoder decoder;
  private final NativeKafkaSourceMetrics metrics;
  /** Whether the format's decode is attached natively — polls then emit typed batches directly. */
  private final boolean nativeDecode;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  // Bounded mode: a split finishes once its next offset reaches its (concrete) stopping offset.
  private final BoundedSplitTracker<TopicPartition> tracker = new BoundedSplitTracker<>();

  NativeKafkaSplitReader(
      String[] configKeys,
      String[] configValues,
      int maxRecords,
      long pollTimeoutMillis,
      NativeMessageDecoderFactory decoderFactory,
      RowType decodedType,
      int rowtimeIndex,
      NativeKafkaSourceMetrics metrics) {
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.rowtimeIndex = rowtimeIndex;
    this.metrics = metrics;
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
  public RecordsWithSplitIds<NativeSourceRecord> fetch() {
    Set<String> pendingFinished = tracker.drainPendingFinished();
    if (!pendingFinished.isEmpty()) {
      List<TopicPartition> partitions = pendingFinished.stream().map(tracker::tracked).toList();
      NativeKafka.unassignKafkaSplits(
          handle,
          partitions.stream().map(TopicPartition::topic).toArray(String[]::new),
          partitions.stream().mapToLong(TopicPartition::partition).toArray());
      RecordsBySplits.Builder<NativeSourceRecord> builder = new RecordsBySplits.Builder<>();
      builder.addFinishedSplits(pendingFinished);
      return builder.build();
    }
    int pending = NativeKafka.pollKafkaBatch(handle, maxRecords, pollTimeoutMillis);
    metrics.recordTransientErrors(NativeKafka.kafkaConsumerTransientErrors(handle));
    RecordsBySplits.Builder<NativeSourceRecord> builder = new RecordsBySplits.Builder<>();
    for (int i = 0; i < pending; i++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[5];
        String[] topic = new String[1];
        NativeKafka.drainKafkaSplit(
            handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        String splitId =
            KafkaPartitionSplit.toSplitId(new TopicPartition(topic[0], (int) meta[0]));
        metrics.recordPoll(
            new TopicPartition(topic[0], (int) meta[0]), meta[1], meta[2], meta[3], meta[4]);
        tracker.recordPosition(splitId, meta[1]);
        VectorSchemaRoot typed = decoded(root);
        long maxRowtime =
            typed == null || rowtimeIndex < 0
                ? Long.MIN_VALUE
                : NativeSourceWatermarks.maxRowtimeMillis(typed, rowtimeIndex);
        // A null batch (every document dropped, e.g. ignore-parse-errors) still carries its offset
        // advance so the split's checkpoint state moves past the consumed records.
        builder.add(
            splitId,
            new NativeSourceRecord(
                typed == null ? null : new ArrowBatch(typed), meta[1], maxRowtime));
      }
    }
    // Native polling caps every batch at the split's stopping offset and reports the consumer's actual
    // position, including progress over Kafka control records. Unassign each completed partition so it
    // cannot fetch beyond the bounded snapshot while the task thread drains this result.
    List<TopicPartition> justFinished = tracker.finishReached(builder::addFinishedSplit);
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
    long[] stops = new long[splits.size()];
    Arrays.fill(stops, KafkaPartitionSplit.NO_STOPPING_OFFSET);
    List<KafkaPartitionSplit> assigned = new ArrayList<>(splits.size());
    for (int i = 0; i < splits.size(); i++) {
      KafkaPartitionSplit split = splits.get(i);
      long stop = split.getStoppingOffset().orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET);
      if (stop >= 0 && split.getStartingOffset() >= stop) {
        tracker.markPendingFinished(split.splitId());
      }
      int assignedIndex = assigned.size();
      topics[assignedIndex] = split.getTopic();
      partitions[assignedIndex] = split.getPartition();
      offsets[assignedIndex] = split.getStartingOffset();
      stops[assignedIndex] = stop;
      assigned.add(split);
      tracker.track(
          split.splitId(),
          split.getTopicPartition(),
          split.getStartingOffset(),
          stop == KafkaPartitionSplit.NO_STOPPING_OFFSET
              ? OptionalLong.empty()
              : OptionalLong.of(stop));
      metrics.register(split.getTopicPartition());
    }
    if (!assigned.isEmpty()) {
      int count = assigned.size();
      NativeKafka.assignKafkaSplits(
          handle,
          Arrays.copyOf(topics, count),
          Arrays.copyOf(partitions, count),
          Arrays.copyOf(offsets, count),
          Arrays.copyOf(stops, count));
    }
  }

  /** Body batch → typed root when this reader carries the format decode; null for an empty result. */
  private VectorSchemaRoot decoded(VectorSchemaRoot root) {
    if (nativeDecode) {
      // Already decoded inside the poll; empty (every document dropped) still advances the offset.
      if (root.getRowCount() == 0) {
        root.close();
        return null;
      }
      return root;
    }
    if (decoder == null) {
      return root;
    }
    try {
      VectorSchemaRoot out = decoder.decode(root);
      if (out.getRowCount() == 0) {
        out.close();
        return null;
      }
      return out;
    } catch (Exception e) {
      throw new RuntimeException("native format decode failed", e);
    }
  }

  @Override
  public void wakeUp() {
    NativeKafka.wakeKafkaConsumer(handle);
  }

  /** Commits completed-checkpoint positions from the fetcher thread that owns this consumer. */
  void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) throws IOException {
    String[] topics = new String[offsets.size()];
    long[] partitions = new long[offsets.size()];
    long[] positions = new long[offsets.size()];
    int index = 0;
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
      topics[index] = entry.getKey().topic();
      partitions[index] = entry.getKey().partition();
      positions[index] = entry.getValue().offset();
      index++;
    }
    NativeKafka.commitKafkaOffsets(handle, topics, partitions, positions);
  }

  @Override
  public void pauseOrResumeSplits(
      Collection<KafkaPartitionSplit> splitsToPause,
      Collection<KafkaPartitionSplit> splitsToResume) {
    setPaused(splitsToPause, true);
    setPaused(splitsToResume, false);
  }

  private void setPaused(Collection<KafkaPartitionSplit> splits, boolean paused) {
    if (splits.isEmpty()) {
      return;
    }
    String[] topics = new String[splits.size()];
    long[] partitions = new long[splits.size()];
    int index = 0;
    for (KafkaPartitionSplit split : splits) {
      topics[index] = split.getTopic();
      partitions[index] = split.getPartition();
      index++;
    }
    try {
      NativeKafka.setKafkaSplitsPaused(handle, topics, partitions, paused);
    } catch (IOException error) {
      throw new UncheckedIOException(error);
    }
  }

  @Override
  public void close() throws Exception {
    NativeKafka.closeKafkaConsumer(handle);
    if (decoder != null) {
      decoder.close();
    }
  }
}
