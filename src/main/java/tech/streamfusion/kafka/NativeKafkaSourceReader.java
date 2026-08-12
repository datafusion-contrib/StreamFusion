package tech.streamfusion.kafka;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka source reader retaining Flink's split state and checkpoint offset-commit contract. */
final class NativeKafkaSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeSourceRecord, ArrowBatch, KafkaPartitionSplit, KafkaPartitionSplitState> {

  private static final Logger LOG = LoggerFactory.getLogger(NativeKafkaSourceReader.class);

  private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit =
      Collections.synchronizedSortedMap(new TreeMap<>());
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsetsOfFinishedSplits =
      new ConcurrentHashMap<>();
  private final NativeKafkaSourceFetcherManager fetcherManager;
  private final KafkaSourceReaderMetrics metrics;
  private final boolean commitOffsetsOnCheckpoint;

  NativeKafkaSourceReader(
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> splitReaderSupplier,
      RecordEmitter<NativeSourceRecord, ArrowBatch, KafkaPartitionSplitState> emitter,
      Configuration config,
      SourceReaderContext context,
      KafkaSourceReaderMetrics metrics) {
    this(
        new NativeKafkaSourceFetcherManager(splitReaderSupplier, config),
        emitter,
        config,
        context,
        metrics);
  }

  private NativeKafkaSourceReader(
      NativeKafkaSourceFetcherManager fetcherManager,
      RecordEmitter<NativeSourceRecord, ArrowBatch, KafkaPartitionSplitState> emitter,
      Configuration config,
      SourceReaderContext context,
      KafkaSourceReaderMetrics metrics) {
    super(fetcherManager, emitter, config, context);
    this.fetcherManager = fetcherManager;
    this.metrics = metrics;
    this.commitOffsetsOnCheckpoint = config.get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
  }

  @Override
  protected void onSplitFinished(Map<String, KafkaPartitionSplitState> finishedSplits) {
    finishedSplits.values().forEach(
        split -> {
          if (split.getCurrentOffset() >= 0) {
            offsetsOfFinishedSplits.put(
                split.getTopicPartition(), new OffsetAndMetadata(split.getCurrentOffset()));
          }
        });
  }

  @Override
  public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
    List<KafkaPartitionSplit> splits = super.snapshotState(checkpointId);
    if (!commitOffsetsOnCheckpoint) {
      return splits;
    }
    if (splits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
      offsetsToCommit.put(checkpointId, Collections.emptyMap());
      return splits;
    }
    Map<TopicPartition, OffsetAndMetadata> offsets =
        offsetsToCommit.computeIfAbsent(checkpointId, ignored -> new HashMap<>());
    for (KafkaPartitionSplit split : splits) {
      if (split.getStartingOffset() >= 0) {
        offsets.put(
            split.getTopicPartition(), new OffsetAndMetadata(split.getStartingOffset()));
      }
    }
    offsets.putAll(offsetsOfFinishedSplits);
    return splits;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    if (!commitOffsetsOnCheckpoint) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = offsetsToCommit.get(checkpointId);
    if (offsets == null) {
      return;
    }
    if (offsets.isEmpty()) {
      removeOffsetsThrough(checkpointId);
      return;
    }
    fetcherManager.commitOffsets(
        offsets,
        (ignored, error) -> {
          if (error != null) {
            metrics.recordFailedCommit();
            LOG.warn("Failed to commit Kafka offsets for checkpoint {}", checkpointId, error);
            return;
          }
          metrics.recordSucceededCommit();
          offsets.forEach(
              (partition, offset) -> metrics.recordCommittedOffset(partition, offset.offset()));
          offsetsOfFinishedSplits.entrySet().removeIf(e -> offsets.containsKey(e.getKey()));
          removeOffsetsThrough(checkpointId);
        });
  }

  private void removeOffsetsThrough(long checkpointId) {
    while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
      offsetsToCommit.remove(offsetsToCommit.firstKey());
    }
  }

  @Override
  protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
    return new KafkaPartitionSplitState(split);
  }

  @Override
  protected KafkaPartitionSplit toSplitType(
      String splitId, KafkaPartitionSplitState splitState) {
    return splitState.toKafkaPartitionSplit();
  }
}
