package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FLIP-27 source reader for the native Kafka source. It reuses the standard single-thread-multiplex
 * machinery — one fetcher thread driving one {@link NativeKafkaSplitReader} (one rdkafka consumer) over
 * the subtask's splits — and emits {@link ArrowBatch}es. Offsets are snapshotted from split state and,
 * when configured, completed-checkpoint positions are committed to Kafka for external monitoring;
 * recovery correctness continues to ride on Flink's checkpoint.
 */
final class NativeKafkaSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeKafkaRecord, ArrowBatch, KafkaPartitionSplit, KafkaPartitionSplitState> {

  private static final Logger LOG = LoggerFactory.getLogger(NativeKafkaSourceReader.class);
  private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit =
      Collections.synchronizedSortedMap(new TreeMap<>());
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsetsOfFinishedSplits =
      new ConcurrentHashMap<>();
  private final boolean commitOffsetsOnCheckpoint;
  private final NativeKafkaSourceMetrics metrics;

  NativeKafkaSourceReader(
      NativeKafkaSourceFetcherManager fetcherManager,
      RecordEmitter<NativeKafkaRecord, ArrowBatch, KafkaPartitionSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context,
      NativeKafkaSourceMetrics metrics) {
    super(fetcherManager, recordEmitter, config, context);
    this.commitOffsetsOnCheckpoint = config.get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
    this.metrics = metrics;
  }

  @Override
  protected void onSplitFinished(Map<String, KafkaPartitionSplitState> finishedSplitIds) {
    finishedSplitIds.values().forEach(
        state -> {
          if (state.getCurrentOffset() >= 0) {
            offsetsOfFinishedSplits.put(
                state.getTopicPartition(), new OffsetAndMetadata(state.getCurrentOffset()));
          }
        });
  }

  @Override
  public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
    List<KafkaPartitionSplit> splits = super.snapshotState(checkpointId);
    if (!commitOffsetsOnCheckpoint) {
      return splits;
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (KafkaPartitionSplit split : splits) {
      if (split.getStartingOffset() >= 0) {
        offsets.put(
            split.getTopicPartition(), new OffsetAndMetadata(split.getStartingOffset()));
      }
    }
    offsets.putAll(offsetsOfFinishedSplits);
    offsetsToCommit.put(checkpointId, offsets);
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
    if (!offsets.isEmpty()) {
      ((NativeKafkaSourceFetcherManager) splitFetcherManager)
          .commitOffsets(
              offsets,
              error -> {
                if (error == null) {
                  offsets.forEach(
                      (partition, offset) -> metrics.recordCommit(partition, offset.offset()));
                  metrics.recordSucceededCommit();
                  offsetsOfFinishedSplits.keySet().removeAll(offsets.keySet());
                  removeOffsetsThrough(checkpointId);
                } else {
                  metrics.recordFailedCommit();
                  LOG.warn(
                      "Failed to commit consumer offsets for checkpoint {}", checkpointId, error);
                }
              });
    } else {
      removeOffsetsThrough(checkpointId);
    }
  }

  private void removeOffsetsThrough(long checkpointId) {
    synchronized (offsetsToCommit) {
      while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
        offsetsToCommit.remove(offsetsToCommit.firstKey());
      }
    }
  }

  @Override
  protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
    return new KafkaPartitionSplitState(split);
  }

  @Override
  protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
    return splitState.toKafkaPartitionSplit();
  }
}
