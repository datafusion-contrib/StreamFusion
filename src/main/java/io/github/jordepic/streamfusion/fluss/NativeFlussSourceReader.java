package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.metadata.TableBucket;

/** FLIP-27 source reader that swaps Fluss' row split reader for a native Arrow log reader. */
final class NativeFlussSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeFlussRecord, ArrowBatch, SourceSplitBase, SourceSplitState> {

  private final NativeFlussFetcherManager fetcherManager;
  private final Map<String, SourceSplitBase> assignedSplits = new HashMap<>();

  NativeFlussSourceReader(
      Supplier<SplitReader<NativeFlussRecord, SourceSplitBase>> splitReaderSupplier,
      RecordEmitter<NativeFlussRecord, ArrowBatch, SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    this(new NativeFlussFetcherManager(splitReaderSupplier, config), recordEmitter, config, context);
  }

  private NativeFlussSourceReader(
      NativeFlussFetcherManager fetcherManager,
      RecordEmitter<NativeFlussRecord, ArrowBatch, SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    super(fetcherManager, recordEmitter, config, context);
    this.fetcherManager = fetcherManager;
  }

  @Override
  public void addSplits(List<SourceSplitBase> splits) {
    super.addSplits(splits);
    for (SourceSplitBase split : splits) {
      assignedSplits.put(split.splitId(), split);
    }
  }

  @Override
  public void handleSourceEvents(SourceEvent sourceEvent) {
    if (sourceEvent instanceof PartitionsRemovedEvent) {
      handlePartitionsRemoved(((PartitionsRemovedEvent) sourceEvent).getRemovedPartitions());
      return;
    }
    super.handleSourceEvents(sourceEvent);
  }

  @Override
  protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
    for (String splitId : finishedSplitIds.keySet()) {
      assignedSplits.remove(splitId);
    }
  }

  @Override
  protected SourceSplitState initializedState(SourceSplitBase split) {
    if (!split.isLogSplit()) {
      throw new IllegalArgumentException(
          "native Fluss source supports log splits only, got " + split.getClass().getSimpleName());
    }
    return new LogSplitState(split.asLogSplit());
  }

  @Override
  protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
    return splitState.toSourceSplit();
  }

  private void handlePartitionsRemoved(Map<Long, String> removedPartitions) {
    if (removedPartitions.isEmpty()) {
      context.sendSourceEventToCoordinator(new PartitionBucketsUnsubscribedEvent(Set.of()));
      return;
    }

    List<SourceSplitBase> splitsToRemove = new ArrayList<>();
    Set<TableBucket> bucketsToAck = new HashSet<>();
    for (SourceSplitBase split : assignedSplits.values()) {
      TableBucket tableBucket = split.getTableBucket();
      Long partitionId = tableBucket.getPartitionId();
      if (partitionId != null && removedPartitions.containsKey(partitionId)) {
        splitsToRemove.add(split);
        bucketsToAck.add(tableBucket);
      }
    }

    if (splitsToRemove.isEmpty()) {
      context.sendSourceEventToCoordinator(new PartitionBucketsUnsubscribedEvent(Set.of()));
      return;
    }

    for (SourceSplitBase split : splitsToRemove) {
      assignedSplits.remove(split.splitId());
    }
    fetcherManager.removeSplitsAndAck(
        splitsToRemove,
        bucketsToAck,
        buckets ->
            context.sendSourceEventToCoordinator(new PartitionBucketsUnsubscribedEvent(buckets)));
  }
}
