package tech.streamfusion.fluss;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
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

/** FLIP-27 source reader that swaps Fluss' row split reader for a native Arrow log reader. */
final class NativeFlussSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeSourceRecord, ArrowBatch, SourceSplitBase, SourceSplitState> {

  private final NativeFlussSourceFetcherManager fetcherManager;

  NativeFlussSourceReader(
      Supplier<SplitReader<NativeSourceRecord, SourceSplitBase>> splitReaderSupplier,
      RecordEmitter<NativeSourceRecord, ArrowBatch, SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    this(new NativeFlussSourceFetcherManager(splitReaderSupplier, config), recordEmitter, config, context);
  }

  private NativeFlussSourceReader(
      NativeFlussSourceFetcherManager fetcherManager,
      RecordEmitter<NativeSourceRecord, ArrowBatch, SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    super(fetcherManager, recordEmitter, config, context);
    this.fetcherManager = fetcherManager;
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
    // Split-state cleanup rides the finished-splits report from the fetch path; nothing to do here.
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
    // The split reader answers which of its splits the partitions own (Fluss's own shape) — no
    // split bookkeeping is duplicated here.
    fetcherManager.removePartitions(
        removedPartitions,
        buckets ->
            context.sendSourceEventToCoordinator(new PartitionBucketsUnsubscribedEvent(buckets)));
  }
}
