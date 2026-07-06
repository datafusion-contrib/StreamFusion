package io.github.jordepic.streamfusion.fluss;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

/** Single-thread Fluss fetcher manager with the partition-removal ACK hook Fluss expects. */
final class NativeFlussFetcherManager
    extends SingleThreadFetcherManager<NativeFlussRecord, SourceSplitBase> {

  NativeFlussFetcherManager(
      Supplier<SplitReader<NativeFlussRecord, SourceSplitBase>> splitReaderSupplier,
      Configuration config) {
    super(splitReaderSupplier, config);
  }

  void removeSplitsAndAck(
      List<SourceSplitBase> splits,
      Set<TableBucket> removedBuckets,
      Consumer<Set<TableBucket>> unsubscribeCallback) {
    SplitFetcher<NativeFlussRecord, SourceSplitBase> fetcher = getRunningFetcher();
    if (fetcher == null) {
      unsubscribeCallback.accept(removedBuckets);
      return;
    }

    List<String> splitIds = splits.stream().map(SourceSplitBase::splitId).toList();
    Set<TableBucket> bucketsToAck = Set.copyOf(removedBuckets);
    fetcher.removeSplits(splits);
    fetcher.enqueueTask(
        new PartitionRemovalAckTask(
            fetcher.fetcherId(), splitIds, bucketsToAck, unsubscribeCallback));
  }

  private final class PartitionRemovalAckTask implements SplitFetcherTask {
    private final int fetcherId;
    private final Collection<String> finishedSplitIds;
    private final Set<TableBucket> removedBuckets;
    private final Consumer<Set<TableBucket>> unsubscribeCallback;

    private PartitionRemovalAckTask(
        int fetcherId,
        Collection<String> finishedSplitIds,
        Set<TableBucket> removedBuckets,
        Consumer<Set<TableBucket>> unsubscribeCallback) {
      this.fetcherId = fetcherId;
      this.finishedSplitIds = finishedSplitIds;
      this.removedBuckets = removedBuckets;
      this.unsubscribeCallback = unsubscribeCallback;
    }

    @Override
    public boolean run() throws IOException {
      RecordsBySplits.Builder<NativeFlussRecord> builder = new RecordsBySplits.Builder<>();
      builder.addFinishedSplits(finishedSplitIds);
      try {
        getQueue().put(fetcherId, builder.build());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while finishing removed Fluss partition splits", e);
      }
      unsubscribeCallback.accept(removedBuckets);
      return true;
    }

    @Override
    public void wakeUp() {
      getQueue().wakeUpPuttingThread(fetcherId);
    }
  }
}
