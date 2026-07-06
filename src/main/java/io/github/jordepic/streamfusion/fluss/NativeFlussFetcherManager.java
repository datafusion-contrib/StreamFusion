package io.github.jordepic.streamfusion.fluss;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
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

    Set<TableBucket> bucketsToAck = Set.copyOf(removedBuckets);
    fetcher.removeSplits(splits);
    fetcher.enqueueTask(new PartitionRemovalAckTask(bucketsToAck, unsubscribeCallback));
  }

  private final class PartitionRemovalAckTask implements SplitFetcherTask {
    private final Set<TableBucket> removedBuckets;
    private final Consumer<Set<TableBucket>> unsubscribeCallback;

    private PartitionRemovalAckTask(
        Set<TableBucket> removedBuckets,
        Consumer<Set<TableBucket>> unsubscribeCallback) {
      this.removedBuckets = removedBuckets;
      this.unsubscribeCallback = unsubscribeCallback;
    }

    @Override
    public boolean run() {
      unsubscribeCallback.accept(removedBuckets);
      return true;
    }

    @Override
    public void wakeUp() {}
  }
}
