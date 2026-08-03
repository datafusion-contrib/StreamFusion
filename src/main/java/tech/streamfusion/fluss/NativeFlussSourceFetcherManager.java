package tech.streamfusion.fluss;

import tech.streamfusion.operator.NativeSourceRecord;
import java.util.Map;
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
final class NativeFlussSourceFetcherManager
    extends SingleThreadFetcherManager<NativeSourceRecord, SourceSplitBase> {

  NativeFlussSourceFetcherManager(
      Supplier<SplitReader<NativeSourceRecord, SourceSplitBase>> splitReaderSupplier,
      Configuration configuration) {
    super(splitReaderSupplier, configuration);
  }

  /**
   * Enqueues one fetcher task that asks the split reader which of its splits belong to the removed
   * partitions, unsubscribes them, and acks the unsubscribed buckets to the coordinator — the shape
   * of Fluss's own {@code FlinkSourceFetcherManager.removePartitions}. Runs on the fetcher thread,
   * where the JNI reader is confined; if no fetcher is running, one is created so the (possibly
   * empty) ack still flows.
   */
  void removePartitions(
      Map<Long, String> removedPartitions, Consumer<Set<TableBucket>> unsubscribeCallback) {
    SplitFetcher<NativeSourceRecord, SourceSplitBase> fetcher = getRunningFetcher();
    if (fetcher != null) {
      enqueueRemovePartitionsTask(fetcher, removedPartitions, unsubscribeCallback);
      return;
    }
    fetcher = createSplitFetcher();
    enqueueRemovePartitionsTask(fetcher, removedPartitions, unsubscribeCallback);
    startFetcher(fetcher);
  }

  private static void enqueueRemovePartitionsTask(
      SplitFetcher<NativeSourceRecord, SourceSplitBase> fetcher,
      Map<Long, String> removedPartitions,
      Consumer<Set<TableBucket>> unsubscribeCallback) {
    NativeFlussSplitReader splitReader = (NativeFlussSplitReader) fetcher.getSplitReader();
    fetcher.enqueueTask(
        new SplitFetcherTask() {
          @Override
          public boolean run() {
            unsubscribeCallback.accept(splitReader.removePartitions(removedPartitions));
            return true;
          }

          @Override
          public void wakeUp() {}
        });
  }
}
