package tech.streamfusion.kafka;

import tech.streamfusion.operator.NativeSourceRecord;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

/** Single-thread native Kafka fetcher manager with Flink's checkpoint-offset commit hook. */
final class NativeKafkaSourceFetcherManager
    extends SingleThreadFetcherManager<NativeSourceRecord, KafkaPartitionSplit> {

  NativeKafkaSourceFetcherManager(
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> splitReaderSupplier,
      Configuration configuration) {
    super(splitReaderSupplier, configuration);
  }

  void commitOffsets(
      Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    if (offsets.isEmpty()) {
      return;
    }
    SplitFetcher<NativeSourceRecord, KafkaPartitionSplit> fetcher = getRunningFetcher();
    if (fetcher == null) {
      fetcher = createSplitFetcher();
      enqueueCommit(fetcher, offsets, callback);
      startFetcher(fetcher);
    } else {
      enqueueCommit(fetcher, offsets, callback);
    }
  }

  private static void enqueueCommit(
      SplitFetcher<NativeSourceRecord, KafkaPartitionSplit> fetcher,
      Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    NativeKafkaSplitReader reader = (NativeKafkaSplitReader) fetcher.getSplitReader();
    fetcher.enqueueTask(
        new SplitFetcherTask() {
          @Override
          public boolean run() throws IOException {
            reader.commitOffsets(offsets, callback);
            return true;
          }

          @Override
          public void wakeUp() {}
        });
  }
}
