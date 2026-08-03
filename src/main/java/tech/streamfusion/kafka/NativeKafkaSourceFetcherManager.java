package tech.streamfusion.kafka;

import tech.streamfusion.operator.NativeSourceRecord;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Runs Kafka consumer operations on the single fetcher thread that owns the native handle. */
final class NativeKafkaSourceFetcherManager
    extends SingleThreadFetcherManager<NativeSourceRecord, KafkaPartitionSplit> {

  NativeKafkaSourceFetcherManager(
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> splitReaderSupplier,
      Configuration configuration) {
    super(splitReaderSupplier, configuration);
  }

  void commitOffsets(
      Map<TopicPartition, OffsetAndMetadata> offsets, Consumer<Exception> completion) {
    if (offsets.isEmpty()) {
      return;
    }
    SplitFetcher<NativeSourceRecord, KafkaPartitionSplit> fetcher = getRunningFetcher();
    if (fetcher != null) {
      enqueueCommitTask(fetcher, offsets, completion);
      return;
    }
    fetcher = createSplitFetcher();
    enqueueCommitTask(fetcher, offsets, completion);
    startFetcher(fetcher);
  }

  private static void enqueueCommitTask(
      SplitFetcher<NativeSourceRecord, KafkaPartitionSplit> fetcher,
      Map<TopicPartition, OffsetAndMetadata> offsets,
      Consumer<Exception> completion) {
    NativeKafkaSplitReader splitReader = (NativeKafkaSplitReader) fetcher.getSplitReader();
    fetcher.enqueueTask(
        new SplitFetcherTask() {
          @Override
          public boolean run() {
            try {
              splitReader.commitOffsets(offsets);
              completion.accept(null);
            } catch (IOException error) {
              completion.accept(error);
            }
            return true;
          }

          @Override
          public void wakeUp() {}
        });
  }
}
