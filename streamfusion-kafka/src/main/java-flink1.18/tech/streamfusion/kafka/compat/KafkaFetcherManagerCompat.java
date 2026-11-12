package tech.streamfusion.kafka.compat;

import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import tech.streamfusion.operator.NativeSourceRecord;

/** Preserves the released source reader's queue ownership between its fetcher and consumer. */
public abstract class KafkaFetcherManagerCompat
    extends SingleThreadFetcherManager<NativeSourceRecord, KafkaPartitionSplit> {
  final FutureCompletingBlockingQueue<RecordsWithSplitIds<NativeSourceRecord>> queue;

  protected KafkaFetcherManagerCompat(
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> supplier,
      Configuration config) {
    this(new FutureCompletingBlockingQueue<>(), supplier, config);
  }

  private KafkaFetcherManagerCompat(
      FutureCompletingBlockingQueue<RecordsWithSplitIds<NativeSourceRecord>> queue,
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> supplier,
      Configuration config) {
    super(queue, supplier, config);
    this.queue = queue;
  }
}
