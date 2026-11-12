package tech.streamfusion.kafka.compat;

import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import tech.streamfusion.operator.NativeSourceRecord;

/** Preserves the released source reader's queue ownership between its fetcher and consumer. */
public abstract class KafkaFetcherManagerCompat
    extends SingleThreadFetcherManager<NativeSourceRecord, KafkaPartitionSplit> {
  protected KafkaFetcherManagerCompat(
      Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> supplier,
      Configuration config) {
    super(supplier, config);
  }
}
