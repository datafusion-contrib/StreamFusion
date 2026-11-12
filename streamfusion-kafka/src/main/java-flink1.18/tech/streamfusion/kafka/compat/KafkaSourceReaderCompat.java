package tech.streamfusion.kafka.compat;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;

/** Supplies the same fetcher-owned queue to the older reader constructor. */
public abstract class KafkaSourceReaderCompat
    extends SingleThreadMultiplexSourceReaderBase<
        NativeSourceRecord, ArrowBatch, KafkaPartitionSplit, KafkaPartitionSplitState> {
  protected KafkaSourceReaderCompat(
      KafkaFetcherManagerCompat fetcher,
      RecordEmitter<NativeSourceRecord, ArrowBatch, KafkaPartitionSplitState> emitter,
      Configuration config,
      SourceReaderContext context) {
    super(fetcher.queue, fetcher, emitter, config, context);
  }
}
