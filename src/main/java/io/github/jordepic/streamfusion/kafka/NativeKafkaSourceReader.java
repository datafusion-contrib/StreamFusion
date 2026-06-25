package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

/**
 * FLIP-27 source reader for the native Kafka source. It reuses the standard single-thread-multiplex
 * machinery — one fetcher thread driving one {@link NativeKafkaSplitReader} (one rdkafka consumer) over
 * the subtask's splits — and emits {@link ArrowBatch}es. Offsets are snapshotted from the split state;
 * unlike Flink's {@code KafkaSourceReader} it does not commit them back to Kafka (correctness rides on
 * Flink's checkpoint, and Kafka-side commit is only external monitoring).
 */
final class NativeKafkaSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeKafkaRecord, ArrowBatch, KafkaPartitionSplit, KafkaPartitionSplitState> {

  NativeKafkaSourceReader(
      Supplier<SplitReader<NativeKafkaRecord, KafkaPartitionSplit>> splitReaderSupplier,
      RecordEmitter<NativeKafkaRecord, ArrowBatch, KafkaPartitionSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    super(splitReaderSupplier, recordEmitter, config, context);
  }

  @Override
  protected void onSplitFinished(Map<String, KafkaPartitionSplitState> finishedSplitIds) {
    // Unbounded source: splits don't finish. Bounded stopping offsets would land here.
  }

  @Override
  protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
    return new KafkaPartitionSplitState(split);
  }

  @Override
  protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
    return splitState.toKafkaPartitionSplit();
  }
}
