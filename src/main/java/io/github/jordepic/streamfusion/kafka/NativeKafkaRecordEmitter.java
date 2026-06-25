package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

/**
 * Emits each decoded Arrow batch downstream and advances its split's checkpoint offset. The offset
 * lives in the split state (snapshotted by the source reader), not committed to Kafka — exactly-once is
 * Flink's checkpoint, with Kafka commits only optional external monitoring (not done here).
 */
final class NativeKafkaRecordEmitter
    implements RecordEmitter<NativeKafkaRecord, ArrowBatch, KafkaPartitionSplitState> {

  @Override
  public void emitRecord(
      NativeKafkaRecord record, SourceOutput<ArrowBatch> output, KafkaPartitionSplitState splitState) {
    output.collect(record.batch());
    splitState.setCurrentOffset(record.nextOffset());
  }
}
