package tech.streamfusion.kafka;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

/** Emits one partition-local Arrow batch and advances that partition's checkpoint offset. */
final class NativeKafkaRecordEmitter
    implements RecordEmitter<NativeSourceRecord, ArrowBatch, KafkaPartitionSplitState> {

  @Override
  public void emitRecord(
      NativeSourceRecord record,
      SourceOutput<ArrowBatch> output,
      KafkaPartitionSplitState splitState) {
    record.emit(output, splitState::setCurrentOffset);
  }
}
