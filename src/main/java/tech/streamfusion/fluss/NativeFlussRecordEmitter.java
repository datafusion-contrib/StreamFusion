package tech.streamfusion.fluss;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** Emits each Arrow batch downstream and advances the Fluss log split state. */
final class NativeFlussRecordEmitter
    implements RecordEmitter<NativeSourceRecord, ArrowBatch, SourceSplitState> {

  @Override
  public void emitRecord(
      NativeSourceRecord record, SourceOutput<ArrowBatch> output, SourceSplitState splitState) {
    record.emit(output, splitState.asLogSplitState()::setNextOffset);
  }
}
