package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** Emits native Fluss Arrow batches and advances the Fluss log split state. */
final class NativeFlussRecordEmitter
    implements RecordEmitter<NativeFlussRecord, ArrowBatch, SourceSplitState> {

  @Override
  public void emitRecord(
      NativeFlussRecord record, SourceOutput<ArrowBatch> output, SourceSplitState splitState) {
    output.collect(record.batch());
    splitState.asLogSplitState().setNextOffset(record.nextOffset());
  }
}
