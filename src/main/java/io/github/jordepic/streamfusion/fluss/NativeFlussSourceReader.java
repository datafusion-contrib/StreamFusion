package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** FLIP-27 source reader that swaps Fluss' row split reader for a native Arrow log reader. */
final class NativeFlussSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeFlussRecord, ArrowBatch, SourceSplitBase, SourceSplitState> {

  NativeFlussSourceReader(
      Supplier<SplitReader<NativeFlussRecord, SourceSplitBase>> splitReaderSupplier,
      RecordEmitter<NativeFlussRecord, ArrowBatch, SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    super(splitReaderSupplier, recordEmitter, config, context);
  }

  @Override
  protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
    // Fluss' enumerator owns split completion bookkeeping.
  }

  @Override
  protected SourceSplitState initializedState(SourceSplitBase split) {
    if (!split.isLogSplit()) {
      throw new IllegalArgumentException(
          "native Fluss source supports log splits only, got " + split.getClass().getSimpleName());
    }
    return new LogSplitState(split.asLogSplit());
  }

  @Override
  protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
    return splitState.toSourceSplit();
  }
}
