package tech.streamfusion.paimon;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import tech.streamfusion.operator.ArrowBatch;

/** Flushes a finite file's final watermark before Flink removes its periodic generator. */
final class PaimonSplitWatermarks implements ReaderOutput<ArrowBatch> {
  private final ReaderOutput<ArrowBatch> delegate;
  private final Map<String, SplitOutput> splits = new HashMap<>();

  PaimonSplitWatermarks(ReaderOutput<ArrowBatch> delegate) {
    this.delegate = delegate;
  }

  @Override
  public SourceOutput<ArrowBatch> createOutputForSplit(String splitId) {
    return splits.computeIfAbsent(
        splitId, id -> new SplitOutput(delegate.createOutputForSplit(id)));
  }

  @Override
  public void releaseOutputForSplit(String splitId) {
    SplitOutput output = splits.remove(splitId);
    if (output != null && output.maximum != Long.MIN_VALUE) {
      output.emitWatermark(new Watermark(output.maximum));
    }
    delegate.releaseOutputForSplit(splitId);
  }

  @Override
  public void collect(ArrowBatch record) {
    delegate.collect(record);
  }

  @Override
  public void collect(ArrowBatch record, long timestamp) {
    delegate.collect(record, timestamp);
  }

  @Override
  public void emitWatermark(Watermark watermark) {
    delegate.emitWatermark(watermark);
  }

  @Override
  public void markIdle() {
    delegate.markIdle();
  }

  @Override
  public void markActive() {
    delegate.markActive();
  }

  private static final class SplitOutput implements SourceOutput<ArrowBatch> {
    private final SourceOutput<ArrowBatch> delegate;
    private long maximum = Long.MIN_VALUE;

    SplitOutput(SourceOutput<ArrowBatch> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void collect(ArrowBatch record) {
      delegate.collect(record);
      maximum = Math.max(maximum, record.sourceWatermarkMillis());
    }

    @Override
    public void collect(ArrowBatch record, long timestamp) {
      delegate.collect(record, timestamp);
      maximum = Math.max(maximum, record.sourceWatermarkMillis());
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      delegate.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      delegate.markIdle();
    }

    @Override
    public void markActive() {
      delegate.markActive();
    }
  }
}
