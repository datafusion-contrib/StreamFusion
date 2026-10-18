package tech.streamfusion.iceberg;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import tech.streamfusion.operator.ArrowBatch;

/** Retains host emission/checkpoint ordering while counting logical rows rather than batches. */
final class IcebergArrowSourceReader implements SourceReader<ArrowBatch, IcebergSourceSplit> {
  private final SourceReader<ArrowBatch, IcebergSourceSplit> delegate;
  private final Counter rows;
  private final Counter outputRows;
  private final IcebergArrowReaderFunction function;
  private ReaderOutput<ArrowBatch> original;
  private CountingOutput output;

  IcebergArrowSourceReader(
      SourceReader<ArrowBatch, IcebergSourceSplit> delegate,
      Counter rows,
      Counter outputRows,
      IcebergArrowReaderFunction function) {
    this.delegate = delegate;
    this.rows = rows;
    this.outputRows = outputRows;
    this.function = function;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<ArrowBatch> target) throws Exception {
    if (target != original) {
      original = target;
      output = new CountingOutput(target);
    }
    return delegate.pollNext(output);
  }

  @Override
  public List<IcebergSourceSplit> snapshotState(long checkpointId) {
    return delegate.snapshotState(checkpointId);
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return delegate.isAvailable();
  }

  @Override
  public void addSplits(List<IcebergSourceSplit> splits) {
    delegate.addSplits(splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    delegate.notifyNoMoreSplits();
  }

  @Override
  public void handleSourceEvents(SourceEvent event) {
    delegate.handleSourceEvents(event);
  }

  @Override
  public void notifyCheckpointComplete(long id) throws Exception {
    delegate.notifyCheckpointComplete(id);
  }

  @Override
  public void notifyCheckpointAborted(long id) throws Exception {
    delegate.notifyCheckpointAborted(id);
  }

  @Override
  public void pauseOrResumeSplits(Collection<String> pause, Collection<String> resume) {
    delegate.pauseOrResumeSplits(pause, resume);
  }

  @Override
  public void close() throws Exception {
    try {
      function.cancelIo();
    } finally {
      delegate.close();
    }
  }

  private final class CountingOutput implements ReaderOutput<ArrowBatch> {
    private final SourceOutput<ArrowBatch> target;

    private CountingOutput(SourceOutput<ArrowBatch> target) {
      this.target = target;
    }

    @Override
    public void collect(ArrowBatch batch) {
      int count = batch.rowCount();
      target.collect(batch);
      rows.inc(count - 1L);
      outputRows.inc(count - 1L);
    }

    @Override
    public void collect(ArrowBatch batch, long timestamp) {
      int count = batch.rowCount();
      target.collect(batch, timestamp);
      rows.inc(count - 1L);
      outputRows.inc(count - 1L);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      target.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      target.markIdle();
    }

    @Override
    public void markActive() {
      target.markActive();
    }

    @Override
    public SourceOutput<ArrowBatch> createOutputForSplit(String split) {
      return new CountingOutput(((ReaderOutput<ArrowBatch>) target).createOutputForSplit(split));
    }

    @Override
    public void releaseOutputForSplit(String split) {
      ((ReaderOutput<ArrowBatch>) target).releaseOutputForSplit(split);
    }
  }
}
