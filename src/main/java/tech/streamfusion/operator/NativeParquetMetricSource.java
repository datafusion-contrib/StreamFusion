package tech.streamfusion.operator;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Adds datafusion-comet's native Parquet scan metric surface to Flink's file source reader. */
public final class NativeParquetMetricSource
    implements Source<ArrowBatch, FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>,
        DynamicParallelismInference {

  private static final String[] ZERO_METRICS = {
    "file_open_errors",
    "file_scan_errors",
    "predicate_evaluation_errors",
    "num_predicate_creation_errors",
    "files_ranges_pruned_statistics",
    "files_ranges_matched_statistics",
    "row_groups_matched_bloom_filter",
    "row_groups_pruned_bloom_filter",
    "row_groups_matched_statistics",
    "row_groups_pruned_statistics",
    "limit_pruned_row_groups",
    "limit_matched_row_groups",
    "bytes_scanned",
    "pushdown_rows_pruned",
    "pushdown_rows_matched",
    "row_pushdown_eval_time",
    "statistics_eval_time",
    "bloom_filter_eval_time",
    "page_index_rows_pruned",
    "page_index_rows_matched",
    "page_index_pages_pruned",
    "page_index_pages_matched",
    "page_index_eval_time",
    "predicate_cache_inner_records",
    "predicate_cache_records",
    "scan_efficiency_ratio_total"
  };

  private final FileSource<ArrowBatch> source;

  public NativeParquetMetricSource(FileSource<ArrowBatch> source) {
    this.source = source;
  }

  @Override
  public SourceReader<ArrowBatch, FileSourceSplit> createReader(SourceReaderContext context)
      throws Exception {
    return new MetricReader(source.createReader(context), context);
  }

  @Override
  public Boundedness getBoundedness() {
    return source.getBoundedness();
  }

  @Override
  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> createEnumerator(
      SplitEnumeratorContext<FileSourceSplit> context) throws Exception {
    return source.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> restoreEnumerator(
      SplitEnumeratorContext<FileSourceSplit> context,
      PendingSplitsCheckpoint<FileSourceSplit> checkpoint)
      throws Exception {
    return source.restoreEnumerator(context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
    return source.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<PendingSplitsCheckpoint<FileSourceSplit>>
      getEnumeratorCheckpointSerializer() {
    return source.getEnumeratorCheckpointSerializer();
  }

  @Override
  public Set<? extends WatermarkDeclaration> declareWatermarks() {
    return source.declareWatermarks();
  }

  @Override
  public int inferParallelism(Context context) {
    return source.inferParallelism(context);
  }

  private static final class MetricReader implements SourceReader<ArrowBatch, FileSourceSplit> {

    private final SourceReader<ArrowBatch, FileSourceSplit> reader;
    private final Counter standardRecordsIn;
    private final Counter outputRows;
    private final Counter opening;
    private final Counter scanningUntilData;
    private final Counter scanningTotal;
    private final Counter processing;
    private final Counter metadataLoad;

    private MetricReader(
        SourceReader<ArrowBatch, FileSourceSplit> reader, SourceReaderContext context) {
      this.reader = reader;
      MetricGroup metrics = context.metricGroup();
      standardRecordsIn = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
      outputRows = metrics.counter("output_rows");
      opening = metrics.counter("time_elapsed_opening");
      scanningUntilData = metrics.counter("time_elapsed_scanning_until_data");
      scanningTotal = metrics.counter("time_elapsed_scanning_total");
      processing = metrics.counter("time_elapsed_processing");
      metadataLoad = metrics.counter("metadata_load_time");
      for (String name : ZERO_METRICS) {
        metrics.counter(name);
      }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<ArrowBatch> output) throws Exception {
      return reader.pollNext(new MetricReaderOutput(output, this));
    }

    private void record(ArrowBatch batch) {
      int rows = batch.rowCount();
      outputRows.inc(rows);
      if (rows > 1) {
        standardRecordsIn.inc(rows - 1L);
      }
      ArrowBatch.NativeScanMetrics scan = batch.nativeScanMetrics();
      if (scan != null) {
        opening.inc(scan.openingNanos);
        scanningUntilData.inc(scan.scanningUntilDataNanos);
        scanningTotal.inc(scan.scanningNanos);
        processing.inc(scan.processingNanos);
        metadataLoad.inc(scan.openingNanos);
      }
    }

    @Override
    public void start() {
      reader.start();
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) {
      return reader.snapshotState(checkpointId);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return reader.isAvailable();
    }

    @Override
    public void addSplits(List<FileSourceSplit> splits) {
      reader.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
      reader.notifyNoMoreSplits();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
      reader.handleSourceEvents(sourceEvent);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      reader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
      reader.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void pauseOrResumeSplits(
        Collection<String> splitsToPause, Collection<String> splitsToResume) {
      reader.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }

  private static final class MetricReaderOutput implements ReaderOutput<ArrowBatch> {

    private final ReaderOutput<ArrowBatch> output;
    private final MetricReader metrics;

    private MetricReaderOutput(ReaderOutput<ArrowBatch> output, MetricReader metrics) {
      this.output = output;
      this.metrics = metrics;
    }

    @Override
    public void collect(ArrowBatch batch) {
      metrics.record(batch);
      output.collect(batch);
    }

    @Override
    public void collect(ArrowBatch batch, long timestamp) {
      metrics.record(batch);
      output.collect(batch, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      output.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      output.markIdle();
    }

    @Override
    public void markActive() {
      output.markActive();
    }

    @Override
    public SourceOutput<ArrowBatch> createOutputForSplit(String splitId) {
      return new MetricSourceOutput(output.createOutputForSplit(splitId), metrics);
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
      output.releaseOutputForSplit(splitId);
    }
  }

  private static final class MetricSourceOutput implements SourceOutput<ArrowBatch> {

    private final SourceOutput<ArrowBatch> output;
    private final MetricReader metrics;

    private MetricSourceOutput(SourceOutput<ArrowBatch> output, MetricReader metrics) {
      this.output = output;
      this.metrics = metrics;
    }

    @Override
    public void collect(ArrowBatch batch) {
      metrics.record(batch);
      output.collect(batch);
    }

    @Override
    public void collect(ArrowBatch batch, long timestamp) {
      metrics.record(batch);
      output.collect(batch, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      output.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      output.markIdle();
    }

    @Override
    public void markActive() {
      output.markActive();
    }
  }
}
