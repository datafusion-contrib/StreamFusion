package tech.streamfusion.parquet;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Whole-file native Parquet scans using the stock bounded file enumerator and reader state. */
public final class NativeParquetSource
    implements Source<ArrowBatch, FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> {
  private final FileSource<ArrowBatch> source;
  private final byte[] schema;
  private final String[] names;
  private final int batchRows;

  public NativeParquetSource(Path[] paths, byte[] schema, String[] names, int batchRows) {
    this.schema = schema.clone();
    this.names = names.clone();
    this.batchRows = batchRows;
    source =
        FileSource.forBulkFileFormat(
                new NativeParquetBulkFormat(schema, names, batchRows, null), paths)
            .build();
  }

  public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
  }

  public SourceReader<ArrowBatch, FileSourceSplit> createReader(SourceReaderContext context) {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    var format =
        new NativeParquetBulkFormat(
            schema, names, batchRows, context.metricGroup().counter("sourceArrowRows"));
    var reader =
        new FileSourceReader<ArrowBatch, FileSourceSplit>(
            context, format, context.getConfiguration());
    return new SourceReader<ArrowBatch, FileSourceSplit>() {
      public void start() {
        reader.start();
      }

      public org.apache.flink.core.io.InputStatus pollNext(ReaderOutput<ArrowBatch> output)
          throws Exception {
        return reader.pollNext(output);
      }

      public List<FileSourceSplit> snapshotState(long checkpoint) {
        return reader.snapshotState(checkpoint);
      }

      public CompletableFuture<Void> isAvailable() {
        return reader.isAvailable();
      }

      public void addSplits(List<FileSourceSplit> splits) {
        reader.addSplits(splits);
      }

      public void notifyNoMoreSplits() {
        reader.notifyNoMoreSplits();
      }

      public void handleSourceEvents(SourceEvent event) {
        reader.handleSourceEvents(event);
      }

      public void notifyCheckpointComplete(long checkpoint) throws Exception {
        reader.notifyCheckpointComplete(checkpoint);
      }

      public void notifyCheckpointAborted(long checkpoint) throws Exception {
        reader.notifyCheckpointAborted(checkpoint);
      }

      public void pauseOrResumeSplits(
          Collection<String> pause, Collection<String> resume) {
        reader.pauseOrResumeSplits(pause, resume);
      }

      public void close() throws Exception {
        // FileSourceReader closes fetchers but does not recycle every queued off-heap record.
        try {
          format.close();
        } finally {
          reader.close();
        }
      }
    };
  }

  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
      createEnumerator(SplitEnumeratorContext<FileSourceSplit> context) {
    return source.createEnumerator(context);
  }

  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
      restoreEnumerator(
          SplitEnumeratorContext<FileSourceSplit> context,
          PendingSplitsCheckpoint<FileSourceSplit> state) {
    return source.restoreEnumerator(context, state);
  }

  public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
    return source.getSplitSerializer();
  }

  public SimpleVersionedSerializer<PendingSplitsCheckpoint<FileSourceSplit>>
      getEnumeratorCheckpointSerializer() {
    return source.getEnumeratorCheckpointSerializer();
  }
}
