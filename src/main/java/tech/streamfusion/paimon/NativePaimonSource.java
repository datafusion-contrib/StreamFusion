package tech.streamfusion.paimon;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.predicate.Predicate;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.WatermarkExpression;

/** Paimon's streaming enumerator and state formats with an Arrow-emitting task-side reader. */
public final class NativePaimonSource
    implements Source<ArrowBatch, FileStoreSourceSplit, PendingSplitsCheckpoint> {
  private static final long serialVersionUID = 1L;
  private final FileStoreTable table;
  private final ReadBuilder read;
  private final ContinuousFileStoreSource delegate;
  private final int batchRows;
  private final int rowtimeIndex;
  private final WatermarkExpression watermarkExpression;

  public NativePaimonSource(
      FileStoreTable table, int[] projection, int batchRows, int rowtimeIndex) {
    this(table, projection, batchRows, rowtimeIndex, WatermarkExpression.rowtime(rowtimeIndex));
  }

  public NativePaimonSource(
      FileStoreTable table,
      int[] projection,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression) {
    this(table, projection, batchRows, rowtimeIndex, watermarkExpression, null);
  }

  public NativePaimonSource(
      FileStoreTable table, int[] projection, int batchRows, int rowtimeIndex,
      WatermarkExpression watermarkExpression, Predicate partitionPredicate) {
    if (batchRows <= 0) {
      throw new IllegalArgumentException("Paimon source batch size must be positive");
    }
    TableScanUtils.streamingReadingValidate(table);
    this.table = table;
    this.read = table.newReadBuilder().withProjection(projection).dropStats();
    if (partitionPredicate != null) read.withFilter(partitionPredicate);
    this.delegate = new ContinuousFileStoreSource(read, table.options(), null);
    this.batchRows = batchRows;
    this.rowtimeIndex = rowtimeIndex;
    this.watermarkExpression = watermarkExpression;
  }

  public static String unsupportedTypeReason(FileStoreTable table) {
    return PaimonArrowFields.unsupportedTypeReason(table.rowType());
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<ArrowBatch, FileStoreSourceSplit> createReader(SourceReaderContext context) {
    return new NativePaimonSourceReader(
        table, read, context, batchRows, rowtimeIndex, watermarkExpression);
  }

  @Override
  public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> createEnumerator(
      SplitEnumeratorContext<FileStoreSourceSplit> context) throws Exception {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<FileStoreSourceSplit> context, PendingSplitsCheckpoint checkpoint)
      throws Exception {
    return delegate.restoreEnumerator(context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<FileStoreSourceSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }
}
