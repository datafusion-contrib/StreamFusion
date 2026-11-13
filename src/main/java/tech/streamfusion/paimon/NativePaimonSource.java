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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.WatermarkExpression;

/** Paimon's bounded or continuous enumeration and state with an Arrow-emitting task-side reader. */
public final class NativePaimonSource
    implements Source<ArrowBatch, FileStoreSourceSplit, PendingSplitsCheckpoint> {
  private static final long serialVersionUID = 1L;
  private final FileStoreTable table;
  private final ReadBuilder read;
  private final Source<?, FileStoreSourceSplit, PendingSplitsCheckpoint> delegate;
  private final boolean boundedBatch;
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
      FileStoreTable table,
      int[] projection,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression,
      Predicate partitionPredicate) {
    this(table, projection, batchRows, rowtimeIndex, watermarkExpression, partitionPredicate, null);
  }

  public static NativePaimonSource bounded(
      FileStoreTable table,
      int[] projection,
      int batchRows,
      Source<?, FileStoreSourceSplit, PendingSplitsCheckpoint> delegate) {
    if (delegate.getBoundedness() != Boundedness.BOUNDED)
      throw new IllegalArgumentException("A bounded Paimon source delegate is required");
    return new NativePaimonSource(table, projection, batchRows, -1, null, null, delegate);
  }

  private NativePaimonSource(
      FileStoreTable table,
      int[] projection,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression,
      Predicate partitionPredicate,
      Source<?, FileStoreSourceSplit, PendingSplitsCheckpoint> delegate) {
    if (batchRows <= 0) {
      throw new IllegalArgumentException("Paimon source batch size must be positive");
    }
    if (delegate == null) TableScanUtils.streamingReadingValidate(table);
    this.table = table;
    this.read = table.newReadBuilder().withProjection(projection).dropStats();
    if (partitionPredicate != null) read.withFilter(partitionPredicate);
    this.boundedBatch = delegate != null;
    this.delegate =
        delegate == null ? new ContinuousFileStoreSource(read, table.options(), null) : delegate;
    this.batchRows = batchRows;
    this.rowtimeIndex = rowtimeIndex;
    this.watermarkExpression = watermarkExpression;
  }

  public Source<ArrowBatch, FileStoreSourceSplit, PendingSplitsCheckpoint> withLineage() {
    return PaimonVersion.withSourceLineage(this, table);
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
        table, read, context, batchRows, rowtimeIndex, watermarkExpression, !boundedBatch);
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
