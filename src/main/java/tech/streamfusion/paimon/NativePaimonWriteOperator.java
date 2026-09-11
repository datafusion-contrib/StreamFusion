package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/**
 * Paimon's table write operator fed with routed Arrow batches instead of rows. Each batch already
 * belongs to one (partition, bucket). For an append table it enters Paimon's bundle write entry,
 * which hands it to the append writer for that bucket, and the file writer encodes the whole batch
 * natively; for a primary-key table it enters the native key-value sink write. State,
 * checkpointing, commit preparation, compaction, and the per-checkpoint refresh of writer options
 * (Paimon's {@code sink.writer-refresh-detectors}) stay Paimon's.
 */
public final class NativePaimonWriteOperator extends TableWriteOperator<BucketedArrowBatch> {

  private final int partitionArity;
  private final boolean stateless;
  private final boolean postpone;
  private final RowType rowType;

  private NativePaimonWriteOperator(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      StoreSinkWrite.Provider storeSinkWriteProvider,
      String initialCommitUser,
      boolean stateless) {
    super(parameters, table, storeSinkWriteProvider, initialCommitUser);
    this.partitionArity = table.partitionKeys().size();
    this.stateless = stateless;
    this.postpone = table.bucketMode() == BucketMode.POSTPONE_MODE;
    this.rowType = LogicalTypeConversion.toLogicalType(table.rowType());
  }

  @Override
  protected StoreSinkWriteState createState(
      int subtaskId,
      StateInitializationContext context,
      StoreSinkWriteState.StateValueFilter stateFilter)
      throws Exception {
    return stateless
        ? new NoopStoreSinkWriteState(subtaskId)
        : super.createState(subtaskId, context, stateFilter);
  }

  @Override
  protected String getCommitUser(StateInitializationContext context) throws Exception {
    if (stateless) {
      return commitUser == null ? initialCommitUser : commitUser;
    }
    return super.getCommitUser(context);
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) throws Exception {
    BucketedArrowBatch batch = element.getValue();
    BinaryRow partition = PaimonPartitions.fromBytes(batch.partition(), partitionArity);
    if (write instanceof NativeKeyValueSinkWrite) {
      ((NativeKeyValueSinkWrite) write)
          .writeBundle(
              partition, postpone ? BucketMode.POSTPONE_BUCKET : batch.bucket(), batch.root());
      return;
    }
    try (VectorSchemaRoot root = batch.root()) {
      ((StoreSinkWriteImpl) write)
          .getWrite()
          .writeBundle(partition, batch.bucket(), new ArrowBatchBundle(root, rowType));
    }
  }

  @Override
  protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
      throws IOException {
    List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);
    tryRefreshWrite();
    return committables;
  }

  /**
   * Operator factory installed by the native Paimon sinks in place of Paimon's row-fed one. A
   * bucket-unaware writer never conflicts with another, so like Paimon's own it keeps no state.
   */
  public static final class Factory extends TableWriteOperator.Factory<BucketedArrowBatch> {
    private final boolean stateless;

    public Factory(
        FileStoreTable table,
        StoreSinkWrite.Provider storeSinkWriteProvider,
        String initialCommitUser,
        boolean stateless) {
      super(table, storeSinkWriteProvider, initialCommitUser);
      this.stateless = stateless;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Committable>> T createStreamOperator(
        StreamOperatorParameters<Committable> parameters) {
      return (T)
          new NativePaimonWriteOperator(
              parameters, table, storeSinkWriteProvider, initialCommitUser, stateless);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return NativePaimonWriteOperator.class;
    }
  }
}
