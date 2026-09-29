package tech.streamfusion.paimon;

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
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/**
 * Paimon's table write operator fed with routed Arrow batches instead of rows. Each batch already
 * belongs to one (partition, bucket). For an append table it enters Paimon's bundle write entry,
 * which hands it to the append writer for that bucket, and the file writer encodes the whole batch
 * natively; for a primary-key table it enters the native key-value sink write. State,
 * checkpointing, commit preparation, and compaction stay Paimon's.
 */
public final class NativePaimonWriteOperator extends TableWriteOperator<BucketedArrowBatch> {

  private final int partitionArity;
  private final boolean stateless;
  private final String initialUser;
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
    this.initialUser = initialCommitUser;
    this.rowType = LogicalTypeConversion.toLogicalType(table.rowType());
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    if (table
        .coreOptions()
        .toConfiguration()
        .get(org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY)) {
      var manager = getContainingTask().getEnvironment().getMemoryManager();
      long budget = (long) memoryPool.freePages() * memoryPool.pageSize();
      if (write instanceof NativeKeyValueSinkWrite) {
        ((NativeKeyValueSinkWrite) write).managedMemory(manager, budget);
      } else if (write instanceof NativeAppendSinkWrite) {
        ((NativeAppendSinkWrite) write).managedMemory(manager, budget);
      }
    }
  }

  @Override
  protected StoreSinkWriteState createState(
      StateInitializationContext context, StoreSinkWriteState.StateValueFilter stateFilter)
      throws Exception {
    return stateless
        ? new NoopStoreSinkWriteState(stateFilter)
        : super.createState(context, stateFilter);
  }

  @Override
  protected String getCommitUser(StateInitializationContext context) throws Exception {
    if (stateless) {
      return initialUser;
    }
    return super.getCommitUser(context);
  }

  @Override
  protected boolean containLogSystem() {
    return false;
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) throws Exception {
    BucketedArrowBatch batch = element.getValue();
    BinaryRow partition = PaimonPartitions.fromBytes(batch.partition(), partitionArity);
    if (write instanceof NativeKeyValueSinkWrite) {
      ((NativeKeyValueSinkWrite) write).writeBundle(partition, batch.bucket(), batch.root());
      return;
    }
    writeAppendBundle(write, partition, batch, rowType);
  }

  static void writeAppendBundle(
      StoreSinkWrite write, BinaryRow partition, BucketedArrowBatch batch, RowType rowType)
      throws Exception {
    if (write instanceof NativeAppendSinkWrite) {
      ((NativeAppendSinkWrite) write).writeBundle(partition, batch.bucket(), batch.root());
      return;
    }
    try (VectorSchemaRoot root = batch.root()) {
      ((StoreSinkWriteImpl) write)
          .getWrite()
          .writeBundle(partition, batch.bucket(), new ArrowBatchBundle(root, rowType));
    }
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
