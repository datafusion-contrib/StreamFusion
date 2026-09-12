package tech.streamfusion.paimon;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CoordinatorCommittingRowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/** Arrow ingestion with Paimon's checkpoint, watermark, and coordinator-commit lifecycle. */
public final class NativePaimonCoordinatorCommitOperator
    extends CoordinatorCommittingRowDataStoreWriteOperator {

  private final int partitionArity;
  private final RowType rowType;

  NativePaimonCoordinatorCommitOperator(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      StoreSinkWrite.Provider provider,
      String commitUser,
      OperatorEventGateway gateway) {
    super(parameters, table, provider, commitUser, gateway);
    partitionArity = table.partitionKeys().size();
    rowType = LogicalTypeConversion.toLogicalType(table.rowType());
  }

  // Released Paimon fixes its lifecycle class's input to InternalRow. Erasing only this method
  // lets the Arrow-typed factory replace ingestion while inheriting the entire commit protocol.
  @Override
  @SuppressWarnings("rawtypes")
  public void processElement(StreamRecord element) throws Exception {
    BucketedArrowBatch batch = (BucketedArrowBatch) element.getValue();
    NativePaimonWriteOperator.writeAppendBundle(
        write, PaimonPartitions.fromBytes(batch.partition(), partitionArity), batch, rowType);
  }

  public static final class Factory extends TableWriteOperator.Factory<BucketedArrowBatch>
      implements CoordinatedOperatorFactory<Committable> {

    private final Committer.Factory<Committable, ManifestCommittable> committerFactory;

    public Factory(
        FileStoreTable table,
        StoreSinkWrite.Provider provider,
        String commitUser,
        Committer.Factory<Committable, ManifestCommittable> committerFactory) {
      super(table, provider, commitUser);
      this.committerFactory = committerFactory;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
        String operatorName, OperatorID operatorID) {
      return new CommittingWriteOperatorCoordinator.Provider(
          operatorID, committerFactory, true, initialCommitUser, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Committable>> T createStreamOperator(
        StreamOperatorParameters<Committable> parameters) {
      OperatorID id = parameters.getStreamConfig().getOperatorID();
      return (T)
          new NativePaimonCoordinatorCommitOperator(
              parameters,
              table,
              storeSinkWriteProvider,
              initialCommitUser,
              parameters.getOperatorEventDispatcher().getOperatorEventGateway(id));
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return NativePaimonCoordinatorCommitOperator.class;
    }
  }
}
