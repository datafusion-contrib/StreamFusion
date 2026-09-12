package tech.streamfusion.paimon;

import javax.annotation.Nullable;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.AppendTableSink;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/**
 * Paimon's bucket-unaware append sink fed with routed Arrow batches. Its write topology, including
 * the in-job compaction coordinator and workers, is inherited unchanged; only the writer operator
 * consumes bundles. It keeps no per-bucket writer state; coordinator commits retain Paimon's
 * separate pending-checkpoint state.
 */
public final class NativePaimonAppendSink extends AppendTableSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  public NativePaimonAppendSink(FileStoreTable table, @Nullable Integer parallelism) {
    super(table, null, parallelism);
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
    writeProvider = NativeAppendSinkWrite.provider(writeProvider);
    if (coordinatorCommitEnabled()) {
      return new NativePaimonCoordinatorCommitOperator.Factory(
          table, writeProvider, commitUser, createCommitterFactory());
    }
    return new NativePaimonWriteOperator.Factory(table, writeProvider, commitUser, true);
  }

  @Override
  protected boolean coordinatorCommitEnabled() {
    return table
        .coreOptions()
        .toConfiguration()
        .get(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED);
  }

  @Override
  protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
    return createRestoreOnlyCommittableStateManager(table);
  }
}
