package tech.streamfusion.paimon;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/** Paimon's conflict-free postpone commit lifecycle with an Arrow writer retaining input order. */
public final class NativePaimonPostponeSink extends FlinkWriteSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  public NativePaimonPostponeSink(FileStoreTable table) {
    super(table, null);
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider provider, String initialCommitUser) {
    StoreSinkWrite.Provider wrapped =
        (table, commitUser, state, io, memory, metrics) ->
            new NativeKeyValueSinkWrite(
                table,
                provider.provide(table, commitUser, state, io, memory, metrics),
                String.format(
                    "%s-u-%s-s-%d-w-",
                    table.coreOptions().dataFilePrefix(), commitUser, state.getSubtaskId()));
    return new NativePaimonWriteOperator.Factory(table, wrapped, initialCommitUser, true);
  }

  @Override
  protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
    return createRestoreOnlyCommittableStateManager(table);
  }
}
