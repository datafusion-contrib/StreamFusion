package tech.streamfusion.paimon;

import java.util.Map;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/** Bounded postpone writes use Paimon's fixed-bucket commit protocol with native Arrow files. */
public final class NativePaimonPostponeFixedBucketSink extends FlinkWriteSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;
  private final Map<BinaryRow, Integer> knownBuckets;
  private final int defaultBuckets;

  public NativePaimonPostponeFixedBucketSink(
      FileStoreTable table, Map<BinaryRow, Integer> knownBuckets, int defaultBuckets) {
    super(PaimonPostponeFixedWrite.writerTable(table), null);
    this.knownBuckets = knownBuckets;
    this.defaultBuckets = defaultBuckets;
  }

  protected StoreSinkWrite.Provider createWriteProvider(
      CheckpointConfig checkpoints, boolean streaming, boolean materializer) {
    return PaimonPostponeFixedWrite.provider(table, checkpoints, streaming, materializer);
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider provider, String commitUser) {
    FileStoreTable delegateTable = table;
    FileStoreTable fileTable = table.copy(Map.of("bucket", "1", "write-only", "true"));
    Map<BinaryRow, Integer> counts = knownBuckets;
    int fallbackCount = defaultBuckets;
    StoreSinkWrite.Provider wrapped =
        (ignored, user, state, io, memory, metrics) ->
            new NativeKeyValueSinkWrite(
                fileTable,
                provider.provide(delegateTable, user, state, io, memory, metrics),
                state,
                counts,
                fallbackCount);
    return new NativePaimonWriteOperator.Factory(fileTable, wrapped, commitUser, false);
  }

  @Override
  protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
    return createRestoreOnlyCommittableStateManager(table);
  }

  protected boolean writeOnly() {
    return true;
  }

  @Override
  protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
    return new PaimonPostponeFixedWrite(table, knownBuckets).committer();
  }
}
