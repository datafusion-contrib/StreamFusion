package tech.streamfusion.paimon;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/**
 * Paimon's fixed-bucket sink with the row-fed write operator swapped for the bundle-fed one. An
 * append table keeps Paimon's own sink write, which takes bundles; a primary-key table gets the
 * native key-value wrapper, which buffers and writes level-0 files while retaining Paimon's
 * selected sink write for compaction, changelog production, and recovery. The native planner only
 * substitutes streaming inserts and never writes with a sink materializer or an overwrite, the
 * three facts Paimon's own provider would otherwise derive from the job.
 */
public final class NativePaimonFixedBucketSink extends FlinkWriteSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  private final boolean primaryKey;

  public NativePaimonFixedBucketSink(FileStoreTable table, boolean primaryKey) {
    super(table, null);
    this.primaryKey = primaryKey;
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
    return new NativePaimonWriteOperator.Factory(
        table,
        primaryKey ? keyValueWriteProvider(writeProvider) : writeProvider,
        commitUser,
        false);
  }

  private static StoreSinkWrite.Provider keyValueWriteProvider(StoreSinkWrite.Provider provider) {
    return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
        new NativeKeyValueSinkWrite(
            table,
            provider.provide(table, commitUser, state, ioManager, memoryPoolFactory, metricGroup));
  }
}
