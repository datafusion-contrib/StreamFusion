package tech.streamfusion.paimon;

import javax.annotation.Nullable;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/** Paimon 1.0's append compaction topology with an Arrow-fed writer. */
public final class NativePaimonAppendSink
    extends org.apache.paimon.flink.sink.UnawareBucketSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  public NativePaimonAppendSink(FileStoreTable table, @Nullable Integer parallelism) {
    super(table, null, null, parallelism);
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider provider, String commitUser) {
    return new NativePaimonWriteOperator.Factory(
        table, NativeAppendSinkWrite.provider(provider), commitUser, true);
  }
}
