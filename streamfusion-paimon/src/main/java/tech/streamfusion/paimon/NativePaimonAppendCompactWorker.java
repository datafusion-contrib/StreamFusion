package tech.streamfusion.paimon;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.table.FileStoreTable;

/** Paimon retains the worker lifecycle; task decoration replaces only file payload rewriting. */
final class NativePaimonAppendCompactWorker extends PaimonAppendWorkerCompat {
  private final boolean bypass;
  private transient NativePaimonCompactRewriter.Counters counters;

  private NativePaimonAppendCompactWorker(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      String user,
      boolean streaming,
      boolean bypass) {
    super(parameters, table, user, streaming);
    this.bypass = bypass;
  }

  @Override
  public void open() throws Exception {
    tech.streamfusion.operator.NativeAllocator.initializeFor(this);
    super.open();
    counters = PaimonCompactionProvider.counters(getMetricGroup());
  }

  @Override
  public void processElement(StreamRecord<Object> element) throws Exception {
    Object task = element.getValue();
    if (bypass) {
      var either = (Either<Committable, ?>) task;
      if (either.isLeft()) {
        output.collect(new StreamRecord<>(either.left()));
        return;
      }
      task = either.right();
    }
    processTask(task, !bypass, counters);
  }

  static final class Factory extends PaimonAppendWorkerCompat.Factory {
    private static final long serialVersionUID = 1L;
    private final boolean streaming, bypass;

    Factory(FileStoreTable table, String user, boolean streaming, boolean bypass) {
      super(table, user, streaming);
      this.streaming = streaming;
      this.bypass = bypass;
    }

    @Override
    public <T extends StreamOperator<Committable>> T createStreamOperator(
        StreamOperatorParameters<Committable> parameters) {
      return (T)
          new NativePaimonAppendCompactWorker(parameters, table, commitUser, streaming, bypass);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
      return NativePaimonAppendCompactWorker.class;
    }
  }
}
