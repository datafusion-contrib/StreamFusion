package tech.streamfusion.paimon;

import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.flink.sink.AppendCompactWorkerOperator;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.table.FileStoreTable;

/** Only the released worker constructor and task type differ. */
abstract class PaimonAppendWorkerCompat extends AppendCompactWorkerOperator<Object> {
  PaimonAppendWorkerCompat(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      String user,
      boolean streaming) {
    super(parameters, table, user);
  }

  final void processTask(
      Object value, boolean refresh, NativePaimonCompactRewriter.Counters counters)
      throws Exception {
    var task = (UnawareAppendCompactionTask) value;

    unawareBucketCompactor.processElement(PaimonAppendTask.wrap(task, counters));
  }

  abstract static class Factory extends AppendCompactWorkerOperator.Factory<Object> {
    Factory(FileStoreTable table, String user, boolean streaming) {
      super(table, user);
    }
  }
}
