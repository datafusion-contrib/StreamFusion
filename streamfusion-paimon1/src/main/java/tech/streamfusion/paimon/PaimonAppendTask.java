package tech.streamfusion.paimon;

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

/** Decorates only the payload writer passed to the original task. */
final class PaimonAppendTask extends UnawareAppendCompactionTask {
  private final UnawareAppendCompactionTask original;
  private final NativePaimonCompactRewriter.Counters counters;

  private PaimonAppendTask(
      UnawareAppendCompactionTask original, NativePaimonCompactRewriter.Counters counters) {
    super(original.partition(), original.compactBefore());
    this.original = original;
    this.counters = counters;
  }

  static UnawareAppendCompactionTask wrap(
      UnawareAppendCompactionTask task, NativePaimonCompactRewriter.Counters counters) {
    return task.getClass() == UnawareAppendCompactionTask.class && task.compactAfter().isEmpty()
        ? new PaimonAppendTask(task, counters)
        : task;
  }

  @Override
  public CommitMessage doCompact(FileStoreTable table, AppendOnlyFileStoreWrite write)
      throws Exception {
    if (!PaimonVersion.supportsAppendWrite(write)) {
      counters.stockGroups.inc();
      return original.doCompact(table, write);
    }
    var adapted = new PaimonAppendCompactWrite(table, write, counters);
    try (AutoCloseable close = adapted::close) {
      return original.doCompact(table, adapted);
    }
  }
}
