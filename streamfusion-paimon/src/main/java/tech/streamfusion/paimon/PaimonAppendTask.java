package tech.streamfusion.paimon;

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

/** Decorates only the payload writer passed to the original task. */
final class PaimonAppendTask extends AppendCompactTask {
  private final AppendCompactTask original;
  private final NativePaimonCompactRewriter.Counters counters;

  private PaimonAppendTask(
      AppendCompactTask original, NativePaimonCompactRewriter.Counters counters) {
    super(original.partition(), original.compactBefore());
    this.original = original;
    this.counters = counters;
  }

  static AppendCompactTask wrap(
      AppendCompactTask task, NativePaimonCompactRewriter.Counters counters) {
    return task.getClass() == AppendCompactTask.class && task.compactAfter().isEmpty()
        ? new PaimonAppendTask(task, counters)
        : task;
  }

  @Override
  public CommitMessage doCompact(FileStoreTable table, BaseAppendFileStoreWrite write)
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
