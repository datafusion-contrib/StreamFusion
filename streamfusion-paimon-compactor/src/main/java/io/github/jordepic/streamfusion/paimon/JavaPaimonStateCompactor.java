package io.github.jordepic.streamfusion.paimon;

import io.github.jordepic.streamfusion.state.StateTableCompactor;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

/**
 * Table maintenance by stock Java Paimon: each checkpoint opens the local state table, asks
 * Paimon's own compaction to look at every live bucket ({@code fullCompaction=false}, so its
 * universal strategy picks — usually nothing), and commits whatever it rewrote as a maintenance
 * snapshot directly beneath the checkpoint's data commit. Sequence numbers are preserved by
 * Paimon's rewriter and deletions drop exactly per its own rules.
 */
public class JavaPaimonStateCompactor implements StateTableCompactor {

  private static final String COMMIT_USER = "streamfusion-compactor";

  @Override
  public boolean available() {
    try {
      Class.forName("org.apache.paimon.table.FileStoreTableFactory");
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }

  @Override
  public boolean supports(String fileFormat) {
    // The deployed Paimon must have a reader/writer for the state files (vortex arrives with
    // Paimon 2.0; parquet is always in the bundle).
    try {
      org.apache.paimon.factories.FormatFactoryUtil.discoverFactory(
          JavaPaimonStateCompactor.class.getClassLoader(), fileFormat.toLowerCase());
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public void compact(String tableDirectory, long checkpointId) throws Exception {
    FileStoreTable table =
        FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDirectory));
    if (table.snapshotManager().latestSnapshotId() == null) {
      return; // nothing committed yet
    }
    Set<Integer> buckets = new HashSet<>();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      buckets.add(((DataSplit) split).bucket());
    }
    if (buckets.isEmpty()) {
      return;
    }
    StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(COMMIT_USER);
    try (StreamTableWrite write = builder.newWrite();
        StreamTableCommit commit = builder.newCommit()) {
      for (int bucket : buckets) {
        write.compact(BinaryRow.EMPTY_ROW, bucket, false);
      }
      List<CommitMessage> messages = write.prepareCommit(true, checkpointId);
      // Nothing picked -> no snapshot; an empty maintenance commit every barrier would bloat
      // snapshot history for no work.
      boolean empty =
          messages.stream().allMatch(message -> ((CommitMessageImpl) message).isEmpty());
      if (!empty) {
        commit.commit(checkpointId, messages);
      }
    }
  }
}
