package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Opt-in reproducer for released Paimon 1.0 split-commit recovery; no native operators involved.
 */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
    named = "sf.paimon1.reproduceSplitCommit",
    matches = "true")
public class PaimonSplitCommitRecoveryReproducerTest {
  @TempDir java.nio.file.Path directory;

  @Test
  public void completedCompactionExposesLatestValue() throws Exception {
    verifyCommit(false);
  }

  @Test
  public void recoveredCompactionMustExposeLatestValue() throws Exception {
    verifyCommit(true);
  }

  private void verifyCommit(boolean fail) throws Exception {
    Path root = new Path(directory.toUri());
    class FailBetweenSnapshots extends LocalFileIO {
      volatile Path appendSnapshot;

      @Override
      public SeekableInputStream newInputStream(Path path) throws IOException {
        if (appendSnapshot != null
            && path.toString().contains("/manifest/")
            && super.exists(appendSnapshot)) {
          appendSnapshot = null;
          throw new IOException("Injected failure after APPEND before COMPACT");
        }
        return super.newInputStream(path);
      }
    }
    var io = new FailBetweenSnapshots();
    new SchemaManager(io, root)
        .createTable(
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", DataTypes.INT())
                .primaryKey("k")
                .option("bucket", "1")
                .option("deletion-vectors.enabled", "true")
                .build());
    var table = FileStoreTableFactory.create(io, root);
    try (var disk = org.apache.paimon.disk.IOManager.create(directory.toString());
        var write = table.newWrite("repro").withIOManager(disk);
        var commit = table.newCommit("repro")) {
      write.write(GenericRow.of(1, 10));
      commit.commit(1, write.prepareCommit(true, 1));
      write.write(GenericRow.of(1, 20));
      var messages = write.prepareCommit(true, Long.MAX_VALUE);
      long next = table.snapshotManager().latestSnapshotId() + 1;
      if (fail) {
        io.appendSnapshot = table.snapshotManager().snapshotPath(next);
        assertThrows(Exception.class, () -> commit.commit(Long.MAX_VALUE, messages));
        assertNull(io.appendSnapshot, "Failure must occur after APPEND");
        assertEquals(
            org.apache.paimon.Snapshot.CommitKind.APPEND,
            table.snapshotManager().latestSnapshot().commitKind());
        try (var recovered = table.newCommit("repro")) {
          recovered.filterAndCommit(Collections.singletonMap(Long.MAX_VALUE, messages));
        }
      } else {
        commit.commit(Long.MAX_VALUE, messages);
      }
      try (var reader = table.newRead().createReader(table.newScan().plan())) {
        var batch = reader.readBatch();
        assertNotNull(batch);
        try {
          var row = batch.next();
          assertNotNull(row);
          assertEquals(20, row.getInt(1), "Recovered commit must expose the newest value");
        } finally {
          batch.releaseBatch();
        }
      }
    }
  }
}
