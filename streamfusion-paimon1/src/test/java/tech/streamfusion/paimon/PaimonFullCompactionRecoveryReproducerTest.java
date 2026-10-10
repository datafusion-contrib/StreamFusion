package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in diagnostic using released Paimon commits and readers without a Flink job. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
    named = "sf.paimon1.reproduceFullCompactionCommit",
    matches = "true")
class PaimonFullCompactionRecoveryReproducerTest {
  @TempDir java.nio.file.Path directory;

  @Test
  void completeCommitProducesFinalChangelog() throws Exception {
    verify(false);
  }

  @Test
  void recoveredCommitMustProduceFinalChangelog() throws Exception {
    verify(true);
  }

  private void verify(boolean fail) throws Exception {
    Path root = new Path(directory.toUri());
    class FailBetweenSnapshots extends LocalFileIO {
      Path appendSnapshot;

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
                .option("changelog-producer", "full-compaction")
                .build());
    var table = FileStoreTableFactory.create(io, root);
    try (var disk = org.apache.paimon.disk.IOManager.create(directory.toString());
        var write = table.newWrite("repro").withIOManager(disk);
        var commit = table.newCommit("repro")) {
      var first = GenericRow.of(1, 10);
      write.write(first);
      write.compact(write.getPartition(first), write.getBucket(first), true);
      commit.commit(1, write.prepareCommit(true, 1));
      var scan = table.newStreamScan();
      assertTrue(values(table, scan.plan()).contains(10));

      var last = GenericRow.of(1, 20);
      write.write(last);
      write.compact(write.getPartition(last), write.getBucket(last), true);
      var messages = write.prepareCommit(true, Long.MAX_VALUE);
      assertTrue(
          messages.stream()
              .anyMatch(
                  message ->
                      !((CommitMessageImpl) message).compactIncrement().changelogFiles().isEmpty()),
          "The prepared commit must contain the final full-compaction changelog");
      if (fail) {
        io.appendSnapshot =
            table.snapshotManager().snapshotPath(table.snapshotManager().latestSnapshotId() + 1);
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
      List<Integer> batch = values(table, table.newScan().plan());
      assertTrue(batch.contains(20), "Latest value must be durable in the batch scan: " + batch);
      List<Integer> changelog = new ArrayList<>();
      for (int attempt = 0; attempt < 4; attempt++) {
        changelog.addAll(values(table, scan.plan()));
      }
      assertTrue(
          changelog.contains(20),
          "Final changelog missing: latest snapshot="
              + table.snapshotManager().latestSnapshot().commitKind()
              + ", batch="
              + batch
              + ", changelog="
              + changelog);
    }
  }

  private static List<Integer> values(FileStoreTable table, TableScan.Plan plan) throws Exception {
    List<Integer> values = new ArrayList<>();
    try (var reader = table.newRead().createReader(plan)) {
      var batch = reader.readBatch();
      while (batch != null) {
        try {
          var row = batch.next();
          while (row != null) {
            values.add(row.getInt(1));
            row = batch.next();
          }
        } finally {
          batch.releaseBatch();
        }
        batch = reader.readBatch();
      }
    }
    return values;
  }
}
