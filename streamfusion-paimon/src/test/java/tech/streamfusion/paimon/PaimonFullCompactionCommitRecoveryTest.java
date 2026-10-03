package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Released Paimon only: fail between the data and full-compaction snapshot commits. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(
    named = "sf.paimon.reproduceSplitCommit",
    matches = "true")
class PaimonFullCompactionCommitRecoveryTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void recoveredFinalCommitPreservesTheFinalChangelog(boolean fail) throws Exception {
    Path root = new Path(directory.toUri());
    class FailBetweenSnapshots extends LocalFileIO {
      final AtomicReference<Path> appendSnapshot = new AtomicReference<>();

      @Override
      public SeekableInputStream newInputStream(Path path) throws IOException {
        Path append = appendSnapshot.get();
        if (append != null
            && path.toString().contains("/manifest/")
            && super.exists(append)
            && appendSnapshot.compareAndSet(append, null)) {
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
      write.write(GenericRow.of(1, 10));
      write.compact(BinaryRow.EMPTY_ROW, 0, true);
      commit.commit(1, write.prepareCommit(true, 1));
      write.write(GenericRow.of(1, 20));
      write.compact(BinaryRow.EMPTY_ROW, 0, true);
      var messages = write.prepareCommit(true, 2);
      if (fail) {
        io.appendSnapshot.set(
            table.snapshotManager().snapshotPath(table.snapshotManager().latestSnapshotId() + 1));
        assertThrows(Exception.class, () -> commit.commit(2, messages));
        assertNull(io.appendSnapshot.get(), "Failure must occur after APPEND");
        assertEquals(
            org.apache.paimon.Snapshot.CommitKind.APPEND,
            table.snapshotManager().latestSnapshot().commitKind());
        try (var recovered = table.newCommit("repro")) {
          recovered.filterAndCommit(Collections.singletonMap(2L, messages));
        }
      } else {
        commit.commit(2, messages);
      }
      try (var reader = table.newRead().createReader(table.newScan().plan())) {
        var batch = reader.readBatch();
        assertNotNull(batch);
        try {
          var row = batch.next();
          assertNotNull(row);
          assertEquals(20, row.getInt(1), "The newest data was committed");
        } finally {
          batch.releaseBatch();
        }
      }
      assertEquals(
          List.of("+I 1|10", "+U 1|20", "-U 1|10"),
          PaimonChangelogSinkWriteTest.changelogRows(table));
    }
  }
}
