package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class LegacyPaimonReadBoundaryTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir;

  private FileStoreTable table(String format) throws Exception {
    var io = LocalFileIO.create();
    var path = new Path(Files.createTempDirectory(tempDir, "paimon1-boundary").toUri());
    new SchemaManager(io, path)
        .createTable(
            new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "v", DataTypes.INT()))
                    .getFields(),
                List.of(),
                List.of("id"),
                Map.of("bucket", "1", "file.format", format, "write-only", "true"),
                ""));
    return FileStoreTableFactory.create(io, path);
  }

  private void write(FileStoreTable table, int value, long checkpoint) throws Exception {
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int i = 0; i < 20; i++) writer.write(GenericRow.of(i, value));
      commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void beforeFilesRetainRetractionsAndResumeOffsets(String format) throws Exception {
    var table = table(format);
    write(table, 1, 1);
    var before = (DataSplit) table.newReadBuilder().newScan().plan().splits().get(0);
    write(table, 2, 2);
    var after = (DataSplit) table.newReadBuilder().newScan().plan().splits().get(0);
    var split =
        DataSplit.builder()
            .withSnapshot(after.snapshotId())
            .withPartition(after.partition())
            .withBucket(after.bucket())
            .withBucketPath(after.bucketPath())
            .withBeforeFiles(before.dataFiles())
            .withDataFiles(after.dataFiles())
            .isStreaming(true)
            .build();
    assertTrue(PaimonVersion.hasBeforeFiles(split));
    assertRead(table, split, false, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void historicalSchemaRetainsMappingAndResumeOffsets(String format) throws Exception {
    var table = table(format);
    write(table, 7, 1);
    new SchemaManager(table.fileIO(), table.location())
        .commitChanges(List.of(SchemaChange.renameColumn("v", "renamed")));
    table = FileStoreTableFactory.create(table.fileIO(), table.location());
    for (var split : table.newReadBuilder().newScan().plan().splits())
      assertRead(table, split, false, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void currentSnapshotRetainsResumeOffsets(String format) throws Exception {
    var table = table(format);
    write(table, 7, 1);
    for (var split : table.newReadBuilder().newScan().plan().splits())
      assertRead(table, split, true, false);
  }

  private void assertRead(
      FileStoreTable table, Split split, boolean nativeExpected, boolean retractionsExpected)
      throws Exception {
    var read = table.newReadBuilder().withProjection(new int[] {1, 0});
    var type = LogicalTypeConversion.toLogicalType(read.readType());
    List<String> expected = new ArrayList<>();
    try (var reader = read.newRead().createReader(split)) {
      reader.forEachRemaining(
          row -> expected.add(row.getRowKind() + ":" + row.getInt(0) + ":" + row.getInt(1)));
    }
    assertFalse(expected.isEmpty());
    if (retractionsExpected)
      assertTrue(
          expected.stream().anyMatch(s -> s.startsWith("DELETE") || s.startsWith("UPDATE_BEFORE")),
          expected.toString());
    for (int skip : new int[] {0, 3, expected.size()}) {
      List<String> actual = new ArrayList<>();
      try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 7, -1)) {
        reader.handleSplitsChanges(
            new SplitsAddition<>(List.of(new FileStoreSourceSplit("s", split, skip))));
        boolean finished = false;
        while (!finished) {
          var fetched = reader.fetch();
          if (fetched.nextSplit() != null) {
            var record = fetched.nextRecordFromSplit();
            while (record != null) {
              try (var root = record.batch().root()) {
                for (var row : RowDataArrowConverter.read(root, type))
                  actual.add(row.getRowKind() + ":" + row.getInt(0) + ":" + row.getInt(1));
              }
              assertEquals(skip + actual.size(), record.nextOffset());
              record = fetched.nextRecordFromSplit();
            }
          }
          finished = !fetched.finishedSplits().isEmpty();
          fetched.recycle();
        }
        assertEquals(
            nativeExpected, reader.nativeFilesRead() > 0 || reader.nativeSnapshotsRead() > 0);
      }
      assertEquals(expected.subList(skip, expected.size()), actual);
    }
  }
}
