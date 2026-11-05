package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonSourceReadTest {
  @ParameterizedTest
  @ValueSource(strings = {"input", "lookup", "full-compaction"})
  void snapshotThenChangelogMatchesStockWithProjectionAndResume(String producer) throws Exception {
    FileStoreTable table = PaimonMergeEngineTest.table(Map.of("changelog-producer", producer));
    int[] projection = {8, 0, 3, 7};
    ReadBuilder read = table.newReadBuilder().withProjection(projection);
    var scan = read.newStreamScan();
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
      writer.write(PaimonMergeEngineTest.rows(60, false));
      writer.commit(1);
      var initial = scan.plan().splits();
      assertFalse(initial.isEmpty());
      assertRead(table, read, initial, true);
      writer.write(PaimonMergeEngineTest.rows(110, false));
      writer.commit(2);
      var changes = scan.plan().splits();
      assertFalse(changes.isEmpty());
      assertRead(table, read, changes, true);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 2})
  void appendSnapshotsAndNewFilesPreservePartitionsAndTypes(int buckets) throws Exception {
    FileStoreTable table =
        PaimonTestTables.createTable(
            java.nio.file.Files.createTempDirectory("paimon-append-read"),
            buckets == -1
                ? Map.of("bucket", "-1", "file.format", PaimonTestTables.fileFormat())
                : Map.of(
                    "bucket",
                    "2",
                    "bucket-key",
                    "id",
                    "file.format",
                    PaimonTestTables.fileFormat()));
    ReadBuilder read = table.newReadBuilder();
    var scan = read.newStreamScan();
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
        for (Object[] values : PaimonTestTables.values(31)) {
          writer.write(PaimonTestTables.paimonRow(values));
        }
        commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
        assertRead(table, read, scan.plan().splits(), true);
      }
    }
  }

  @org.junit.jupiter.api.Test
  void historicalSchemaUsesJavaMappingBeforeArrow() throws Exception {
    FileStoreTable table =
        PaimonTestTables.createTable(
            java.nio.file.Files.createTempDirectory("paimon-schema-read"),
            Map.of("bucket", "-1", "file.format", PaimonTestTables.fileFormat()));
    var builder = table.newStreamWriteBuilder().withCommitUser("schema");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (Object[] values : PaimonTestTables.values(23)) {
        writer.write(PaimonTestTables.paimonRow(values));
      }
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    PaimonTestCompat.schemaManager(table.fileIO(), table.location())
        .commitChanges(
            List.of(org.apache.paimon.schema.SchemaChange.renameColumn("name", "renamed")));
    table = org.apache.paimon.table.FileStoreTableFactory.create(table.fileIO(), table.location());
    var read = table.newReadBuilder().withProjection(new int[] {1, 0});
    assertRead(table, read, read.newStreamScan().plan().splits(), false);
  }

  @org.junit.jupiter.api.Test
  void latestSkipsSnapshotAndFollowsOnlyNewChangelog() throws Exception {
    FileStoreTable table =
        PaimonMergeEngineTest.table(Map.of("changelog-producer", "input", "scan.mode", "latest"));
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
      writer.write(PaimonMergeEngineTest.rows(60, false));
      writer.commit(1);
      var read = table.newReadBuilder().withProjection(new int[] {0, 3});
      var scan = read.newStreamScan();
      var initial = scan.plan();
      assertTrue(initial == null || initial.splits().isEmpty());
      writer.write(PaimonMergeEngineTest.rows(90, false));
      writer.commit(2);
      var changes = scan.plan().splits();
      assertFalse(changes.isEmpty());
      assertRead(table, read, changes, true);
    }
  }

  static void assertRead(
      FileStoreTable table, ReadBuilder read, List<Split> splits, boolean expectNative)
      throws Exception {
    var type = LogicalTypeConversion.toLogicalType(read.readType());
    for (Split split : splits) {
      List<String> expected = new ArrayList<>();
      try (var reader = read.newRead().createReader(split)) {
        org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
            batch;
        while ((batch = reader.readBatch()) != null) {
          org.apache.paimon.data.InternalRow row;
          while ((row = batch.next()) != null) {
            expected.add(string(new FlinkRowData(row), type));
          }
          batch.releaseBatch();
        }
      }
      for (int skip : new int[] {0, Math.min(3, expected.size()), expected.size()}) {
        List<String> actual = new ArrayList<>();
        try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 7, -1)) {
          reader.handleSplitsChanges(
              new SplitsAddition<>(List.of(new FileStoreSourceSplit("split", split, skip))));
          boolean finished = false;
          while (!finished) {
            var fetched = reader.fetch();
            if (fetched.nextSplit() != null) {
              NativeSourceRecord record;
              while ((record = fetched.nextRecordFromSplit()) != null) {
                try (VectorSchemaRoot root = record.batch().root()) {
                  for (RowData row : RowDataArrowConverter.read(root, type)) {
                    actual.add(string(row, type));
                  }
                }
                assertEquals(skip + actual.size(), record.nextOffset());
              }
            }
            finished = !fetched.finishedSplits().isEmpty();
            fetched.recycle();
          }
          assertEquals(
              expectNative, reader.nativeFilesRead() > 0 || reader.nativeSnapshotsRead() > 0);
        }
        assertEquals(expected.subList(skip, expected.size()), actual);
      }
    }
  }

  private static String string(RowData row, org.apache.flink.table.types.logical.RowType type) {
    var paimonType = org.apache.paimon.flink.LogicalTypeConversion.toDataType(type);
    return row.getRowKind()
        + ":"
        + PaimonTestTables.render(
            new org.apache.paimon.flink.FlinkRowWrapper(row),
            (org.apache.paimon.types.RowType) paimonType);
  }
}
