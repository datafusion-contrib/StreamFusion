package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonCompactionReadTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void physicalWinnersRetainStoredSequencesAndKinds(boolean thin, boolean dropDelete)
      throws Exception {
    var path = new Path(directory.resolve("table").toUri());
    var io = LocalFileIO.create();
    new FileSystemSchemaManager(io, path)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("v", DataTypes.INT())
                .primaryKey("id")
                .option("bucket", "1")
                .option("file.format", "parquet")
                .option("data-file.thin-mode", Boolean.toString(thin))
                .option("write-only", "true")
                .build());
    var table = FileStoreTableFactory.create(io, path);
    var builder = table.newStreamWriteBuilder().withCommitUser("physical-read");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.write(GenericRow.ofKind(RowKind.INSERT, 1, 10));
      writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 2, 20));
      writer.write(GenericRow.ofKind(RowKind.UPDATE_BEFORE, 3, 30));
      writer.write(GenericRow.ofKind(RowKind.DELETE, 4, 40));
      commit.commit(1, writer.prepareCommit(true, 1));
      writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 11));
      writer.write(GenericRow.ofKind(RowKind.DELETE, 2, 21));
      writer.write(GenericRow.ofKind(RowKind.INSERT, 5, 50));
      commit.commit(2, writer.prepareCommit(true, 2));
    }
    var splits = table.newReadBuilder().newScan().plan().splits();
    var first = (DataSplit) splits.get(0);
    var split =
        DataSplit.builder()
            .withSnapshot(first.snapshotId())
            .withPartition(first.partition())
            .withBucket(first.bucket())
            .withBucketPath(first.bucketPath())
            .withDataFiles(
                splits.stream()
                    .flatMap(s -> ((DataSplit) s).dataFiles().stream())
                    .collect(java.util.stream.Collectors.toList()))
            .rawConvertible(false)
            .isStreaming(false)
            .build();
    assertEquals(2, split.dataFiles().size());
    List<String> expected = new ArrayList<>();
    try (var spill = IOManager.create(directory.resolve("spill").toString());
        var reader =
            new RecordReaderIterator<KeyValue>(
                ((KeyValueFileStore) table.store())
                    .newRead()
                    .withIOManager(spill)
                    .forceKeepDelete()
                    .createReader(split))) {
      while (reader.hasNext()) {
        var row = reader.next();
        if (!dropDelete || row.isAdd())
          expected.add(
              row.key().getInt(0)
                  + ":"
                  + row.sequenceNumber()
                  + ":"
                  + row.valueKind().toByteValue()
                  + ":"
                  + row.value().getInt(0)
                  + ":"
                  + row.value().getInt(1));
      }
    }
    List<String> actual = new ArrayList<>();
    var outputTable = table.copy(Map.of("target-file-row-num", "1"));
    var outputWriter =
        new NativePaimonKeyValueFileWriter(outputTable, PaimonKeyValueLayout.of(outputTable));
    try (var reader =
            PaimonRustReader.compaction(
                table, split, PaimonKeyValueLayout.of(table), 2, dropDelete);
        var output = outputWriter.compaction(first.partition(), first.bucket(), 2)) {
      for (var batch = reader.next(); batch != null; batch = reader.next()) {
        try (var root = batch) {
          output.write(root);
          for (int row = 0; row < root.getRowCount(); row++) {
            List<String> fields = new ArrayList<>();
            for (var vector : root.getFieldVectors()) fields.add(vector.getObject(row).toString());
            actual.add(String.join(":", fields));
          }
        }
      }
      assertTrue(reader.filesOpened() >= 2);
      output.close();
      var files = output.result();
      assertEquals(1, files.size(), "compaction rolls by file size across input batches");
      var file = files.get(0);
      assertEquals(actual.size(), file.rowCount());
      assertEquals(2, file.level());
      assertEquals(FileSource.COMPACT, file.fileSource().orElseThrow());
      List<String> reopened = new ArrayList<>();
      var factory =
          ((KeyValueFileStore) table.store())
              .newReaderFactoryBuilder()
              .buildWithoutDeletionVector(first.partition(), first.bucket());
      try (var records = new RecordReaderIterator<KeyValue>(factory.createRecordReader(file))) {
        while (records.hasNext()) {
          var row = records.next();
          reopened.add(
              row.key().getInt(0)
                  + ":"
                  + row.sequenceNumber()
                  + ":"
                  + row.valueKind().toByteValue()
                  + ":"
                  + row.value().getInt(0)
                  + ":"
                  + row.value().getInt(1));
        }
      }
      assertEquals(expected, reopened);
    }
    assertEquals(dropDelete ? 2 : 5, actual.size());
    assertEquals(expected, actual);
    List<NativePaimonCompactRewriter> rewriters = new ArrayList<>();
    var maintenance = table.copy(Map.of("write-only", "false"));
    try (var spill = IOManager.create(directory.resolve("compact-spill").toString());
        var writer = maintenance.newWrite("compaction")) {
      writer
          .withIOManager(spill)
          .withCompactRewriterFactory(
              (partition, bucket, stock) -> {
                var rewriter =
                    new NativePaimonCompactRewriter(
                        maintenance, partition, bucket, stock, new UnregisteredMetricsGroup());
                rewriters.add(rewriter);
                return rewriter;
              });
      writer.compact(first.partition(), first.bucket(), true);
      var commits = writer.prepareCommit(true, 3);
      assertFalse(commits.isEmpty());
      assertEquals(1, rewriters.size());
      assertEquals(1, rewriters.get(0).nativeGroups.getCount());
      assertEquals(0, rewriters.get(0).stockGroups.getCount());
      assertEquals(2, rewriters.get(0).rows.getCount());
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retainedTombstoneBeatsAnOlderFilePublishedAfterCompaction(boolean thin) throws Exception {
    for (boolean nativeMode : List.of(false, true)) {
      var table = smallTable("late-" + nativeMode, thin);
      List<CommitMessage> older;
      try (var write = table.newWrite("older")) {
        write.write(GenericRow.of(1, 100));
        older = write.prepareCommit(true, 0);
      }
      try (var write = table.newWrite("current");
          var commit = table.newCommit("current")) {
        write.write(GenericRow.of(2, 200));
        write.write(GenericRow.ofKind(RowKind.DELETE, 1, 100));
        commit.commit(0, write.prepareCommit(true, 0));
        write.write(GenericRow.of(2, 201));
        commit.commit(1, write.prepareCommit(true, 1));
      }
      var selected = table.copy(Map.of("write-only", "false"));
      var files =
          table.newScan().plan().splits().stream()
              .flatMap(split -> ((DataSplit) split).dataFiles().stream())
              .collect(java.util.stream.Collectors.toList());
      var sections = new IntervalPartition(files, PaimonVersion.keyComparator(table)).partition();
      var captured = new AtomicReference<CompactRewriter>();
      try (var spill = IOManager.create(directory.resolve("spill").toString())) {
        var write = ((KeyValueFileStore) selected.store()).newWrite("retain");
        try (AutoCloseable close = write::close) {
          write.withIOManager(spill);
          write.withCompactRewriterFactory(
              (partition, bucket, stock) -> {
                var rewriter =
                    nativeMode
                        ? new NativePaimonCompactRewriter(
                            selected, partition, bucket, stock, new UnregisteredMetricsGroup())
                        : stock;
                captured.set(rewriter);
                return rewriter;
              });
          write.createWriter(BinaryRow.EMPTY_ROW, 0);
          var result = captured.get().rewrite(2, false, sections);
          assertEquals(
              1,
              result.after().stream().mapToLong(file -> file.deleteRowCount().orElseThrow()).sum());
          assertEquals(
              1,
              result.after().stream()
                  .mapToLong(file -> file.minSequenceNumber())
                  .min()
                  .orElseThrow());
          try (var commit = selected.newCommit("retain")) {
            commit.commit(
                0,
                List.of(
                    new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(List.of(), List.of(), List.of()),
                        new CompactIncrement(result.before(), result.after(), List.of()))));
          }
        }
      }
      try (var commit = table.newCommit("older")) {
        commit.commit(0, older);
      }
      assertEquals(List.of("2|201"), PaimonTestTables.readRows(table, table.rowType()));
      compactFully(selected, nativeMode, false);
      assertEquals(List.of("2|201"), PaimonTestTables.readRows(table, table.rowType()));
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deleteOnlyFullCompactionCommitsNoOutputFile(boolean thin) throws Exception {
    for (boolean nativeMode : List.of(false, true)) {
      var table = smallTable("empty-" + nativeMode, thin);
      try (var write = table.newWrite("seed");
          var commit = table.newCommit("seed")) {
        write.write(GenericRow.of(1, 100));
        commit.commit(0, write.prepareCommit(true, 0));
        write.write(GenericRow.ofKind(RowKind.DELETE, 1, 100));
        commit.commit(1, write.prepareCommit(true, 1));
      }
      compactFully(table.copy(Map.of("write-only", "false")), nativeMode, true);
      assertTrue(table.newScan().plan().splits().isEmpty());
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  private FileStoreTable smallTable(String name, boolean thin) throws Exception {
    var path = new Path(directory.resolve(name).toUri());
    var io = LocalFileIO.create();
    new FileSystemSchemaManager(io, path)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("v", DataTypes.INT())
                .primaryKey("id")
                .option("bucket", "1")
                .option("file.format", "parquet")
                .option("data-file.thin-mode", Boolean.toString(thin))
                .option("write-only", "true")
                .build());
    return FileStoreTableFactory.create(io, path);
  }

  private void compactFully(FileStoreTable table, boolean nativeMode, boolean empty)
      throws Exception {
    var rewriters = new ArrayList<NativePaimonCompactRewriter>();
    try (var spill = IOManager.create(directory.resolve("full-spill").toString());
        var write = table.newWrite("full");
        var commit = table.newCommit("full")) {
      write
          .withIOManager(spill)
          .withCompactRewriterFactory(
              (partition, bucket, stock) -> {
                if (!nativeMode) return stock;
                var rewriter =
                    new NativePaimonCompactRewriter(
                        table, partition, bucket, stock, new UnregisteredMetricsGroup());
                rewriters.add(rewriter);
                return rewriter;
              });
      write.compact(BinaryRow.EMPTY_ROW, 0, true);
      commit.commit(0, write.prepareCommit(true, 0));
    }
    if (nativeMode) {
      assertEquals(1, rewriters.stream().mapToLong(r -> r.nativeGroups.getCount()).sum());
      assertEquals(0, rewriters.stream().mapToLong(r -> r.stockGroups.getCount()).sum());
      assertEquals(empty ? 0 : 1, rewriters.stream().mapToLong(r -> r.rows.getCount()).sum());
    }
  }
}
