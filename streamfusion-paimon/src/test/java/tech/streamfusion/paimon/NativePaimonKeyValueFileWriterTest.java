package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Level-0 files written natively from merged Arrow batches against twins Paimon's merge-tree writer
 * wrote row by row from the same changelog: the merged rows read back, every file's key bounds,
 * sequence range, delete count, statistics, and footer must agree.
 */
class NativePaimonKeyValueFileWriterTest {

  private static final int ROWS = 300;
  private static final int KEYS = 90;
  private static final int PUSH_ROWS = 37;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "default",
        "ignore-delete",
        "single-bucket-zstd",
        "input",
        "input-counts",
        "input-none",
        "input-ignore-delete"
      })
  void nativeLevelZeroFilesMatchStockTwins(String variant) throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("bucket", "2");
    switch (variant) {
      case "default" -> {}
      case "ignore-delete" -> options.put("ignore-delete", "true");
      case "single-bucket-zstd" -> {
        options.put("bucket", "1");
        options.put("file.compression", "zstd");
      }
      case "input", "input-counts", "input-none", "input-ignore-delete" -> {
        options.put("changelog-producer", "input");
        if (variant.equals("input-counts")) {
          options.put("changelog-file.compression", "zstd");
          options.put("changelog-file.stats-mode", "counts");
        } else if (variant.equals("input-none")) {
          options.put("changelog-file.stats-mode", "none");
        } else if (variant.equals("input-ignore-delete")) {
          options.put("ignore-delete", "true");
        }
      }
      default -> throw new IllegalArgumentException(variant);
    }
    List<Object[]> changelog = PaimonTestTables.changelog(ROWS, KEYS);
    FileStoreTable nativeTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-pk-native"), options);
    FileStoreTable twinTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-pk-twin"), options);

    writeNatively(nativeTable, changelog);
    writeStock(twinTable, changelog);

    RowType rowType = nativeTable.rowType();
    RowType keyType = PaimonKeyValueLayout.of(nativeTable).keyType;
    List<String> expectedRows = PaimonTestTables.readRows(twinTable, rowType);
    boolean ignoreDelete = nativeTable.coreOptions().ignoreDelete();
    assertEquals(ignoreDelete, expectedRows.size() == KEYS, "deletes take effect unless ignored");
    assertEquals(expectedRows, PaimonTestTables.readRows(nativeTable, rowType));

    Map<String, List<DataFileMeta>> nativeFiles = allFiles(nativeTable);
    Map<String, List<DataFileMeta>> twinFiles = allFiles(twinTable);
    assertEquals(twinFiles.keySet(), nativeFiles.keySet(), "same partitions and buckets");
    for (String destination : twinFiles.keySet()) {
      List<DataFileMeta> expected = twinFiles.get(destination);
      List<DataFileMeta> actual = nativeFiles.get(destination);
      assertEquals(expected.size(), actual.size(), "files in " + destination);
      for (int i = 0; i < expected.size(); i++) {
        DataFileMeta twin = expected.get(i);
        DataFileMeta ours = actual.get(i);
        assertEquals(twin.rowCount(), ours.rowCount(), destination);
        assertEquals(twin.minSequenceNumber(), ours.minSequenceNumber(), destination);
        assertEquals(twin.maxSequenceNumber(), ours.maxSequenceNumber(), destination);
        assertEquals(twin.deleteRowCount(), ours.deleteRowCount(), destination);
        assertEquals(twin.level(), ours.level(), destination);
        assertEquals(twin.schemaId(), ours.schemaId(), destination);
        assertEquals(twin.fileFormat(), ours.fileFormat(), destination);
        assertEquals(twin.extraFiles(), ours.extraFiles(), destination);
        assertEquals(twin.valueStatsCols(), ours.valueStatsCols(), destination);
        assertEquals(
            PaimonTestTables.describeKeys(twin, keyType),
            PaimonTestTables.describeKeys(ours, keyType),
            destination);
        assertEquals(
            PaimonTestTables.describe(twin, rowType),
            PaimonTestTables.describe(ours, rowType),
            "value statistics in " + destination);
      }
    }
    assertEquals(PaimonTestTables.footers(twinTable), PaimonTestTables.footers(nativeTable));
    if (variant.startsWith("input")) {
      assertEquals(
          PaimonChangelogSinkWriteTest.changelogRows(twinTable),
          PaimonChangelogSinkWriteTest.changelogRows(nativeTable));
      assertEquals(
          ignoreDelete
              ? changelog.stream()
                  .filter(
                      r ->
                          !((org.apache.flink.types.RowKind) r[0])
                              .equals(org.apache.flink.types.RowKind.DELETE))
                  .count()
              : ROWS,
          PaimonChangelogSinkWriteTest.changelogRows(nativeTable).size());
    }
  }

  private static Map<String, List<DataFileMeta>> allFiles(FileStoreTable table) {
    Map<String, List<DataFileMeta>> files = PaimonTestTables.dataFiles(table);
    if (table.coreOptions().changelogProducer()
        == org.apache.paimon.CoreOptions.ChangelogProducer.INPUT) {
      for (var entry :
          table
              .store()
              .newScan()
              .withSnapshot(1)
              .withKind(org.apache.paimon.table.source.ScanMode.CHANGELOG)
              .plan()
              .files()) {
        files
            .computeIfAbsent(
                "changelog/" + entry.partition() + "@" + entry.bucket(), k -> new ArrayList<>())
            .add(entry.file());
      }
    }
    return files;
  }

  @Test
  void filesRollAtTheTargetSizeIntoDisjointAscendingKeyRanges() throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("bucket", "1");
    options.put("target-file-size", "1 kb");
    int rows = 10_000;
    List<Object[]> changelog = PaimonTestTables.changelog(rows, rows);
    FileStoreTable nativeTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-pk-roll"), options);
    FileStoreTable twinTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-pk-roll-twin"), options);

    writeNatively(nativeTable, changelog);
    writeStock(twinTable, changelog);

    RowType rowType = nativeTable.rowType();
    RowType keyType = PaimonKeyValueLayout.of(nativeTable).keyType;
    assertEquals(
        PaimonTestTables.readRows(twinTable, rowType),
        PaimonTestTables.readRows(nativeTable, rowType));
    for (List<DataFileMeta> files : PaimonTestTables.dataFiles(nativeTable).values()) {
      assertTrue(files.size() > 1, "rolled into several files");
      long total = 0;
      String previousMax = null;
      for (DataFileMeta file : files) {
        total += file.rowCount();
        String min = PaimonTestTables.render(file.minKey(), keyType);
        String max = PaimonTestTables.render(file.maxKey(), keyType);
        assertTrue(
            previousMax == null || previousMax.compareTo(min) < 0, "disjoint ascending ranges");
        assertTrue(min.compareTo(max) <= 0);
        previousMax = max;
      }
      assertEquals(files.stream().mapToLong(DataFileMeta::rowCount).sum(), total);
    }
  }

  @Test
  void postponeReplayKeepsFileOrderWhenTheClockStallsAndManifestOrderChanges() throws Exception {
    String prefix = "data-u-writer-s-0-w-";
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-postpone-file-order"),
            Map.of(
                "bucket",
                "-2",
                "target-file-size",
                "1 kb",
                "data-file.prefix",
                prefix,
                "metadata.stats-mode",
                "none"));
    var extractor = new org.apache.paimon.table.sink.RowPartitionKeyExtractor(table.schema());
    List<Object[]> changes = PaimonTestTables.changelog(20_000, KEYS);
    BinaryRow partition =
        extractor.partition(PaimonTestTables.primaryKeyPaimonRow(changes.get(0))).copy();
    List<RowData> rows =
        changes.stream()
            .filter(
                row ->
                    extractor
                        .partition(PaimonTestTables.primaryKeyPaimonRow(row))
                        .equals(partition))
            .map(PaimonTestTables::primaryKeyFlinkRow)
            .toList();
    PaimonKeyValueLayout layout = PaimonKeyValueLayout.of(table);
    long time = org.apache.paimon.data.Timestamp.now().getMillisecond();
    PaimonPostponeFileOrder order =
        new PaimonPostponeFileOrder(table, partition, prefix, () -> time);
    org.apache.paimon.io.DataIncrement increment;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(
                allocator, layout.keyColumns, table.rowType().getFieldCount(), true, false)) {
      buffer.push(
          RowDataArrowConverter.write(
              rows, PaimonTestTables.PRIMARY_KEY_FLINK_TYPE, allocator, true),
          0);
      increment =
          new NativePaimonKeyValueFileWriter(table, layout)
              .write(partition, -2, buffer.flushUnmerged(), order);
    }
    List<DataFileMeta> written = increment.newFiles();
    assertTrue(written.size() > 1, "multiple files described with a fixed wall clock");
    List<DataFileMeta> reversed = new ArrayList<>(written);
    java.util.Collections.reverse(reversed);
    var split =
        org.apache.paimon.table.source.DataSplit.builder()
            .withSnapshot(1)
            .withPartition(partition)
            .withBucket(-2)
            .withBucketPath(table.location().toString())
            .withTotalBuckets(-2)
            .withDataFiles(reversed)
            .isStreaming(false)
            .build();
    assertEquals(
        written,
        org.apache.paimon.table.PostponeUtils.groupPostponeFiles(List.of(split))
            .get(0)
            .dataFiles());
    try (StreamTableCommit commit =
        table.newStreamWriteBuilder().withCommitUser("writer").newCommit()) {
      commit.commit(
          1,
          List.of(
              new CommitMessageImpl(
                  partition, -2, -2, increment, CompactIncrement.emptyIncrement())));
    }
    var restored = new PaimonPostponeFileOrder(table, partition, prefix, () -> time - 1000);
    assertTrue(
        restored.get().compareTo(written.get(written.size() - 1).creationTime()) > 0,
        "recovery continues after committed files even if the clock moves backwards");
  }

  @Test
  void inputChangelogRollsIndependentlyWithoutLosingRepeatedKeys() throws Exception {
    Map<String, String> options =
        Map.of("bucket", "1", "target-file-size", "1 kb", "changelog-producer", "input");
    FileStoreTable nativeTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-input-roll"), options);
    FileStoreTable twinTable =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-input-roll-twin"), options);
    List<Object[]> rows = PaimonTestTables.changelog(10_000, KEYS);
    writeNatively(nativeTable, rows);
    writeStock(twinTable, rows);

    int dataFiles = 0;
    int changelogFiles = 0;
    long changelogRows = 0;
    for (var entry : allFiles(nativeTable).entrySet()) {
      if (entry.getKey().startsWith("changelog/")) {
        changelogFiles += entry.getValue().size();
        changelogRows += entry.getValue().stream().mapToLong(DataFileMeta::rowCount).sum();
      } else {
        dataFiles += entry.getValue().size();
      }
    }
    assertTrue(changelogFiles > dataFiles, "raw changes roll beyond the deduplicated files");
    assertEquals(rows.size(), changelogRows);
    assertEquals(
        PaimonChangelogSinkWriteTest.changelogRows(twinTable),
        PaimonChangelogSinkWriteTest.changelogRows(nativeTable));
    assertEquals(
        PaimonTestTables.readRows(twinTable, twinTable.rowType()),
        PaimonTestTables.readRows(nativeTable, nativeTable.rowType()));
  }

  @Test
  void floatingPointKeysAreDeclinedWhileTheFixtureKeyIsAccepted() throws Exception {
    FileStoreTable accepted =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("paimon-pk-keys"), Map.of("bucket", "1"));
    assertNull(PaimonKeyValueLayout.unsupportedKeyReason(accepted));

    org.apache.paimon.fs.Path path =
        new org.apache.paimon.fs.Path(Files.createTempDirectory("paimon-pk-double").toUri());
    new org.apache.paimon.schema.SchemaManager(
            org.apache.paimon.fs.local.LocalFileIO.create(), path)
        .createTable(
            Schema.newBuilder()
                .column("k", DataTypes.DOUBLE().notNull())
                .column("v", DataTypes.INT())
                .primaryKey("k")
                .option("bucket", "1")
                .build());
    FileStoreTable declined =
        org.apache.paimon.table.FileStoreTableFactory.create(
            org.apache.paimon.fs.local.LocalFileIO.create(), path);
    assertTrue(PaimonKeyValueLayout.unsupportedKeyReason(declined).contains("DOUBLE"));
  }

  /** Routes the changelog like the sink does, buffers per bucket, and commits the native files. */
  private static void writeNatively(FileStoreTable table, List<Object[]> changelog)
      throws Exception {
    StreamTableWrite router = table.newStreamWriteBuilder().withCommitUser("router").newWrite();
    Map<String, List<Object[]>> destinations = new LinkedHashMap<>();
    Map<String, BinaryRow> partitions = new LinkedHashMap<>();
    Map<String, Integer> buckets = new LinkedHashMap<>();
    for (Object[] row : changelog) {
      InternalRow paimonRow = PaimonTestTables.primaryKeyPaimonRow(row);
      BinaryRow partition = router.getPartition(paimonRow);
      int bucket = router.getBucket(paimonRow);
      String key = partition + "@" + bucket;
      destinations.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      partitions.put(key, partition.copy());
      buckets.put(key, bucket);
    }
    router.close();

    PaimonKeyValueLayout layout = PaimonKeyValueLayout.of(table);
    NativePaimonKeyValueFileWriter files = new NativePaimonKeyValueFileWriter(table, layout);
    int kindColumn = table.rowType().getFieldCount();
    List<CommitMessage> messages = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator()) {
      for (String key : destinations.keySet()) {
        List<Object[]> rows = destinations.get(key);
        try (KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(
                allocator,
                layout.keyColumns,
                kindColumn,
                true,
                table.coreOptions().ignoreDelete())) {
          long sequence = 0;
          for (int start = 0; start < rows.size(); start += PUSH_ROWS) {
            List<RowData> slice =
                rows.subList(start, Math.min(rows.size(), start + PUSH_ROWS)).stream()
                    .map(PaimonTestTables::primaryKeyFlinkRow)
                    .collect(Collectors.toList());
            VectorSchemaRoot root =
                RowDataArrowConverter.write(
                    slice, PaimonTestTables.PRIMARY_KEY_FLINK_TYPE, allocator, true);
            sequence += buffer.push(root, sequence);
          }
          KeyedUpsertBuffer.Flushed flushed =
              buffer.flush(
                  table.coreOptions().changelogProducer()
                      == org.apache.paimon.CoreOptions.ChangelogProducer.INPUT);
          if (flushed != null) {
            messages.add(
                new CommitMessageImpl(
                    partitions.get(key),
                    buckets.get(key),
                    table.bucketSpec().getNumBuckets(),
                    files.write(partitions.get(key), buckets.get(key), flushed),
                    CompactIncrement.emptyIncrement()));
          }
        }
      }
    }
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("native").newCommit();
    commit.commit(1, messages);
    commit.close();
  }

  private static void writeStock(FileStoreTable table, List<Object[]> changelog) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("twin").newWrite();
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("twin").newCommit();
    for (Object[] row : changelog) {
      write.write(PaimonTestTables.primaryKeyPaimonRow(row));
    }
    commit.commit(1, write.prepareCommit(false, 1));
    write.close();
    commit.close();
  }
}
