package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.parquet.NativeParquet;

class PaimonSnapshotMergeTest {
  @Test
  void multipleFilesPerRunSectionsAndCancellation() throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", DataTypes.STRING())));
    var table =
        PaimonMergeEngineTest.table(
            Map.of("changelog-producer", "input", "write-only", "true"), type);
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    int[][] keys = {{0, 1, 2, 3}, {8, 9, 10, 11}, {0, 1, 2, 3, 8, 9, 10, 11}, {100, 101, 102, 103}};
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int i = 0; i < keys.length; i++) {
        for (int key : keys[i]) {
          writer.write(GenericRow.of(key, BinaryString.fromString("v" + i)));
        }
        commit.commit(i + 1, writer.prepareCommit(true, i + 1));
      }
    }
    var read = table.newReadBuilder().withProjection(new int[] {1});
    boolean multipleFiles = false, multipleSections = false, cancelled = false;
    for (var split : read.newStreamScan().plan().splits()) {
      var data = (DataSplit) split;
      var sections =
          new IntervalPartition(
                  data.dataFiles(), ((PrimaryKeyFileStoreTable) table).store().newKeyComparator())
              .partition();
      multipleSections |= sections.size() > 1;
      multipleFiles |= sections.stream().flatMap(List::stream).anyMatch(r -> r.files().size() > 1);
      assertEquals(
          stock(read, data), collect(table, read, data, true, 2, 0, Integer.MAX_VALUE).rows);
      try (var snapshot =
          NativePaimonSnapshotReader.create(
              table, data, LogicalTypeConversion.toLogicalType(read.readType()), 1)) {
        if (snapshot != null) {
          try (var first = snapshot.next()) {
            assertNotNull(first);
          }
          Thread.currentThread().interrupt();
          try {
            assertThrows(
                InterruptedIOException.class,
                () -> {
                  VectorSchemaRoot batch;
                  while ((batch = snapshot.next()) != null) {
                    batch.close();
                  }
                });
            cancelled = true;
          } finally {
            Thread.interrupted();
          }
        }
      }
    }
    assertTrue(multipleFiles);
    assertTrue(multipleSections);
    assertTrue(cancelled);
    assertEquals("", NativePaimon.liveNativeHandles());
    assertEquals("", NativeParquet.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void overlappingSnapshotsMatchStockAndRestoreAcrossJavaAndNative(boolean nativeWriter)
      throws Exception {
    FileStoreTable table =
        PaimonMergeEngineTest.table(Map.of("changelog-producer", "input", "write-only", "true"));
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
      for (int i = 1; i <= 3; i++) {
        writer.write(PaimonMergeEngineTest.rows(i * 120, false));
        writer.commit(i);
      }
    }
    var read = table.newReadBuilder().withProjection(new int[] {4, 7, 6, 8, 0});
    int merged = 0;
    for (var split : read.newStreamScan().plan().splits()) {
      DataSplit data = (DataSplit) split;
      List<String> expected = stock(read, data);
      Result all = collect(table, read, data, true, 3, 0, Integer.MAX_VALUE);
      assertEquals(expected, all.rows);
      merged += all.merged;
      for (boolean initialNative : new boolean[] {false, true}) {
        Result prefix = collect(table, read, data, initialNative, 2, 0, 1);
        Result suffix =
            collect(table, read, data, !initialNative, 5, prefix.offset, Integer.MAX_VALUE);
        List<String> restored = new ArrayList<>(prefix.rows);
        restored.addAll(suffix.rows);
        assertEquals(expected, restored);
      }
    }
    assertTrue(merged > 0, "Must exercise the snapshot merger, not only native raw reads");
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "complex-sequence",
        "partial-groups",
        "partial-aggregate",
        "aggregate",
        "memory",
        "fan-in"
      })
  void unverifiedSnapshotsRetainJava(String reason) throws Exception {
    Map<String, String> extra;
    switch (reason) {
      case "complex-sequence":
        extra = Map.of("sequence.field", "nested");
        break;
      case "partial-groups":
        extra =
            Map.of(
                "merge-engine",
                "partial-update",
                "ignore-delete",
                "true",
                "fields.seq.sequence-group",
                "txt");
        break;
      case "partial-aggregate":
        extra =
            Map.of(
                "merge-engine",
                "partial-update",
                "ignore-delete",
                "true",
                "fields.v.aggregate-function",
                "sum",
                "fields.seq.sequence-group",
                "v");
        break;
      case "aggregate":
        extra = Map.of("merge-engine", "aggregation", "ignore-delete", "true");
        break;
      case "memory":
        extra = Map.of("sort-spill-buffer-size", "1 kb");
        break;
      case "fan-in":
        extra = Map.of("sort-spill-threshold", "2");
        break;
      default:
        extra = Map.of();
        break;
    }
    var options = new HashMap<>(extra);
    options.put("changelog-producer", "input");
    options.put("write-only", "true");
    var table = PaimonMergeEngineTest.table(options);
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
      for (int i = 1; i <= 3; i++) {
        writer.write(PaimonMergeEngineTest.rows(i * 60, false));
        writer.commit(i);
      }
    }
    var read = table.newReadBuilder();
    for (var split : read.newStreamScan().plan().splits()) {
      DataSplit data = (DataSplit) split;
      assertNull(
          NativePaimonSnapshotReader.create(
              table, data, LogicalTypeConversion.toLogicalType(read.readType()), 7),
          reason);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compositeKeysAndPartitionColumnsMatchJava(boolean stringKey) throws Exception {
    var fields =
        new RowType(
            List.of(
                new DataField(0, "pt", DataTypes.STRING().notNull()),
                new DataField(
                    1,
                    "id",
                    stringKey ? DataTypes.STRING().notNull() : DataTypes.BIGINT().notNull()),
                new DataField(2, "k", DataTypes.INT().notNull()),
                new DataField(3, "v", DataTypes.ARRAY(DataTypes.INT()))));
    var io = LocalFileIO.create();
    var path = new Path(Files.createTempDirectory("snapshot-keys").toUri());
    new SchemaManager(io, path)
        .createTable(
            new Schema(
                fields.getFields(),
                List.of("pt"),
                List.of("pt", "id", "k"),
                Map.of(
                    "bucket",
                    "1",
                    "file.format",
                    PaimonTestTables.fileFormat(),
                    "write-only",
                    "true",
                    "changelog-producer",
                    "input"),
                null));
    var table = FileStoreTableFactory.create(io, path);
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
        for (int i = 0; i < 40; i++) {
          Object id =
              stringKey
                  ? BinaryString.fromString(List.of("é", "Z", "😀", "e", "").get(i % 5))
                  : (long) i - 20;
          var row =
              GenericRow.of(
                  BinaryString.fromString("p" + (i % 2)),
                  id,
                  i % 3,
                  i % 7 == 0 ? null : new GenericArray(new Integer[] {checkpoint, null, -i}));
          if (checkpoint == 3 && i % 3 == 0) {
            row.setRowKind(RowKind.DELETE);
          }
          writer.write(row);
        }
        commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
      }
    }
    var read = table.newReadBuilder().withProjection(new int[] {3, 1, 0});
    int merged = 0;
    for (var split : read.newStreamScan().plan().splits()) {
      var data = (DataSplit) split;
      var result = collect(table, read, data, true, 3, 0, Integer.MAX_VALUE);
      assertEquals(stock(read, data), result.rows);
      merged += result.merged;
    }
    assertTrue(merged > 0);
  }

  static List<String> stock(ReadBuilder read, DataSplit split) throws Exception {
    List<String> result = new ArrayList<>();
    try (var reader = read.newRead().createReader(split)) {
      reader.forEachRemaining(
          row ->
              result.add(row.getRowKind() + ":" + PaimonTestTables.render(row, read.readType())));
    }
    return result;
  }

  static Result collect(
      FileStoreTable table,
      ReadBuilder read,
      DataSplit split,
      boolean nativeSnapshot,
      int batchSize,
      long skip,
      int maxBatches)
      throws Exception {
    List<String> result = new ArrayList<>();
    long offset = skip;
    try (var reader =
        new NativePaimonSplitReader(table, read, read.newRead(), batchSize, -1)
            .withNativeSnapshots(nativeSnapshot)) {
      reader.handleSplitsChanges(
          new SplitsAddition<>(List.of(new FileStoreSourceSplit("split", split, skip))));
      for (int i = 0; i < maxBatches; i++) {
        var fetched = reader.fetch();
        if (fetched.nextSplit() != null) {
          var record = fetched.nextRecordFromSplit();
          if (record != null) {
            offset = record.nextOffset();
            try (var root = record.batch().root()) {
              for (var row :
                  RowDataArrowConverter.read(
                      root, LogicalTypeConversion.toLogicalType(read.readType()))) {
                result.add(
                    row.getRowKind()
                        + ":"
                        + PaimonTestTables.render(new FlinkRowWrapper(row), read.readType()));
              }
            }
          }
        }
        boolean finished = !fetched.finishedSplits().isEmpty();
        fetched.recycle();
        if (finished) {
          break;
        }
      }
      return new Result(result, offset, reader.nativeSnapshotsRead());
    }
  }

  static final class Result {
    private final List<String> rows;
    private final long offset;
    private final int merged;

    Result(List<String> rows, long offset, int merged) {
      this.rows = rows;
      this.offset = offset;
      this.merged = merged;
    }

    public List<String> rows() {
      return rows;
    }

    public long offset() {
      return offset;
    }

    public int merged() {
      return merged;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Result that = (Result) other;
      return java.util.Objects.equals(rows, that.rows)
          && offset == that.offset
          && merged == that.merged;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(rows);
      result = 31 * result + Long.hashCode(offset);
      result = 31 * result + merged;
      return result;
    }

    @Override
    public String toString() {
      return "Result[rows=" + rows + ", offset=" + offset + ", merged=" + merged + "]";
    }
  }
}
