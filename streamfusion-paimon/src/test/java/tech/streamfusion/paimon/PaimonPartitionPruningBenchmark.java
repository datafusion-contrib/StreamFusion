package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Scan planning plus production file reads; table writes and commits are outside timing. */
@EnabledIfEnvironmentVariable(named = "SF_PAIMON_PARTITION_PRUNING_BENCHMARK", matches = "true")
class PaimonPartitionPruningBenchmark {
  private static final int PARTITIONS = 32;
  private static final String SELECTED = "p00";
  private enum Mode { NATIVE_FULL, NATIVE_PRUNED, STOCK_PRUNED }

  @Test
  void comparePartitionPredicateHandoff() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_PARTITION_ROWS", "131072"));
    int repetitions = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_PARTITION_REPEATS", "3"));
    assertTrue(rows > 0 && rows % PARTITIONS == 0 && repetitions > 0);
    for (int width : new int[] {64, 256}) {
      Path directory = Files.createTempDirectory("paimon-partition-pruning");
      try {
        FileStoreTable table = create(directory, rows, width);
        for (int iteration = -1; iteration < repetitions; iteration++) {
          for (int offset = 0; offset < Mode.values().length; offset++) {
            Mode mode = Mode.values()[Math.floorMod(iteration + offset, Mode.values().length)];
            measure(table, rows, width, mode, iteration);
          }
        }
      } finally {
        try (var files = Files.walk(directory)) {
          for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(file);
        }
      }
    }
  }

  static FileStoreTable create(Path directory, int rows, int width) throws Exception {
    var path = new org.apache.paimon.fs.Path(directory.toUri());
    var schema = Schema.newBuilder().column("id", DataTypes.BIGINT().notNull())
        .column("pt", DataTypes.STRING().notNull()).column("payload", DataTypes.STRING())
        .partitionKeys("pt").options(Map.of("bucket", "-1", "file.format", "parquet",
            "write-only", "true")).build();
    new SchemaManager(LocalFileIO.create(), path).createTable(schema);
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
    var builder = table.newStreamWriteBuilder().withCommitUser("pruning-benchmark");
    var payload = BinaryString.fromString("x".repeat(width));
    try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
      for (int row = 0; row < rows; row++) {
        writer.write(GenericRow.of((long) row,
            BinaryString.fromString(String.format(java.util.Locale.ROOT, "p%02d", row % PARTITIONS)),
            payload));
      }
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    return table;
  }

  private static void measure(FileStoreTable table, int rows, int width, Mode mode, int iteration)
      throws Exception {
    var read = table.newReadBuilder().withProjection(new int[] {0, 1, 2}).dropStats();
    if (mode != Mode.NATIVE_FULL) {
      read.withFilter(new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString(SELECTED)));
    }
    long decoded = 0;
    long selected = 0;
    long checksum = 0;
    long fileBytes = 0;
    long files = 0;
    long nativeFiles = 0;
    long started = System.nanoTime();
    var splits = read.newStreamScan().plan().splits();
    for (int index = 0; index < splits.size(); index++) {
      var split = (DataSplit) splits.get(index);
      files += split.dataFiles().size();
      for (var file : split.dataFiles()) fileBytes += file.fileSize();
      if (mode == Mode.STOCK_PRUNED) {
        try (var reader = read.newRead().createReader(split)) {
          RecordReader.RecordIterator<InternalRow> batch;
          while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
              decoded++;
              if (row.getString(1).toString().equals(SELECTED)) {
                selected++;
                checksum += row.getLong(0);
              }
            }
            batch.releaseBatch();
          }
        }
      } else {
        try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)) {
          reader.handleSplitsChanges(new SplitsAddition<>(List.of(
              new FileStoreSourceSplit("partition-" + index, split))));
          boolean finished = false;
          while (!finished) {
            var fetched = reader.fetch();
            try {
              if (fetched.nextSplit() != null) {
                var record = fetched.nextRecordFromSplit();
                if (record != null) {
                  try (var root = record.batch().root()) {
                    var ids = (BigIntVector) root.getVector(0);
                    var partitions = (VarCharVector) root.getVector(1);
                    decoded += root.getRowCount();
                    for (int row = 0; row < root.getRowCount(); row++) {
                      if (SELECTED.equals(partitions.getObject(row).toString())) {
                        selected++;
                        checksum += ids.get(row);
                      }
                    }
                  }
                }
              }
              finished = !fetched.finishedSplits().isEmpty();
            } finally {
              fetched.recycle();
            }
          }
          nativeFiles += reader.nativeFilesRead();
        }
      }
    }
    long nanos = System.nanoTime() - started;
    long expected = rows / PARTITIONS;
    assertEquals(expected, selected);
    assertEquals(PARTITIONS * expected * (expected - 1) / 2, checksum);
    assertEquals(mode == Mode.NATIVE_FULL ? rows : expected, decoded);
    if (mode != Mode.STOCK_PRUNED) assertEquals(files, nativeFiles, "must run native file decoding");
    if (iteration >= 0) {
      System.out.printf(java.util.Locale.ROOT,
          "PAIMON_PARTITION_PRUNING mode=%s rows=%d partitions=%d payload_bytes=%d iteration=%d "
              + "read_ns=%d splits=%d planned_files=%d planned_file_bytes=%d decoded_rows=%d "
              + "selected_rows=%d checksum=%d native_files=%d%n",
          mode, rows, PARTITIONS, width, iteration, nanos, splits.size(), files, fileBytes,
          decoded, selected, checksum, nativeFiles);
    }
  }
}
