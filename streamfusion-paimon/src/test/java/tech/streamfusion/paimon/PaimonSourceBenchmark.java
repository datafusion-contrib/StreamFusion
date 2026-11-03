package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Committed file reads to Java rows or Arrow, with an id checksum; release builds only. */
class PaimonSourceBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_SHARING_BENCHMARK", matches = "true")
  void compareThreeArrowConsumers() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SOURCE_ROWS", "262144"));
    String format = System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet");
    FileStoreTable table =
        PaimonTestTables.createTable(
            Files.createTempDirectory("paimon-sharing-bench"),
            Map.of("bucket", "-1", "file.format", format));
    var builder = table.newStreamWriteBuilder().withCommitUser("sharing-bench");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (var row : PaimonTestTables.values(rows)) writer.write(PaimonTestTables.paimonRow(row));
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    var read = table.newReadBuilder().withProjection(new int[] {0, 1, 2, 7});
    var splits = read.newStreamScan().plan().splits();
    double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    long[] files = new long[2];
    Long expectedChecksum = null;
    for (int run = 0; run < 4; run++) {
      for (int offset = 0; offset < 2; offset++) {
        int mode = (run + offset) % 2;
        int readers = mode == 0 ? 3 : 1;
        int consumers = mode == 0 ? 1 : 3;
        long count = 0;
        long checksum = 0;
        long nativeFiles = 0;
        long start = System.nanoTime();
        for (int pass = 0; pass < readers; pass++) {
          for (int split = 0; split < splits.size(); split++) {
            try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)) {
              reader.handleSplitsChanges(
                  new SplitsAddition<>(
                      List.of(new FileStoreSourceSplit("file-" + split, splits.get(split)))));
              boolean finished = false;
              while (!finished) {
                var fetched = reader.fetch();
                if (fetched.nextSplit() != null) {
                  var record = fetched.nextRecordFromSplit();
                  if (record != null) {
                    record.batch().shareAcross(consumers);
                    for (int consumer = 0; consumer < consumers; consumer++) {
                      try (var root = record.batch().root()) {
                        count += root.getRowCount();
                        checksum += arrowChecksum(root, false);
                      }
                    }
                  }
                }
                finished = !fetched.finishedSplits().isEmpty();
                fetched.recycle();
              }
              nativeFiles += reader.nativeFilesRead();
            }
          }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        if (run > 0) best[mode] = Math.min(best[mode], seconds);
        files[mode] = nativeFiles;
        assertEquals(3L * rows, count);
        if (expectedChecksum == null) expectedChecksum = checksum;
        else assertEquals(expectedChecksum.longValue(), checksum);
      }
    }
    assertTrue(files[1] > 0);
    assertEquals(3 * files[1], files[0]);
    System.out.printf(
        "PAIMON_SHARING format=%s rows=%d consumers=3 independent_s=%.4f shared_s=%.4f"
            + " speedup=%.2fx independent_files=%d shared_files=%d%n",
        format, rows, best[0], best[1], best[0] / best[1], files[0], files[1]);
  }

  private enum ReadMode {
    JAVA_ROWS,
    JAVA_ARROW,
    NATIVE_ARROW
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_SOURCE_BENCHMARK", matches = "true")
  void compareReleasedReader() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SOURCE_ROWS", "262144"));
    String format = System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet");
    for (boolean primaryKey : new boolean[] {false, true}) {
      FileStoreTable table =
          primaryKey
              ? PaimonMergeEngineTest.table(
                  Map.of(
                      "changelog-producer",
                      "input",
                      "write-only",
                      "true",
                      "scan.mode",
                      "latest",
                      "file.format",
                      format))
              : PaimonTestTables.createTable(
                  Files.createTempDirectory("paimon-source-bench"),
                  Map.of("bucket", "-1", "file.format", format));
      if (primaryKey) {
        try (var initial =
            new PaimonMergeEngineTest.Writer(
                table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
          initial.write(PaimonMergeEngineTest.rows(1, false));
          initial.commit(1);
        }
      }
      var read =
          table
              .newReadBuilder()
              .withProjection(primaryKey ? new int[] {0, 3, 4, 7} : new int[] {0, 1, 2, 7});
      var scan = read.newStreamScan();
      if (primaryKey) {
        scan.plan();
      }
      var builder = table.newStreamWriteBuilder().withCommitUser("bench");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        if (primaryKey) {
          for (var row : PaimonMergeEngineTest.rows(rows, false)) {
            writer.write(row);
          }
        } else {
          for (var row : PaimonTestTables.values(rows)) {
            writer.write(PaimonTestTables.paimonRow(row));
          }
        }
        commit.commit(2, writer.prepareCommit(true, 2));
      }
      var splits = scan.plan().splits();
      double[] best = {
        Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY
      };
      for (int run = 0; run < 4; run++) {
        Long expected = null;
        for (int offset = 0; offset < ReadMode.values().length; offset++) {
          ReadMode mode = ReadMode.values()[(offset + run) % ReadMode.values().length];
          long checksum = 0;
          long count = 0;
          long start = System.nanoTime();
          for (int i = 0; i < splits.size(); i++) {
            if (mode != ReadMode.NATIVE_ARROW) {
              try (var reader = read.newRead().createReader(splits.get(i))) {
                RowType outputType = LogicalTypeConversion.toLogicalType(read.readType());
                RowDataSerializer copy = new RowDataSerializer(outputType);
                List<RowData> pending = new ArrayList<>();
                org.apache.paimon.reader.RecordReader.RecordIterator<
                        org.apache.paimon.data.InternalRow>
                    batch;
                while ((batch = reader.readBatch()) != null) {
                  org.apache.paimon.data.InternalRow row;
                  while ((row = batch.next()) != null) {
                    count++;
                    if (mode == ReadMode.JAVA_ROWS) {
                      checksum += primaryKey ? row.getInt(0) : row.getLong(0);
                    } else {
                      // Match the production fallback's ownership copy before batch conversion.
                      pending.add(copy.copy(new FlinkRowData(row)));
                      if (pending.size() == 4096) {
                        checksum += javaArrowChecksum(pending, outputType, primaryKey);
                        pending.clear();
                      }
                    }
                  }
                  batch.releaseBatch();
                }
                if (!pending.isEmpty()) {
                  checksum += javaArrowChecksum(pending, outputType, primaryKey);
                }
              }
            } else {
              try (var reader =
                  new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)) {
                reader.handleSplitsChanges(
                    new SplitsAddition<>(
                        List.of(new FileStoreSourceSplit("file-" + i, splits.get(i)))));
                boolean finished = false;
                while (!finished) {
                  var fetched = reader.fetch();
                  if (fetched.nextSplit() != null) {
                    var record = fetched.nextRecordFromSplit();
                    if (record != null) {
                      try (var root = record.batch().root()) {
                        count += root.getRowCount();
                        checksum += arrowChecksum(root, primaryKey);
                      }
                    }
                  }
                  finished = !fetched.finishedSplits().isEmpty();
                  fetched.recycle();
                }
                assertTrue(reader.nativeFilesRead() > 0);
              }
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              "PAIMON_SOURCE_SAMPLE format=%s table=%s rows=%d iteration=%d mode=%s seconds=%.6f"
                  + " count=%d checksum=%d%n",
              format,
              primaryKey ? "changelog" : "append",
              rows,
              run,
              mode,
              seconds,
              count,
              checksum);
          if (run > 0) {
            best[mode.ordinal()] = Math.min(best[mode.ordinal()], seconds);
          }
          assertEquals(rows, count);
          if (expected == null) {
            expected = checksum;
          } else {
            assertEquals(expected.longValue(), checksum);
          }
        }
      }
      System.out.printf(
          "PAIMON_SOURCE format=%s mode=%s rows=%d stock_s=%.3f java_arrow_s=%.3f"
              + " native_s=%.3f speedup=%.2fx arrow_speedup=%.2fx%n",
          format,
          primaryKey ? "changelog" : "append",
          rows,
          best[0],
          best[1],
          best[2],
          best[0] / best[2],
          best[1] / best[2]);
    }
  }

  private static long javaArrowChecksum(List<RowData> rows, RowType type, boolean primaryKey) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, primaryKey)) {
      return arrowChecksum(root, primaryKey);
    }
  }

  private static long arrowChecksum(VectorSchemaRoot root, boolean primaryKey) {
    long checksum = 0;
    for (int row = 0; row < root.getRowCount(); row++) {
      checksum +=
          primaryKey
              ? ((IntVector) root.getVector(0)).get(row)
              : ((BigIntVector) root.getVector(0)).get(row);
    }
    return checksum;
  }
}
