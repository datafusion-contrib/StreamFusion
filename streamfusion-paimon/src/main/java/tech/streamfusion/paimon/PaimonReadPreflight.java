package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.table.FileStoreTable;

/** Footer estimates shared by source and compaction admission policies. */
final class PaimonReadPreflight {
  private PaimonReadPreflight() {}

  static boolean overlappingSequences(List<SortedRun> section) {
    var files =
        section.stream()
            .flatMap(run -> run.files().stream())
            .sorted(java.util.Comparator.comparingLong(DataFileMeta::minSequenceNumber))
            .collect(java.util.stream.Collectors.toList());
    long maximum = -1;
    for (DataFileMeta file : files) {
      if (file.minSequenceNumber() <= maximum) return true;
      maximum = Math.max(maximum, file.maxSequenceNumber());
    }
    return false;
  }

  /** The prior append gate, retained for comparisons with the exact previous rewriter source. */
  static boolean rawWithinBudget(
      FileStoreTable table,
      List<DataFileMeta> files,
      Schema input,
      String[] names,
      String bucketPath,
      int batchRows)
      throws IOException {
    long budget = table.coreOptions().sortSpillBufferSize() / 2;
    for (var file : files)
      if (fileMemory(table, file, input, names, bucketPath, batchRows) > budget) return false;
    return true;
  }

  static Estimate appendEstimate(
      FileStoreTable table,
      List<DataFileMeta> files,
      Schema input,
      String[] names,
      String bucketPath,
      int batchRows)
      throws IOException {
    long maximum = 0;
    String largest = "";
    for (var file : files) {
      if (Thread.currentThread().isInterrupted())
        throw new java.io.InterruptedIOException("Paimon compact preflight cancelled");
      long bytes = fileMemory(table, file, input, names, bucketPath, batchRows);
      if (bytes < 0) bytes = Long.MAX_VALUE;
      if (bytes >= maximum) {
        maximum = bytes;
        largest = file.externalPath().orElse(bucketPath + "/" + file.fileName());
      }
    }
    return new Estimate(largest, maximum);
  }

  static final class Estimate {
    final String file;
    final long bytes;

    Estimate(String file, long bytes) {
      this.file = file;
      this.bytes = bytes;
    }
  }

  private static long fileMemory(
      FileStoreTable table,
      DataFileMeta file,
      Schema input,
      String[] names,
      String bucketPath,
      int batchRows)
      throws IOException {
    try (var reader =
        new NativePaimonFileReader(
            PaimonCodecs.reader(
                file.fileFormat(),
                PaimonVersion.legacyOrcTimestamp(table.coreOptions().toConfiguration())),
            table.fileIO(),
            new Path(file.externalPath().orElse(bucketPath + "/" + file.fileName())),
            file.fileSize(),
            input,
            names,
            batchRows)) {
      return reader.readerMemory();
    }
  }

  static boolean withinBudget(
      FileStoreTable table,
      List<List<SortedRun>> sections,
      Schema input,
      String[] names,
      String bucketPath,
      int batchRows)
      throws IOException {
    long budget = table.coreOptions().sortSpillBufferSize();
    for (List<SortedRun> section : sections) {
      long total = 0;
      for (SortedRun run : section) {
        long maximum = 0;
        for (var file : run.files()) {
          maximum = Math.max(maximum, fileMemory(table, file, input, names, bucketPath, batchRows));
        }
        if (maximum > budget / 2 - total) return false;
        total += maximum;
      }
    }
    return true;
  }
}
