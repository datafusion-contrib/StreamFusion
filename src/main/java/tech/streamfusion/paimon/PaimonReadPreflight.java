package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.table.FileStoreTable;

/** Footer-only admission against the existing projected Parquet/ORC decode budget. */
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
            maximum = Math.max(maximum, reader.readerMemory());
          }
        }
        if (maximum > budget / 2 - total) return false;
        total += maximum;
      }
    }
    return true;
  }
}
