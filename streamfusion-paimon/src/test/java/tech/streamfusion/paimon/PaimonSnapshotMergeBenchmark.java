package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Includes Java merge-to-Arrow or native merge, file opening, planning, and Arrow import. */
class PaimonSnapshotMergeBenchmark {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "int",
        "decimal",
        "timestamp",
        "binary",
        "date",
        "float",
        "double",
        "first-row",
        "sequence",
        "dynamic",
        "partial-update",
        "partial-delete",
        "partial-ignore"
      })
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_SNAPSHOT_BENCHMARK", matches = "true")
  void compareSnapshotCatchup(String keyType) throws Exception {
    String selected = System.getenv("SF_PAIMON_SNAPSHOT_CASES");
    org.junit.jupiter.api.Assumptions.assumeTrue(
        selected == null || List.of(selected.split(",")).contains(keyType));
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SNAPSHOT_ROWS", "65536"));
    org.apache.paimon.types.DataType selectedValue0;
    switch (keyType) {
      case "decimal":
        selectedValue0 = DataTypes.DECIMAL(38, 2).notNull();
        break;
      case "timestamp":
        selectedValue0 = DataTypes.TIMESTAMP(6).notNull();
        break;
      case "binary":
        selectedValue0 = DataTypes.VARBINARY(4).notNull();
        break;
      case "date":
        selectedValue0 = DataTypes.DATE().notNull();
        break;
      case "float":
        selectedValue0 = DataTypes.FLOAT().notNull();
        break;
      case "double":
        selectedValue0 = DataTypes.DOUBLE().notNull();
        break;
      default:
        selectedValue0 = DataTypes.INT().notNull();
        break;
    }
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", selectedValue0),
                new DataField(1, "v", DataTypes.STRING()),
                new DataField(2, "nested", DataTypes.ARRAY(DataTypes.INT())),
                new DataField(3, "ordinal", DataTypes.INT().notNull())));
    for (int runs : (keyType.equals("int") ? new int[] {1, 4, 8} : new int[] {4})) {
      var options = new java.util.HashMap<String, String>();
      options.put("changelog-producer", "input");
      options.put("write-only", "true");
      options.put(
          "sort-spill-buffer-size",
          System.getenv().getOrDefault("SF_PAIMON_SNAPSHOT_BUDGET", "64 mb"));
      java.util.Map<String, String> selectedValue1;
      switch (keyType) {
        case "first-row":
          selectedValue1 =
              Map.of(
                  "merge-engine",
                  "first-row",
                  "changelog-producer",
                  "none",
                  "ignore-delete",
                  "true");
          break;
        case "sequence":
          selectedValue1 = Map.of("sequence.field", "ordinal");
          break;
        case "dynamic":
          selectedValue1 = Map.of("bucket", "-1");
          break;
        case "partial-update":
          selectedValue1 = Map.of("merge-engine", "partial-update");
          break;
        case "partial-delete":
          selectedValue1 =
              Map.of(
                  "merge-engine",
                  "partial-update",
                  "partial-update.remove-record-on-delete",
                  "true");
          break;
        case "partial-ignore":
          selectedValue1 = Map.of("merge-engine", "partial-update", "ignore-delete", "true");
          break;
        default:
          selectedValue1 = Map.of();
          break;
      }
      options.putAll(selectedValue1);
      var table = PaimonMergeEngineTest.table(options, type);
      var builder = table.newStreamWriteBuilder().withCommitUser("bench");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (int checkpoint = 1; checkpoint <= runs; checkpoint++) {
          for (int i = 0; i < rows; i++) {
            var row =
                GenericRow.of(
                    key(i - rows / 2, keyType),
                    keyType.startsWith("partial") && checkpoint % 2 == 0
                        ? null
                        : BinaryString.fromString("value-" + checkpoint + "-" + i),
                    keyType.startsWith("partial") && checkpoint % 2 != 0
                        ? null
                        : new GenericArray(new Integer[] {checkpoint, null, -i}),
                    (runs - checkpoint) * rows + i);
            if (checkpoint == runs && i % 7 == 0 && !keyType.equals("partial-update")) {
              row.setRowKind(RowKind.DELETE);
            }
            if (keyType.equals("dynamic")) writer.write(row, i % 2);
            else writer.write(row);
          }
          commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
        }
      }
      var read = table.newReadBuilder();
      var splits = read.newStreamScan().plan().splits();
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        long[] expected = null;
        for (int offset = 0; offset < 2; offset++) {
          int nativeRead = (iteration + offset) % 2;
          long count = 0, checksum = 0;
          int merged = 0;
          long start = System.nanoTime();
          for (var split : splits) {
            try (var reader =
                new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)
                    .withNativeSnapshots(nativeRead == 1)) {
              reader.handleSplitsChanges(
                  new SplitsAddition<>(List.of(new FileStoreSourceSplit("split", split))));
              while (true) {
                var fetched = reader.fetch();
                if (fetched.nextSplit() != null) {
                  var record = fetched.nextRecordFromSplit();
                  if (record != null) {
                    try (var root = record.batch().root()) {
                      count += root.getRowCount();
                      var ids = (org.apache.arrow.vector.IntVector) root.getVector(3);
                      for (int row = 0; row < root.getRowCount(); row++) {
                        checksum += ids.get(row);
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
              merged += reader.nativeSnapshotsRead();
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          if (iteration > 0) {
            best[nativeRead] = Math.min(best[nativeRead], seconds);
          }
          if (nativeRead == 1) {
            assertTrue(merged > 0, "Must benchmark native merging");
          }
          if (expected == null) {
            expected = new long[] {count, checksum};
          } else {
            assertArrayEquals(expected, new long[] {count, checksum});
          }
        }
      }
      System.out.printf(
          "PAIMON_SNAPSHOT key=%s rows=%d commits=%d java_arrow_s=%.3f native_arrow_s=%.3f"
              + " speedup=%.2fx budget_bytes=%d%n",
          keyType,
          rows,
          runs,
          best[0],
          best[1],
          best[0] / best[1],
          table.coreOptions().sortSpillBufferSize());
    }
  }

  private static Object key(int i, String type) {
    switch (type) {
      case "decimal":
        return org.apache.paimon.data.Decimal.fromBigDecimal(
            java.math.BigDecimal.valueOf(i, 2), 38, 2);
      case "timestamp":
        return org.apache.paimon.data.Timestamp.fromEpochMillis(
            Math.floorDiv(i, 1000), Math.floorMod(i, 1000) * 1000);
      case "binary":
        return java.nio.ByteBuffer.allocate(4).putInt(i).array();
      case "float":
        return (float) i;
      case "double":
        return (double) i;
      default:
        return i;
    }
  }
}
