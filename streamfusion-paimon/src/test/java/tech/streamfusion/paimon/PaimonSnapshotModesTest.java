package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.collect;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.stock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonSnapshotModesTest {
  static Stream<Arguments> modes() {
    return Stream.of("deduplicate", "first-row", "ascending", "descending")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        dynamic ->
                            Stream.of(false, true)
                                .map(ignore -> Arguments.of(mode, dynamic, ignore))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void mergeModesMatchStockAcrossProjectionAndRestore(
      String mode, boolean dynamic, boolean ignoreDelete) throws Exception {
    boolean sequence = mode.equals("ascending") || mode.equals("descending");
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "seq", DataTypes.DOUBLE()),
                new DataField(2, "seq2", DataTypes.INT()),
                new DataField(3, "v", DataTypes.STRING())));
    var options = new java.util.HashMap<String, String>();
    options.put("write-only", "true");
    options.put("changelog-producer", "none");
    if (sequence) {
      options.put("sequence.field", "seq,seq2");
      options.put("sequence.field.sort-order", mode);
    }
    options.put("bucket", dynamic ? "-1" : "2");
    options.put("merge-engine", mode.equals("first-row") ? mode : "deduplicate");
    if (mode.equals("first-row")) options.put("ignore-delete", Boolean.toString(ignoreDelete));
    // Deduplication writes stored retracts before enabling ignore-delete, as in historical files.
    var table = PaimonMergeEngineTest.table(options, type);
    assertEquals(dynamic ? BucketMode.HASH_DYNAMIC : BucketMode.HASH_FIXED, table.bucketMode());
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    Double[] sequences = {
      null,
      -0.0d,
      0.0d,
      Double.NEGATIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.NaN,
      Double.longBitsToDouble(0xfff8000000000001L)
    };
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int checkpoint = 1; checkpoint <= 4; checkpoint++) {
        for (int i = 0; i < 35; i++) {
          var row =
              GenericRow.of(
                  i,
                  sequences[(i + checkpoint) % sequences.length],
                  i % 3 == 0 ? null : checkpoint % 2,
                  BinaryString.fromString("v" + checkpoint + "-" + i));
          if (checkpoint > 1) row.setRowKind(RowKind.UPDATE_AFTER);
          if ((ignoreDelete || !mode.equals("first-row")) && i % 5 == 0) {
            row.setRowKind(checkpoint % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
          }
          writer.write(row, i % 2);
        }
        commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
      }
    }
    table = table.copy(Map.of("ignore-delete", Boolean.toString(ignoreDelete)));
    check(table);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"BOOLEAN", "BIGINT", "FLOAT", "DECIMAL", "STRING", "BINARY", "DATE", "TIMESTAMP"})
  void scalarSequencesOmittedByProjectionMatchStock(String sequenceType) throws Exception {
    org.apache.paimon.types.DataType type;
    switch (sequenceType) {
      case "BOOLEAN":
        type = DataTypes.BOOLEAN();
        break;
      case "BIGINT":
        type = DataTypes.BIGINT();
        break;
      case "FLOAT":
        type = DataTypes.FLOAT();
        break;
      case "DECIMAL":
        type = DataTypes.DECIMAL(38, 2);
        break;
      case "STRING":
        type = DataTypes.STRING();
        break;
      case "BINARY":
        type = DataTypes.VARBINARY(8);
        break;
      case "DATE":
        type = DataTypes.DATE();
        break;
      default:
        type = DataTypes.TIMESTAMP(6);
        break;
    }
    var fields =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "seq", type),
                new DataField(2, "v", DataTypes.STRING())));
    for (String direction : List.of("ascending", "descending")) {
      var table =
          PaimonMergeEngineTest.table(
              Map.of(
                  "write-only",
                  "true",
                  "changelog-producer",
                  "input",
                  "sequence.field",
                  "seq",
                  "sequence.field.sort-order",
                  direction),
              fields);
      var builder = table.newStreamWriteBuilder().withCommitUser("test");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
          for (int i = 0; i < 12; i++) {
            int n = (i + checkpoint) % 5 - 2;
            Object value;
            switch (sequenceType) {
              case "BOOLEAN":
                value = n > 0;
                break;
              case "BIGINT":
                value = (long) n;
                break;
              case "FLOAT":
                value =
                    new Float[] {
                          Float.NaN,
                          Float.intBitsToFloat(0xffc00001),
                          -0.0f,
                          0.0f,
                          Float.POSITIVE_INFINITY
                        }
                        [n + 2];
                break;
              case "DECIMAL":
                value =
                    org.apache.paimon.data.Decimal.fromBigDecimal(
                        java.math.BigDecimal.valueOf(n, 2), 38, 2);
                break;
              case "STRING":
                value = BinaryString.fromString(List.of("é", "😀", "e", "Z", "").get(n + 2));
                break;
              case "BINARY":
                value = new byte[] {(byte) n};
                break;
              case "DATE":
                value = n;
                break;
              default:
                value = org.apache.paimon.data.Timestamp.fromEpochMillis(n, 1000);
                break;
            }
            writer.write(
                GenericRow.of(
                    i,
                    i % 4 == 0 ? null : value,
                    BinaryString.fromString("v" + checkpoint + "-" + i)));
          }
          commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
        }
      }
      check(table);
    }
  }

  private static void check(FileStoreTable table) throws Exception {
    for (int[] projection :
        List.of(
            new int[] {table.rowType().getFieldCount() - 1},
            new int[] {1, table.rowType().getFieldCount() - 1, 0})) {
      var read = table.newReadBuilder().withProjection(projection);
      int merged = 0;
      for (var split : read.newStreamScan().plan().splits()) {
        var data = (DataSplit) split;
        var expected = stock(read, data);
        assertFalse(expected.isEmpty());
        var all = collect(table, read, data, true, 2, 0, Integer.MAX_VALUE);
        assertEquals(expected, all.rows());
        merged += all.merged();
        for (boolean initialNative : new boolean[] {false, true}) {
          var prefix = collect(table, read, data, initialNative, 2, 0, 3);
          var suffix =
              collect(table, read, data, !initialNative, 5, prefix.offset(), Integer.MAX_VALUE);
          var restored = new ArrayList<>(prefix.rows());
          restored.addAll(suffix.rows());
          assertEquals(expected, restored);
        }
      }
      assertTrue(merged > 0, "Must exercise native snapshot merging");
    }
    assertEquals("", NativePaimon.liveNativeHandles());
    assertEquals("", tech.streamfusion.parquet.NativeParquet.liveNativeHandles());
    assertEquals("", tech.streamfusion.orc.NativeOrc.liveNativeHandles());
  }
}
