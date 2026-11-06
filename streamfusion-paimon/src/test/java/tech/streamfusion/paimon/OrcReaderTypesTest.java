package tech.streamfusion.paimon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Retain Java CHAR normalization until the released reader matches it recursively. */
class OrcReaderTypesTest {
  private static BinaryString text(String value) {
    return BinaryString.fromString(value);
  }

  static Stream<Arguments> nestedTypes() {
    return Stream.of(
        Arguments.of(
            DataTypes.ARRAY(DataTypes.CHAR(4)),
            List.of(
                new GenericArray(new Object[] {text("é   "), null, text("    ")}),
                new GenericArray(new Object[0]))),
        Arguments.of(
            DataTypes.MAP(DataTypes.CHAR(4), DataTypes.VARCHAR(20)),
            List.of(
                new GenericMap(Map.of(text("a   "), text("keep   "))), new GenericMap(Map.of()))),
        Arguments.of(
            new RowType(
                List.of(
                    new DataField(5, "c", DataTypes.CHAR(4)),
                    new DataField(6, "v", DataTypes.VARCHAR(20)))),
            List.of(GenericRow.of(text("x   "), text("keep   ")), GenericRow.of(null, null))),
        Arguments.of(
            DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.CHAR(4)))),
            List.of(
                new GenericArray(
                    new Object[] {
                      null,
                      new GenericMap(
                          Map.of(text("k"), new GenericArray(new Object[] {text("a   "), null})))
                    }))));
  }

  @ParameterizedTest
  @MethodSource("nestedTypes")
  void nestedCharsSurviveSnapshotChangelogProjectionAndRestore(DataType valueType, List<?> values)
      throws Exception {
    var type =
        RowType.of(
            new DataField(0, "id", DataTypes.INT().notNull()), new DataField(1, "v", valueType));
    var table =
        PaimonMergeEngineTest.table(
            Map.of("file.format", "orc", "write-only", "true", "changelog-producer", "input"),
            type);
    var read = table.newReadBuilder().withProjection(new int[] {1, 0});
    var scan = read.newStreamScan();
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 3)) {
      List<InternalRow> rows = new ArrayList<>();
      rows.add(GenericRow.of(0, null));
      for (int i = 0; i < values.size(); i++) rows.add(GenericRow.of(i + 1, values.get(i)));
      for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
        writer.write(rows);
        writer.commit(checkpoint);
        PaimonSourceReadTest.assertRead(table, read, scan.plan().splits(), false);
      }
    }
  }
}
