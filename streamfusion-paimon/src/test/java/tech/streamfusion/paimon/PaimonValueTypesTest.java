package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.collect;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.stock;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.ListCollectors;

/** Scalar edge values through actual native files and the released Java readers/statistics. */
class PaimonValueTypesTest {
  static Stream<Arguments> values() {
    List<Arguments> result = new ArrayList<>();
    add(result, DataTypes.BOOLEAN(), List.of(true, false));
    add(result, DataTypes.TINYINT(), List.of(Byte.MIN_VALUE, (byte) 0, Byte.MAX_VALUE));
    add(result, DataTypes.SMALLINT(), List.of(Short.MIN_VALUE, (short) 0, Short.MAX_VALUE));
    add(result, DataTypes.INT(), List.of(Integer.MIN_VALUE, 0, Integer.MAX_VALUE));
    add(result, DataTypes.BIGINT(), List.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE));
    add(
        result,
        DataTypes.FLOAT(),
        List.of(
            Float.MIN_VALUE,
            -Float.MAX_VALUE,
            -0f,
            0f,
            Float.NaN,
            Float.NEGATIVE_INFINITY,
            Float.POSITIVE_INFINITY));
    add(
        result,
        DataTypes.DOUBLE(),
        List.of(
            Double.MIN_VALUE,
            -Double.MAX_VALUE,
            -0d,
            0d,
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY));
    add(result, DataTypes.DOUBLE(), List.of(Double.NaN, Double.NaN));
    add(result, DataTypes.FLOAT(), List.of(-0f, 0f));
    add(result, DataTypes.FLOAT(), List.of(0f, -0f));
    add(result, DataTypes.DOUBLE(), List.of(-0d, 0d));
    add(result, DataTypes.DOUBLE(), List.of(0d, -0d));
    for (int precision : List.of(9, 18, 38)) {
      var maximum = new BigDecimal("9".repeat(precision - 2) + ".99");
      add(
          result,
          DataTypes.DECIMAL(precision, 2),
          List.of(
              Decimal.fromBigDecimal(maximum, precision, 2),
              Decimal.fromBigDecimal(maximum.negate(), precision, 2),
              Decimal.fromBigDecimal(BigDecimal.ZERO, precision, 2)));
    }
    add(
        result,
        DataTypes.CHAR(4),
        Stream.of("a", "é😀", "x   ", "abcd")
            .map(BinaryString::fromString)
            .collect(ListCollectors.toList()));
    add(
        result,
        DataTypes.VARCHAR(8),
        Stream.of("", "é😀", "prefix ")
            .map(BinaryString::fromString)
            .collect(ListCollectors.toList()));
    add(result, DataTypes.BINARY(3), List.of(new byte[] {0, 0, 0}, new byte[] {-1, 0, 127}));
    add(
        result,
        DataTypes.VARBINARY(20),
        List.of(new byte[] {}, new byte[] {-1}, new byte[] {0, 0, 0}));
    add(result, DataTypes.DATE(), List.of(-200000, -1, 0, 200000));
    return result.stream();
  }

  private static void add(List<Arguments> result, DataType type, List<?> values) {
    for (String format : List.of("parquet", "orc")) result.add(Arguments.of(type, values, format));
  }

  @ParameterizedTest(name = "{0}, {2}")
  @MethodSource("values")
  void valuesAndStatisticsMatchTheStockWriter(DataType valueType, List<?> values, String format)
      throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "value", valueType)));
    var options = Map.of("file.format", format, "write-only", "true");
    var stock = PaimonMergeEngineTest.table(options, type);
    var nativeTable = PaimonMergeEngineTest.table(options, type);
    List<InternalRow> input = new ArrayList<>();
    input.add(GenericRow.of(0, null));
    for (int i = 0; i < values.size(); i++) input.add(GenericRow.of(i + 1, values.get(i)));
    for (boolean nativeWriter : new boolean[] {false, true}) {
      var table = nativeWriter ? nativeTable : stock;
      try (var writer =
          new PaimonMergeEngineTest.Writer(
              table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 3)) {
        for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
          writer.write(input);
          writer.commit(checkpoint);
        }
      }
    }
    assertEquals(
        PaimonTestTables.readRows(stock, type), PaimonTestTables.readRows(nativeTable, type));
    for (var table : List.of(stock, nativeTable)) {
      var read = table.newReadBuilder();
      for (var split : read.newStreamScan().plan().splits()) {
        var data = (DataSplit) split;
        var decoded = collect(table, read, data, true, 2, 0, Integer.MAX_VALUE);
        assertEquals(stock(read, data), decoded.rows());
        assertTrue(decoded.merged() > 0, "scalar values must use native snapshot decoding");
      }
    }
    var expected = PaimonTestTables.dataFiles(stock);
    var actual = PaimonTestTables.dataFiles(nativeTable);
    assertEquals(expected.keySet(), actual.keySet());
    for (var key : expected.keySet()) {
      assertEquals(expected.get(key).size(), actual.get(key).size());
      for (int i = 0; i < expected.get(key).size(); i++)
        assertEquals(
            PaimonTestTables.describe(expected.get(key).get(i), type),
            PaimonTestTables.describe(actual.get(key).get(i), type));
    }
  }
}
