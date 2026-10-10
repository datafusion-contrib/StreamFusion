package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.collect;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.stock;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.parquet.NativeParquet;

class PaimonSnapshotKeyTypesTest {
  static Stream<Arguments> keyTypes() {
    List<Arguments> cases = new ArrayList<>();
    add(cases, DataTypes.BOOLEAN(), List.of(true, false));
    add(cases, DataTypes.TINYINT(), List.of((byte) 127, (byte) -128, (byte) 0, (byte) -1));
    add(cases, DataTypes.SMALLINT(), List.of((short) 32767, (short) -32768, (short) 0, (short) -1));
    add(cases, DataTypes.INT(), List.of(Integer.MAX_VALUE, Integer.MIN_VALUE, 0, -1));
    add(cases, DataTypes.BIGINT(), List.of(Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L));
    for (int precision : new int[] {9, 18, 38}) {
      var maximum = BigDecimal.TEN.pow(precision).subtract(BigDecimal.ONE).movePointLeft(2);
      add(
          cases,
          DataTypes.DECIMAL(precision, 2),
          Stream.of(
                  maximum,
                  maximum.negate(),
                  new BigDecimal("-0.01"),
                  BigDecimal.ZERO,
                  new BigDecimal("0.01"))
              .map(v -> Decimal.fromBigDecimal(v, precision, 2))
              .collect(ListCollectors.toList()));
    }
    add(
        cases,
        DataTypes.CHAR(4),
        Stream.of("é   ", "Z   ", "😀   ", "e   ", "    ")
            .map(BinaryString::fromString)
            .collect(ListCollectors.toList()));
    add(
        cases,
        DataTypes.VARCHAR(20),
        Stream.of("é", "Z", "😀", "e", "", "e ", "ee")
            .map(BinaryString::fromString)
            .collect(ListCollectors.toList()));
    add(
        cases,
        DataTypes.BINARY(2),
        List.of(
            new byte[] {-1, 0},
            new byte[] {0, 0},
            new byte[] {-128, 0},
            new byte[] {127, -1},
            new byte[] {0, 1}));
    add(
        cases,
        DataTypes.VARBINARY(20),
        List.of(
            new byte[] {-1},
            new byte[] {},
            new byte[] {0},
            new byte[] {-128},
            new byte[] {127},
            new byte[] {0, 0}));
    add(cases, DataTypes.DATE(), List.of(20000, -20000, -1, 0, 11016));
    for (int precision = 0; precision <= 6; precision++) {
      long unitMicros = (long) Math.pow(10, 6 - precision);
      var values =
          Stream.of(123456789000000L, -123456789000000L, -unitMicros, 0L, unitMicros)
              .map(
                  v ->
                      Timestamp.fromEpochMillis(
                          Math.floorDiv(v, 1000), (int) Math.floorMod(v, 1000) * 1000))
              .collect(ListCollectors.toList());
      add(cases, DataTypes.TIMESTAMP(precision), values);
      add(cases, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision), values);
      if (precision > 0) {
        var positive =
            values.stream()
                .map(
                    v ->
                        Timestamp.fromEpochMillis(
                            v.getMillisecond() + 1_000_000_000_000L, v.getNanoOfMillisecond()))
                .collect(ListCollectors.toList());
        for (boolean nativeWriter : new boolean[] {false, true}) {
          cases.add(Arguments.of(DataTypes.TIMESTAMP(precision), positive, nativeWriter, "orc"));
          cases.add(
              Arguments.of(
                  DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision),
                  positive,
                  nativeWriter,
                  "orc"));
        }
      }
    }
    for (String format : List.of("parquet", "orc")) {
      cases.add(
          Arguments.of(
              DataTypes.FLOAT(),
              List.of(
                  Float.NaN,
                  Float.intBitsToFloat(0xffc00001),
                  Float.intBitsToFloat(0x7fc00001),
                  Float.NEGATIVE_INFINITY,
                  -Float.MAX_VALUE,
                  -0.0f,
                  0.0f,
                  Float.MIN_VALUE,
                  Float.MAX_VALUE,
                  Float.POSITIVE_INFINITY),
              false,
              format));
      cases.add(
          Arguments.of(
              DataTypes.DOUBLE(),
              List.of(
                  Double.NaN,
                  Double.longBitsToDouble(0xfff8000000000001L),
                  Double.longBitsToDouble(0x7ff8000000000001L),
                  Double.NEGATIVE_INFINITY,
                  -Double.MAX_VALUE,
                  -0.0d,
                  0.0d,
                  Double.MIN_VALUE,
                  Double.MAX_VALUE,
                  Double.POSITIVE_INFINITY),
              false,
              format));
    }
    return cases.stream();
  }

  private static void add(List<Arguments> cases, DataType type, List<?> values) {
    for (boolean nativeWriter : new boolean[] {false, true}) {
      for (String format : List.of("parquet", "orc"))
        cases.add(Arguments.of(type, values, nativeWriter, format));
    }
  }

  @ParameterizedTest(name = "{0}, native writer={2}, format={3}")
  @MethodSource("keyTypes")
  void scalarCompositeKeysMatchJavaAcrossProjectionAndRestore(
      DataType keyType, List<?> keys, boolean nativeWriter, String format) throws Exception {
    var fields =
        new RowType(
            List.of(
                new DataField(0, "pt", DataTypes.STRING().notNull()),
                new DataField(1, "id", keyType.notNull()),
                new DataField(2, "k", DataTypes.INT().notNull()),
                new DataField(3, "v", DataTypes.ARRAY(DataTypes.INT()))));
    var io = LocalFileIO.create();
    var path = new Path(Files.createTempDirectory("snapshot-key-types").toUri());
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
                    format,
                    "write-only",
                    "true",
                    "changelog-producer",
                    "input"),
                null));
    var table = FileStoreTableFactory.create(io, path);
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 3)) {
      for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
        for (String partition : List.of("p0", "p1")) {
          List<InternalRow> rows = new ArrayList<>();
          for (int i = 0; i < keys.size() * 3; i++) {
            int ordinal = checkpoint % 2 == 0 ? keys.size() * 3 - i - 1 : i;
            var row =
                GenericRow.of(
                    BinaryString.fromString(partition),
                    keys.get(ordinal / 3),
                    ordinal % 3,
                    ordinal % 5 == 0
                        ? null
                        : new GenericArray(new Integer[] {checkpoint, null, -ordinal}));
            row.setRowKind(
                checkpoint == 1
                    ? RowKind.INSERT
                    : checkpoint == 3 && ordinal % 4 == 0 ? RowKind.DELETE : RowKind.UPDATE_AFTER);
            rows.add(row);
          }
          writer.write(rows);
        }
        writer.commit(checkpoint);
      }
    }
    for (int[] projection : List.of(new int[] {3, 1, 0}, new int[] {3, 0})) {
      var read = table.newReadBuilder().withProjection(projection);
      int merged = 0;
      for (var split : read.newStreamScan().plan().splits()) {
        var data = (DataSplit) split;
        var expected = stock(read, data);
        assertFalse(expected.isEmpty());
        var result = collect(table, read, data, true, 2, 0, Integer.MAX_VALUE);
        assertEquals(expected, result.rows());
        merged += result.merged();
        if (projection.length == 3) {
          for (boolean initialNative : new boolean[] {false, true}) {
            var prefix = collect(table, read, data, initialNative, 1, 0, 1);
            var suffix =
                collect(table, read, data, !initialNative, 3, prefix.offset(), Integer.MAX_VALUE);
            var restored = new ArrayList<>(prefix.rows());
            restored.addAll(suffix.rows());
            assertEquals(expected, restored);
          }
        }
      }
      int precision =
          keyType instanceof org.apache.paimon.types.TimestampType
              ? ((org.apache.paimon.types.TimestampType) keyType).getPrecision()
              : keyType instanceof org.apache.paimon.types.LocalZonedTimestampType
                  ? ((org.apache.paimon.types.LocalZonedTimestampType) keyType).getPrecision()
                  : 0;
      boolean orderingRisk =
          format.equals("orc")
              && precision > 0
              && keys.stream().anyMatch(k -> ((Timestamp) k).getMillisecond() < 0);
      assertEquals(
          orderingRisk ? 0 : 2,
          merged,
          "Native merging requires ordered decoded keys; ORC's last negative second retains Java");
    }
    assertEquals("", NativePaimon.liveNativeHandles());
    assertEquals("", NativeParquet.liveNativeHandles());
    assertEquals("", tech.streamfusion.orc.NativeOrc.liveNativeHandles());
  }

  static Stream<Arguments> unsupportedKeys() {
    return Stream.of(
        Arguments.of(DataTypes.TIMESTAMP(9), List.of(Timestamp.fromEpochMillis(0, 1))),
        Arguments.of(
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9), List.of(Timestamp.fromEpochMillis(0, 1))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unsupportedKeys")
  void unsupportedKeyOrderingOrPrecisionRetainsJava(DataType keyType, List<?> keys)
      throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", keyType.notNull()), new DataField(1, "v", DataTypes.INT())));
    var table =
        PaimonMergeEngineTest.table(
            Map.of("write-only", "true", "changelog-producer", "input"), type);
    var builder = table.newStreamWriteBuilder().withCommitUser("test");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
        for (Object key : keys) {
          writer.write(GenericRow.of(key, checkpoint));
        }
        commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
      }
    }
    var read = table.newReadBuilder().withProjection(new int[] {1});
    for (var split : read.newStreamScan().plan().splits()) {
      var data = (DataSplit) split;
      assertFalse(data.rawConvertible());
      assertNull(
          NativePaimonSnapshotReader.create(
              table, data, LogicalTypeConversion.toLogicalType(read.readType()), 2));
      var result = collect(table, read, data, true, 2, 0, Integer.MAX_VALUE);
      assertEquals(stock(read, data), result.rows());
      assertEquals(0, result.merged());
    }
  }
}
