package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.collect;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.stock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
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
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonSnapshotPartialUpdateTest {
  static Stream<Arguments> modes() {
    return Stream.of(
            "deduplicate", "first-row", "partial-update", "partial-ignore", "partial-remove")
        .flatMap(
            mode ->
                Stream.of("parquet", "orc")
                    .flatMap(
                        format ->
                            Stream.of(false, true)
                                .map(dynamic -> Arguments.of(mode, format, dynamic))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void actualSequenceTiesMatchJavaAcrossProjectionAndRestore(
      String mode, String format, boolean dynamic) throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "seq", DataTypes.INT()),
                new DataField(2, "v", DataTypes.ARRAY(DataTypes.INT())),
                new DataField(3, "txt", DataTypes.STRING())));
    var options = new HashMap<String, String>();
    options.put("file.format", format);
    options.put("bucket", dynamic ? "-1" : "1");
    options.put("merge-engine", mode.startsWith("partial") ? "partial-update" : mode);
    options.put("ignore-delete", "false");
    options.put(
        "partial-update.remove-record-on-delete", Boolean.toString(mode.equals("partial-remove")));
    if (dynamic && !mode.equals("first-row")) {
      options.put("sequence.field", "seq");
      options.put("sequence.field.sort-order", "descending");
    }
    var table = PaimonMergeEngineTest.table(options, type);
    var runs = new ArrayList<List<InternalRow>>();
    for (int run = 0; run < 7; run++) {
      var rows = new ArrayList<InternalRow>();
      for (int key = 0; key < 31; key++) {
        // Some fields retain older versions; nullable sequences can be projected away.
        var row =
            GenericRow.of(
                key,
                key % 3 == 0 ? null : run % 2,
                (key + run) % 3 == 0 ? null : new GenericArray(new Integer[] {run, null, key}),
                (key + run) % 2 == 0 ? null : BinaryString.fromString("v" + run + "-" + key));
        row.setRowKind(run % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
        if ((mode.equals("partial-ignore")
                || mode.equals("partial-remove")
                || mode.equals("deduplicate"))
            && (key + run) % 5 == 0)
          row.setRowKind(run % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
        rows.add(row);
      }
      // Singleton keys prove Java's original UPDATE_AFTER kind is retained.
      rows.add(GenericRow.ofKind(RowKind.UPDATE_AFTER, 100 + run, null, null, null));
      runs.add(rows);
    }
    var split = writeRuns(table, runs);
    if (mode.equals("partial-ignore")) {
      assertNull(
          NativePaimonSnapshotReader.create(
              table, split, LogicalTypeConversion.toLogicalType(type), 3),
          "Basic partial updates without a delete policy must retain Java for stored retracts");
      table = table.copy(Map.of("ignore-delete", "true"));
    }
    assertEquals(7, split.dataFiles().size());
    assertEquals(
        1, split.dataFiles().stream().map(DataFileMeta::minSequenceNumber).distinct().count());
    assertEquals(
        1, split.dataFiles().stream().map(DataFileMeta::maxSequenceNumber).distinct().count());
    for (int[] projection :
        List.of(new int[] {0, 1, 2, 3}, new int[] {3, 2}, new int[] {2, 0, 1})) {
      var read = table.newReadBuilder().withProjection(projection);
      var expected = stock(read, split);
      assertFalse(expected.isEmpty());
      var all = collect(table, read, split, true, 3, 0, Integer.MAX_VALUE);
      assertEquals(expected, all.rows());
      assertTrue(all.merged() > 0, "Must enter native snapshot merge");
      for (boolean initialNative : new boolean[] {false, true}) {
        var prefix = collect(table, read, split, initialNative, 2, 0, 5);
        var suffix =
            collect(table, read, split, !initialNative, 7, prefix.offset(), Integer.MAX_VALUE);
        var restored = new ArrayList<>(prefix.rows());
        restored.addAll(suffix.rows());
        assertEquals(expected, restored);
      }
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  static Stream<Arguments> values() {
    var nested = new ArrayList<Arguments>();
    for (String format : List.of("parquet", "orc")) {
      nested.add(
          Arguments.of(
              DataTypes.ARRAY(DataTypes.INT()),
              List.of(
                  new GenericArray(new Integer[] {1, null, -2}), new GenericArray(new Integer[0])),
              format));
      var map = new java.util.LinkedHashMap<BinaryString, Integer>();
      map.put(BinaryString.fromString("key"), null);
      map.put(BinaryString.fromString(""), 7);
      nested.add(
          Arguments.of(
              DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
              List.of(new GenericMap(map), new GenericMap(Map.of())),
              format));
      nested.add(
          Arguments.of(
              DataTypes.ROW(
                  DataTypes.FIELD(3, "a", DataTypes.INT()),
                  DataTypes.FIELD(4, "b", DataTypes.STRING())),
              List.of(GenericRow.of(1, null), GenericRow.of(null, BinaryString.fromString("é"))),
              format));
      for (int precision : new int[] {0, 3, 6}) {
        var timestamps =
            List.of(Timestamp.fromEpochMillis(123456000), Timestamp.fromEpochMillis(234567000));

        nested.add(Arguments.of(DataTypes.TIMESTAMP(precision), timestamps, format));
        nested.add(
            Arguments.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision), timestamps, format));
      }
    }
    return Stream.concat(PaimonValueTypesTest.values(), nested.stream());
  }

  @ParameterizedTest(name = "{0}, {2}")
  @MethodSource("values")
  void partialUpdateSelectsWholeArrowCellsOfEverySupportedValueType(
      DataType valueType, List<?> values, String format) throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", valueType),
                new DataField(
                    2,
                    "other",
                    valueType instanceof RowType
                        ? new RowType(
                            ((RowType) valueType)
                                .getFields().stream()
                                    .map(f -> new DataField(f.id() + 10, f.name(), f.type()))
                                    .collect(ListCollectors.toList()))
                        : valueType)));
    var table =
        PaimonMergeEngineTest.table(
            Map.of("merge-engine", "partial-update", "file.format", format, "bucket", "1"), type);
    var runs = new ArrayList<List<InternalRow>>();
    for (int run = 0; run < 3; run++) {
      var rows = new ArrayList<InternalRow>();
      for (int i = 0; i < values.size(); i++) {
        rows.add(
            GenericRow.of(
                i,
                run == 0 ? values.get(i) : null,
                run == 1 ? values.get(values.size() - i - 1) : null));
      }
      rows.add(GenericRow.of(values.size(), null, null));
      runs.add(rows);
    }
    var split = writeRuns(table, runs);
    var read = table.newReadBuilder().withProjection(new int[] {2, 1, 0});
    var actual = collect(table, read, split, true, 2, 0, Integer.MAX_VALUE);
    assertTrue(actual.merged() > 0);
    assertEquals(stock(read, split), actual.rows());
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  static DataSplit writeRuns(FileStoreTable table, List<List<InternalRow>> runs) throws Exception {
    var partition =
        new RowPartitionKeyExtractor(table.schema()).partition(runs.get(0).get(0)).copy();
    var layout = PaimonKeyValueLayout.of(table);
    var type = LogicalTypeConversion.toLogicalType(table.rowType());
    var files = new ArrayList<DataFileMeta>();
    try (var allocator = new RootAllocator()) {
      for (var run : runs) {
        try (var buffer =
            new KeyedUpsertBuffer(
                allocator, layout.keyColumns, table.rowType().getFieldCount(), true, false)) {
          List<RowData> rows =
              run.stream()
                  .map(row -> (RowData) new org.apache.paimon.flink.FlinkRowData(row))
                  .collect(ListCollectors.toList());
          buffer.push(RowDataArrowConverter.write(rows, type, allocator, true), 0);
          files.addAll(
              new NativePaimonKeyValueFileWriter(table, layout)
                  .write(partition, 0, buffer.flush())
                  .newFiles());
        }
      }
    }
    return DataSplit.builder()
        .withSnapshot(1)
        .withPartition(partition)
        .withBucket(0)
        .withBucketPath(
            ((PrimaryKeyFileStoreTable) table)
                .store()
                .pathFactory()
                .bucketPath(partition, 0)
                .toString())
        .withDataFiles(files)
        .isStreaming(false)
        .rawConvertible(false)
        .build();
  }
}
