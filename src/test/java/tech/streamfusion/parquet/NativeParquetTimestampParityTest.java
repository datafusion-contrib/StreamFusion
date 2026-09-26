package tech.streamfusion.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeParquetTimestampParityTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"millis", "micros", "nanos", "int96"})
  void physicalTimestampUnitsMatchFlinkIncludingOverflow(String unit) throws Exception {
    RowType type = RowType.of(new TimestampType(9));
    List<RowData> rows = new ArrayList<>();
    for (long millis :
        new long[] {
          Long.MIN_VALUE,
          -62_135_596_800_000L,
          -1,
          0,
          9_223_459_200_000L,
          253_402_300_799_999L,
          Long.MAX_VALUE
        }) {
      rows.add(GenericRowData.of(TimestampData.fromEpochMillis(millis, 999_999)));
    }
    rows.add(GenericRowData.of((Object) null));
    Path stock = directory.resolve("stock.parquet");
    Configuration config = new Configuration(false);
    config.setBoolean("parquet.write.int64.timestamp", !unit.equals("int96"));
    config.set("parquet.timestamp.time.unit", unit.equals("int96") ? "micros" : unit);
    var path = new org.apache.flink.core.fs.Path(stock.toUri());
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer = ParquetRowDataBuilder.createWriterFactory(type, config, true).create(output);
      for (RowData row : rows) writer.addElement(row);
      writer.finish();
    }
    Path nativeFile = directory.resolve("native.parquet");
    try (var allocator = new RootAllocator();
        var batch = RowDataArrowConverter.write(rows, type, allocator);
        var output = Files.newOutputStream(nativeFile);
        var writer =
            new ParquetCodec()
                .createEncoder(
                    batch.getSchema(),
                    new int[0],
                    new String[] {unit.equals("int96") ? "timestamp.int96" : "timestamp.unit"},
                    new String[] {unit.equals("int96") ? "true" : unit},
                    false,
                    output)) {
      writer.write(batch, new int[0], 0, rows.size());
      writer.finish();
    }
    // Inspect the physical longs: a reader's timestamp conversion must not hide wire differences.
    assertEquals(
        physicalValues(stock, unit.equals("int96")),
        physicalValues(nativeFile, unit.equals("int96")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "America/Los_Angeles", "Asia/Kathmandu"})
  void localInt96MatchesHostCalendarAndTimezone(String zone) throws Exception {
    var previous = java.util.TimeZone.getDefault();
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
    try {
      RowType type = RowType.of(new TimestampType(9));
      List<RowData> rows = new ArrayList<>();
      for (String value :
          List.of(
              "0001-01-01T01:02:03.123456789",
              "1500-03-01T01:02:03.123456789",
              "1969-12-31T23:59:59.999999999",
              "1970-01-01T00:00:00",
              "2024-03-10T02:30:00.123456789",
              "2024-11-03T01:30:00.999999999",
              "9999-12-31T23:59:59.123456789")) {
        rows.add(
            GenericRowData.of(
                TimestampData.fromLocalDateTime(java.time.LocalDateTime.parse(value))));
      }
      rows.add(GenericRowData.of((Object) null));
      Path stock = directory.resolve("local-stock.parquet");
      var path = new org.apache.flink.core.fs.Path(stock.toUri());
      try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
        var writer =
            ParquetRowDataBuilder.createWriterFactory(type, new Configuration(false), false)
                .create(output);
        for (var row : rows) writer.addElement(row);
        writer.finish();
      }
      Path nativeFile = directory.resolve("local-native.parquet");
      try (var allocator = new RootAllocator();
          var batch = RowDataArrowConverter.write(rows, type, allocator);
          var output = Files.newOutputStream(nativeFile);
          var writer =
              new ParquetCodec()
                  .createEncoder(
                      batch.getSchema(),
                      new int[0],
                      new String[] {"timestamp.int96", "timestamp.local"},
                      new String[] {"true", "true"},
                      false,
                      output)) {
        writer.write(batch, new int[0], 0, rows.size());
        writer.finish();
      }
      assertEquals(physicalValues(stock, true), physicalValues(nativeFile, true));
    } finally {
      java.util.TimeZone.setDefault(previous);
    }
  }

  @org.junit.jupiter.api.Test
  void nestedInt96PreservesNullEmptyAndRepeatedLevelsAcrossRowGroups() throws Exception {
    var timestamp = new TimestampType(9);
    var nested =
        RowType.of(
            timestamp,
            new org.apache.flink.table.types.logical.ArrayType(timestamp),
            new org.apache.flink.table.types.logical.MapType(
                new org.apache.flink.table.types.logical.IntType(false), timestamp));
    RowType type = RowType.of(nested, new org.apache.flink.table.types.logical.IntType());
    var ts = TimestampData.fromEpochMillis(-62_135_596_800_001L, 123_456);
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 2053; i++) {
      var map = new java.util.LinkedHashMap<Integer, TimestampData>();
      map.put(1, ts);
      map.put(2, null);
      Object value =
          switch (i % 4) {
            case 0 -> null;
            case 1 -> GenericRowData.of(null, null, null);
            case 2 ->
                GenericRowData.of(
                    ts,
                    new org.apache.flink.table.data.GenericArrayData(new Object[0]),
                    new org.apache.flink.table.data.GenericMapData(java.util.Map.of()));
            default ->
                GenericRowData.of(
                    ts,
                    new org.apache.flink.table.data.GenericArrayData(new Object[] {ts, null, ts}),
                    new org.apache.flink.table.data.GenericMapData(map));
          };
      rows.add(GenericRowData.of(value, i));
    }
    Path stock = directory.resolve("nested-stock.parquet");
    var path = new org.apache.flink.core.fs.Path(stock.toUri());
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer =
          ParquetRowDataBuilder.createWriterFactory(type, new Configuration(false), true)
              .create(output);
      for (var row : rows) writer.addElement(row);
      writer.finish();
    }
    Path nativeFile = directory.resolve("nested-native.parquet");
    try (var allocator = new RootAllocator();
        var batch = RowDataArrowConverter.write(rows, type, allocator);
        var output = Files.newOutputStream(nativeFile);
        var writer =
            new ParquetCodec()
                .createEncoder(
                    batch.getSchema(),
                    new int[0],
                    new String[] {"timestamp.int96", "block.size"},
                    new String[] {"true", "1024"},
                    false,
                    output)) {
      writer.write(batch, new int[0], 0, 1027);
      writer.write(batch, new int[0], 1027, rows.size() - 1027);
      writer.finish();
    }
    assertEquals(groups(stock), groups(nativeFile));
    try (var reader =
        org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(nativeFile.toUri()), new Configuration(false)))) {
      org.junit.jupiter.api.Assertions.assertTrue(reader.getRowGroups().size() > 1);
      assertEquals(
          org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96,
          reader
              .getFooter()
              .getFileMetaData()
              .getSchema()
              .getColumns()
              .get(0)
              .getPrimitiveType()
              .getPrimitiveTypeName());
    }
  }

  private static List<String> groups(Path path) throws Exception {
    var values = new ArrayList<String>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
            .build()) {
      Group row;
      while ((row = reader.read()) != null) values.add(row.toString());
    }
    return values;
  }

  private static List<Object> physicalValues(Path path, boolean int96) throws Exception {
    List<Object> values = new ArrayList<>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
            .build()) {
      Group row;
      while ((row = reader.read()) != null) {
        values.add(
            row.getFieldRepetitionCount(0) == 0
                ? null
                : int96
                    ? java.util.HexFormat.of().formatHex(row.getInt96(0, 0).getBytes())
                    : row.getLong(0, 0));
      }
    }
    return values;
  }
}
