package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NativeIcebergTemporalPredicateTest {
  @TempDir Path temp;

  static Stream<Arguments> cases() {
    return Stream.of("date", "time", "timestamp", "timestamptz", "decimal", "binary")
        .flatMap(kind -> Stream.of(false, true).map(residual -> Arguments.of(kind, residual)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void temporalAndScalarFiltersMatchTheStockReader(String kind, boolean residual) throws Exception {
    org.apache.iceberg.types.Type type;
    Object[] values;
    Object literal;
    switch (kind) {
      case "date" -> {
        type = Types.DateType.get();
        values =
            new Object[] {
              java.time.LocalDate.of(1969, 12, 30),
              java.time.LocalDate.of(1969, 12, 31),
              java.time.LocalDate.of(1970, 1, 1),
              null
            };
        literal = "1969-12-31";
      }
      case "time" -> {
        type = Types.TimeType.get();
        values =
            new Object[] {
              java.time.LocalTime.ofNanoOfDay(123_456_000),
              java.time.LocalTime.ofNanoOfDay(124_000_000),
              java.time.LocalTime.ofNanoOfDay(124_567_000),
              null
            };
        literal = "00:00:00.123456";
      }
      case "timestamp" -> {
        type = Types.TimestampType.withoutZone();
        values =
            new Object[] {
              java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_998_000),
              java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000),
              java.time.LocalDateTime.of(1970, 1, 1, 0, 0),
              null
            };
        literal = "1969-12-31T23:59:59.999999";
      }
      case "timestamptz" -> {
        type = Types.TimestampType.withZone();
        values =
            new Object[] {
              java.time.OffsetDateTime.parse("1969-12-31T23:59:59.999998Z"),
              java.time.OffsetDateTime.parse("1969-12-31T23:59:59.999999Z"),
              java.time.OffsetDateTime.parse("1970-01-01T00:00:00Z"),
              null
            };
        literal = "1969-12-31T23:59:59.999999+00:00";
      }
      case "decimal" -> {
        type = Types.DecimalType.of(38, 4);
        values =
            new Object[] {
              new java.math.BigDecimal("-123.4501"),
              new java.math.BigDecimal("-123.4500"),
              new java.math.BigDecimal("123.4500"),
              null
            };
        literal = new java.math.BigDecimal("-123.4500");
      }
      case "binary" -> {
        type = Types.BinaryType.get();
        values =
            new Object[] {
              java.nio.ByteBuffer.wrap(new byte[] {0}),
              java.nio.ByteBuffer.wrap(new byte[] {0, (byte) 255}),
              java.nio.ByteBuffer.wrap(new byte[] {(byte) 255}),
              null
            };
        literal = java.nio.ByteBuffer.wrap(new byte[] {0, (byte) 255});
      }
      default -> throw new AssertionError(kind);
    }
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", type));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("scalars").toUri().toString());
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/data.parquet")),
            table.spec(),
            null);
    try (writer) {
      var row = GenericRecord.create(schema);
      for (int id = 0; id < values.length; id++) {
        row.setField("id", id);
        row.setField("value", values[id]);
        writer.write(row);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    var predicate = Expressions.greaterThanOrEqual("value", literal);
    var filters = List.<org.apache.iceberg.expressions.Expression>of(predicate);
    var scan = table.newScan();
    if (residual) scan = scan.filter(predicate);
    try (var tasks = scan.planFiles()) {
      for (var task : tasks) {
        var expected = new ArrayList<Integer>();
        var stock =
            new RowDataReaderFunction(
                new Configuration(),
                schema,
                schema,
                null,
                true,
                table.io(),
                table.encryption(),
                filters);
        var split =
            IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(List.of(task)));
        try (var input = stock.apply(split)) {
          while (input.hasNext()) {
            var records = input.next();
            try {
              while (records.nextSplit() != null) {
                org.apache.iceberg.flink.source.reader.RecordAndPosition<
                        org.apache.flink.table.data.RowData>
                    row;
                while ((row = records.nextRecordFromSplit()) != null)
                  expected.add(row.record().getInt(0));
              }
            } finally {
              records.recycle();
            }
          }
        }
        if (kind.equals("time") || kind.equals("binary")) {
          var rejected =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      new IcebergNativeTaskReader(
                          task, schema, List.of(1), table.io(), 2, 0, filters, true, null));
          assertTrue(rejected.getMessage().contains("Native source predicates do not admit"));
          continue;
        }
        var actual = new ArrayList<Integer>();
        try (var reader =
            new IcebergNativeTaskReader(
                task, schema, List.of(1), table.io(), 2, 0, filters, true, null)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          while ((next = reader.next()) != null) {
            try (var root = next) {
              for (int row = 0; row < root.getRowCount(); row++)
                actual.add((Integer) root.getVector(0).getObject(row));
            }
          }
        }
        assertEquals(expected, actual, kind + " residual=" + residual);
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
