package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeIcebergTypeTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3})
  void scalarValuesKeepPrecisionAcrossNativeDataFilesAndStockReadback(int formatVersion)
      throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "f", Types.FloatType.get()),
            Types.NestedField.optional(3, "d", Types.DoubleType.get()),
            Types.NestedField.optional(4, "amount", Types.DecimalType.of(38, 6)),
            Types.NestedField.optional(5, "clock", Types.TimeType.get()),
            Types.NestedField.optional(6, "local", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(7, "instant", Types.TimestampType.withZone()));
    String location = temp.resolve("types").toUri().toString();
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)),
                location);
    var rowType = FlinkSchemaUtil.convert(table.schema());
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setParallelism(1);
    var input =
        env.fromSequence(0, 0)
            .map(
                ignored ->
                    new ArrowBatch(
                        RowDataArrowConverter.write(rows(), rowType, NativeAllocator.SHARED)))
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        new Configuration(),
        Map.of("distribution-mode", "none"));
    env.execute("iceberg-native-scalar-types");
    table.refresh();
    int count = 0;
    try (var records = IcebergGenerics.read(table).build()) {
      for (var record : records) {
        int id = (Integer) record.getField("id");
        var expected = rows().get(id);
        if (id == 2) {
          for (var field : table.schema().columns().subList(1, 7))
            assertNull(record.getField(field.name()));
        } else {
          assertEquals(
              Float.floatToIntBits(expected.getFloat(1)),
              Float.floatToIntBits((Float) record.getField("f")));
          assertEquals(
              Double.doubleToLongBits(expected.getDouble(2)),
              Double.doubleToLongBits((Double) record.getField("d")));
          assertEquals(expected.getDecimal(3, 38, 6).toBigDecimal(), record.getField("amount"));
          assertEquals(
              LocalTime.ofNanoOfDay(expected.getInt(4) * 1_000_000L), record.getField("clock"));
          assertEquals(expected.getTimestamp(5, 6).toLocalDateTime(), record.getField("local"));
          assertEquals(
              expected.getTimestamp(6, 6).toInstant().atOffset(ZoneOffset.UTC),
              record.getField("instant"));
        }
        count++;
      }
    }
    assertEquals(3, count);
    try (var tasks = table.newScan().includeColumnStats().planFiles()) {
      for (var task : tasks) {
        assertEquals(Map.of(2, 1L, 3, 1L), task.file().nanValueCounts());
        try (var decoder =
            new IcebergNativeTaskReader(
                task,
                table.schema(),
                table.schema().columns().stream().map(f -> f.fieldId()).toList(),
                table.io(),
                2,
                0)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          int offset = 0;
          while ((next = decoder.next()) != null) {
            try (var root = next) {
              var reader = ArrowConversion.createArrowReader(root, rowType);
              for (int row = 0; row < root.getRowCount(); row++) {
                var actual = reader.read(row);
                var expected = rows().get(offset++);
                assertEquals(expected.getInt(0), actual.getInt(0));
                for (int column = 1; column < 7; column++)
                  assertEquals(expected.isNullAt(column), actual.isNullAt(column));
                if (actual.getInt(0) == 2) continue;
                assertEquals(
                    Float.floatToIntBits(expected.getFloat(1)),
                    Float.floatToIntBits(actual.getFloat(1)));
                assertEquals(
                    Double.doubleToLongBits(expected.getDouble(2)),
                    Double.doubleToLongBits(actual.getDouble(2)));
                assertEquals(expected.getDecimal(3, 38, 6), actual.getDecimal(3, 38, 6));
                assertEquals(expected.getInt(4), actual.getInt(4));
                assertEquals(expected.getTimestamp(5, 6), actual.getTimestamp(5, 6));
                assertEquals(expected.getTimestamp(6, 6), actual.getTimestamp(6, 6));
              }
            }
          }
          assertEquals(3, offset);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static List<RowData> rows() {
    return List.of(
        GenericRowData.of(
            0,
            Float.NaN,
            Double.NaN,
            DecimalData.fromBigDecimal(
                new BigDecimal("99999999999999999999999999999999.999999"), 38, 6),
            86_399_999,
            TimestampData.fromLocalDateTime(LocalDateTime.parse("0001-01-01T00:00:00.000001")),
            TimestampData.fromInstant(Instant.parse("9999-12-31T23:59:59.999999Z"))),
        GenericRowData.of(
            1,
            -0.0f,
            Double.NEGATIVE_INFINITY,
            DecimalData.fromBigDecimal(new BigDecimal("-0.000001"), 38, 6),
            0,
            TimestampData.fromLocalDateTime(LocalDateTime.parse("1969-12-31T23:59:59.999999")),
            TimestampData.fromInstant(Instant.parse("1969-12-31T23:59:59.999999Z"))),
        GenericRowData.of(2, null, null, null, null, null, null));
  }
}
