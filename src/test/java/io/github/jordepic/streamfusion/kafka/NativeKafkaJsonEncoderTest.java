package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaJsonEncoderTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new IntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BooleanType(),
            new DoubleType()
          },
          new String[] {"id", "name", "enabled", "score"});

  @Test
  void matchesFlinkForWholeBatchesWithAndWithoutNullFields() throws Exception {
    GenericRowData first = GenericRowData.of(1, StringData.fromString("quote: \" and 雪"), true, 2.5);
    GenericRowData nulls = GenericRowData.of(2, null, false, null);
    List<RowData> rows = List.of(first, nulls);

    assertMatchesFlink(rows, false);
    assertMatchesFlink(rows, true);
  }

  @Test
  void matchesFlinkTimestampFormatting() throws Exception {
    RowType timestamps =
        RowType.of(
            new LogicalType[] {new TimestampType(3), new TimestampType(9)},
            new String[] {"millis", "nanos"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                TimestampData.fromEpochMillis(1_577_934_245_678L),
                TimestampData.fromEpochMillis(1_577_934_245_678L, 123_456)));

    assertMatchesFlink(rows, timestamps, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, timestamps, TimestampFormat.ISO_8601, false);
  }

  private static void assertMatchesFlink(List<RowData> rows, boolean ignoreNullFields)
      throws Exception {
    assertMatchesFlink(rows, ROW_TYPE, TimestampFormat.SQL, ignoreNullFields);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields)
      throws Exception {
    JsonRowDataSerializationSchema flink =
        new JsonRowDataSerializationSchema(
            rowType,
            timestampFormat,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            false,
            ignoreNullFields);
    flink.open(
        new SerializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                NativeKafkaJsonEncoderTest.class.getClassLoader());
          }
        });

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          NativeKafka.encodeKafkaJsonBatch(
              array.memoryAddress(),
              schema.memoryAddress(),
              ignoreNullFields,
              timestampFormat == TimestampFormat.SQL ? "SQL" : "ISO-8601");

      assertEquals(rows.size(), actual.length);
      for (int i = 0; i < rows.size(); i++) {
        assertArrayEquals(flink.serialize(rows.get(i)), actual[i], "row " + i);
      }
    }
  }
}
