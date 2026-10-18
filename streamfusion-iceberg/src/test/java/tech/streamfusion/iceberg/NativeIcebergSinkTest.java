package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

class NativeIcebergSinkTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3})
  void nativeParquetCommitsAndReadsBackWithStockIceberg(int formatVersion) throws Exception {
    String location = temp.resolve("table").toUri().toString();
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(17, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)),
                location);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    var input =
        env.fromSequence(0, 0)
            .map(
                ignored -> {
                  var vector = new IntVector("id", NativeAllocator.SHARED);
                  vector.allocateNew(2001);
                  for (int i = 0; i < 2001; i++) vector.set(i, i);
                  vector.setValueCount(2001);
                  return new ArrowBatch(VectorSchemaRoot.of(vector));
                })
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        new Configuration(),
        Map.of("distribution-mode", "none", "target-file-size-bytes", "1"));
    env.execute("native-iceberg-data-file-readback");
    table.refresh();
    List<Integer> values = new ArrayList<>();
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows) values.add((Integer) row.getField("id"));
    }
    values.sort(Integer::compareTo);
    assertEquals(java.util.stream.IntStream.range(0, 2001).boxed().toList(), values);
    List<Long> sizes = new ArrayList<>();
    try (var tasks = table.newScan().includeColumnStats().planFiles()) {
      for (var task : tasks) {
        assertEquals(
            java.util.Set.of(table.schema().findField("id").fieldId()),
            task.file().valueCounts().keySet());
        assertEquals(
            task.file().fileSizeInBytes(),
            table.io().newInputFile(task.file().location()).getLength());
        assertFalse(task.file().splitOffsets().isEmpty());
        sizes.add(task.file().recordCount());
      }
    }
    sizes.sort(Long::compareTo);
    assertEquals(List.of(1L, 1000L, 1000L), sizes);
    assertEquals(
        "", NativeIceberg.liveNativeHandles(), "Native encoders must be released after commit");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "uncompressed,v1",
    "snappy,v2",
    "zstd,v1",
    "gzip,v2"
  })
  void nullablePrimitiveColumnsPreserveHostReadback(String codec, String pageVersion)
      throws Exception {
    String location = temp.resolve("primitives").toUri().toString();
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "number", Types.LongType.get()),
            Types.NestedField.optional(3, "text", Types.StringType.get()),
            Types.NestedField.optional(4, "flag", Types.BooleanType.get()),
            Types.NestedField.optional(5, "bytes", Types.BinaryType.get()),
            Types.NestedField.optional(6, "day", Types.DateType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(
                    TableProperties.FORMAT_VERSION,
                    "2",
                    TableProperties.PARQUET_COMPRESSION,
                    codec,
                    TableProperties.PARQUET_PAGE_VERSION,
                    pageVersion),
                location);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(1);
    var input =
        env.fromSequence(0, 0)
            .map(ignored -> primitiveBatch())
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        new Configuration(),
        Map.of("distribution-mode", "none"));
    env.execute("native-iceberg-primitive-readback");
    table.refresh();
    int count = 0;
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows) {
        int id = (Integer) row.getField("id");
        if (id == 2) {
          for (String field : List.of("number", "text", "flag", "bytes", "day"))
            assertNull(row.getField(field));
        } else {
          assertEquals(id == 0 ? Long.MIN_VALUE : Long.MAX_VALUE, row.getField("number"));
          assertEquals(id == 0 ? "" : "é🐍", row.getField("text"));
          assertEquals(id == 0, row.getField("flag"));
          assertEquals(
              java.nio.ByteBuffer.wrap(id == 0 ? new byte[0] : new byte[] {0, (byte) 255}),
              row.getField("bytes"));
          assertEquals(java.time.LocalDate.ofEpochDay(id == 0 ? -1 : 0), row.getField("day"));
        }
        count++;
      }
    }
    assertEquals(3, count);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static ArrowBatch primitiveBatch() {
    var allocator = NativeAllocator.SHARED;
    var id = new IntVector("id", allocator);
    var number = new org.apache.arrow.vector.BigIntVector("number", allocator);
    var text = new org.apache.arrow.vector.VarCharVector("text", allocator);
    var flag = new org.apache.arrow.vector.BitVector("flag", allocator);
    var bytes = new org.apache.arrow.vector.VarBinaryVector("bytes", allocator);
    var day = new org.apache.arrow.vector.DateDayVector("day", allocator);
    var root = VectorSchemaRoot.of(id, number, text, flag, bytes, day);
    root.allocateNew();
    for (int i = 0; i < 3; i++) id.setSafe(i, i);
    number.setSafe(0, Long.MIN_VALUE);
    number.setSafe(1, Long.MAX_VALUE);
    text.setSafe(0, new byte[0]);
    text.setSafe(1, "é🐍".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    flag.setSafe(0, 1);
    flag.setSafe(1, 0);
    bytes.setSafe(0, new byte[0]);
    bytes.setSafe(1, new byte[] {0, (byte) 255});
    day.setSafe(0, -1);
    day.setSafe(1, 0);
    for (var vector : root.getFieldVectors()) vector.setValueCount(3);
    root.setRowCount(3);
    return new ArrowBatch(root);
  }

  @Test
  void partitionedFanoutKeepsInterleavedSelectionsInArrow() throws Exception {
    String location = temp.resolve("partitioned").toUri().toString();
    var schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    var spec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(schema, spec, Map.of(TableProperties.FORMAT_VERSION, "2"), location);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(1);
    var input =
        env.fromSequence(0, 0)
            .map(
                ignored -> {
                  var vector = new IntVector("id", NativeAllocator.SHARED);
                  vector.allocateNew(10001);
                  for (int i = 0; i < 10001; i++) vector.set(i, i);
                  vector.setValueCount(10001);
                  return new ArrowBatch(VectorSchemaRoot.of(vector));
                })
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        new Configuration(),
        Map.of("distribution-mode", "none", "target-file-size-bytes", "1"));
    env.execute("native-iceberg-partitioned-fanout");
    table.refresh();
    var seen = new java.util.HashSet<Integer>();
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows)
        assertTrue(seen.add((Integer) row.getField("id")), "Duplicate row across native files");
    }
    assertEquals(
        java.util.stream.IntStream.range(0, 10001)
            .boxed()
            .collect(java.util.stream.Collectors.toSet()),
        seen);
    var partitions = new java.util.HashSet<Integer>();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        partitions.add(task.file().partition().get(0, Integer.class));
        assertTrue(
            task.file().recordCount() <= 1000, "Each Java partition writer rolls independently");
      }
    }
    assertEquals(4, partitions.size());
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
