package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

class NativeIcebergEncoderMemoryTest {
  @TempDir Path temp;

  @org.junit.jupiter.api.Test
  void sharedBatchLeaseEndsBeforeTheNextInputAndSurvivesIndependentFileDrains() throws Exception {
    TaskOffHeapMemory.initialize(
        Configuration.fromMap(Map.of("taskmanager.memory.task.off-heap.size", "8mb")));
    var schema = new Schema(Types.NestedField.required(17, "id", Types.IntegerType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(schema, PartitionSpec.unpartitioned(), temp.resolve("leases").toString());
    var context = new ArrowWriteContext();
    var settings = IcebergParquetSettings.translate(Map.of());
    var paths =
        java.util.List.of(table.location() + "/even.parquet", table.location() + "/odd.parquet");
    try (var even =
            new NativeIcebergAppender(
                context,
                table.io().newOutputFile(paths.get(0)),
                SchemaParser.toJson(schema),
                settings,
                MetricsConfig.forTable(table));
        var odd =
            new NativeIcebergAppender(
                context,
                table.io().newOutputFile(paths.get(1)),
                SchemaParser.toJson(schema),
                settings,
                MetricsConfig.forTable(table))) {
      for (int batch = 0; batch < 2; batch++) {
        try (var ids = new IntVector("id", NativeAllocator.SHARED)) {
          ids.allocateNew(128);
          for (int i = 0; i < 128; i++) ids.set(i, batch * 128 + i);
          ids.setValueCount(128);
          try (var root = VectorSchemaRoot.of(ids)) {
            context.begin(root);
            try {
              var rows = ArrowConversion.createArrowReader(root, FlinkSchemaUtil.convert(schema));
              for (int i = 0; i < 128; i++) {
                var row = rows.read(i);
                context.position(i, row);
                (i % 2 == 0 ? even : odd).add(row);
                if (i % 16 == 15) {
                  even.length();
                  odd.length();
                  assertTrue(NativeIceberg.liveNativeHandles().contains("RecordBatch=1"));
                }
              }
              context.drain();
            } finally {
              context.clear();
            }
            assertFalse(NativeIceberg.liveNativeHandles().contains("RecordBatch"));
          }
        }
      }
    }
    for (int parity = 0; parity < 2; parity++) {
      var actual = new java.util.ArrayList<Integer>();
      try (var records =
          org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(paths.get(parity)))
              .project(schema)
              .createReaderFunc(
                  fileSchema ->
                      org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                          schema, fileSchema))
              .<org.apache.iceberg.data.Record>build()) {
        for (var record : records) actual.add((Integer) record.getField("id"));
      }
      final int expectedParity = parity;
      assertEquals(
          java.util.stream.IntStream.range(0, 256)
              .filter(i -> i % 2 == expectedParity)
              .boxed()
              .toList(),
          actual);
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, TaskOffHeapMemory.reservedBytes());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encoderRetentionIsChargedAndReleasedOnCloseOrQuotaFailure(boolean exhaust) throws Exception {
    TaskOffHeapMemory.initialize(
        Configuration.fromMap(Map.of("taskmanager.memory.task.off-heap.size", "8mb")));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(17, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve("table").toUri().toString());
    var context = new ArrowWriteContext();
    var settings = IcebergParquetSettings.translate(Map.of("parquet.enable.dictionary", "false"));
    try (var ids = new IntVector("id", NativeAllocator.SHARED)) {
      ids.allocateNew(10000);
      for (int i = 0; i < 10000; i++) ids.set(i, i);
      ids.setValueCount(10000);
      try (var root = VectorSchemaRoot.of(ids);
          var appender =
              new NativeIcebergAppender(
                  context,
                  table.io().newOutputFile(table.location() + "/data.parquet"),
                  SchemaParser.toJson(table.schema()),
                  settings,
                  MetricsConfig.forTable(table))) {
        context.begin(root);
        var rows = ArrowConversion.createArrowReader(root, FlinkSchemaUtil.convert(table.schema()));
        for (int i = 0; i < 64; i++) {
          var row = rows.read(i);
          context.position(i, row);
          appender.add(row);
        }
        context.drain();
        assertTrue(
            TaskOffHeapMemory.reservedBytes() > NativeAllocator.SHARED.getAllocatedMemory(),
            "encoder buffers must be charged in addition to shared Arrow allocations");
        if (exhaust) {
          long remaining = TaskOffHeapMemory.availableBytes() - 4096;
          try (var held =
              TaskOffHeapMemory.reserve("fixture", "leave room for C Data structs", remaining)) {
            var failure =
                assertThrows(
                    NativeMemoryLimitException.class,
                    () -> {
                      for (int i = 64; i < 9000; i++) {
                        var row = rows.read(i);
                        context.position(i, row);
                        appender.add(row);
                      }
                      context.drain();
                    });
            context.fail(failure);
          }
        } else {
          for (int i = 64; i < 9000; i++) {
            var row = rows.read(i);
            context.position(i, row);
            appender.add(row);
          }
          context.drain();
          appender.close();
          assertEquals(9000, appender.metrics().recordCount());
        }
        context.clear();
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, TaskOffHeapMemory.reservedBytes());
  }
}
