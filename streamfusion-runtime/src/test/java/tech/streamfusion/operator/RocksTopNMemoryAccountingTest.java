package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

class RocksTopNMemoryAccountingTest {

  @TempDir Path directory;
  private long previousCapacity;

  @BeforeEach
  void configureSharedPool() {
    previousCapacity = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(128 << 10);
  }

  @AfterEach
  void restoreSharedPool() {
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void persistedKeysDoNotAccumulateSharedReservations(boolean miniBatch) {
    BufferAllocator allocator = NativeAllocator.SHARED;
    long owner = TaskOffHeapMemory.registerOwner("operator", "RocksDB TopN regression");
    long handle = 0;
    try (VectorSchemaRoot input =
        VectorSchemaRoot.of(new BigIntVector("k", allocator), new BigIntVector("v", allocator))) {
      try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, input.getSchema(), null, schema);
        handle =
            Native.createRocksDBTopNRanker(
                new int[] {0},
                new int[] {-1},
                new int[] {1},
                new int[] {0},
                new int[] {0},
                0,
                1,
                -1,
                false,
                false,
                false,
                miniBatch,
                0,
                0,
                -owner - 1,
                schema.memoryAddress(),
                directory.resolve("db").toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                new byte[0][]);
      }
      for (int batch = 0; batch < 100; batch++) {
        input.allocateNew();
        for (int row = 0; row < 16; row++) {
          ((BigIntVector) input.getVector(0)).setSafe(row, batch * 16L + row);
          ((BigIntVector) input.getVector(1)).setSafe(row, 1);
        }
        input.setRowCount(16);
        try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
            ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
          Native.pushRocksDBTopNRanker(
              handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              batch,
              outArray.memoryAddress(),
              outSchema.memoryAddress());
          try (VectorSchemaRoot output =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
            assertEquals(miniBatch ? 0 : 16, output.getRowCount());
          }
        } finally {
          input.clear();
        }
        if (miniBatch) {
          assertTrue(Native.rocksdbTopNRankerStateBytes(handle) > 0);
          try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
              ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
            Native.flushRocksDBTopNRanker(
                handle, outArray.memoryAddress(), outSchema.memoryAddress());
            try (VectorSchemaRoot output =
                Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
              assertEquals(16, output.getRowCount());
            }
          }
        }
        assertEquals(0, Native.rocksdbTopNRankerStateBytes(handle));
        assertEquals(0, Native.rocksdbTopNRankerStagingBytes(handle));
        assertEquals(0, Native.rocksdbTopNRankerCacheStats(handle)[3]);
        assertEquals(0, allocator.getAllocatedMemory());
        assertEquals(0, TaskOffHeapMemory.reservedBytes(), "completed batch " + batch);
      }
      Native.checkpointRocksDBTopNRanker(handle, directory.resolve("checkpoint").toString());
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
    } finally {
      if (handle != 0) {
        Native.closeRocksDBTopNRanker(handle);
      }
      TaskOffHeapMemory.closeOwner(owner);
    }
  }
}
