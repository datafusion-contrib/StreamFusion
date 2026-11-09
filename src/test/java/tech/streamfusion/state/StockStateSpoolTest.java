package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.operator.TaskOffHeapMemory;

class StockStateSpoolTest {
  @TempDir Path temporaryDirectory;

  private final RowDataSerializer keys = new RowDataSerializer(RowType.of(new VarCharType()));

  @BeforeAll
  static void memoryAuthority() {
    if (TaskOffHeapMemory.capacityBytes() == 0) {
      var config = new Configuration();
      config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
      TaskOffHeapMemory.initialize(config);
    }
  }

  private RowData key(String value) {
    return GenericRowData.of(StringData.fromString(value));
  }

  @Test
  void manyOwnersAndSignedTimersStayIndependentAcrossRepeatedPasses() throws Exception {
    long initialMemory = TaskOffHeapMemory.reservedBytes();
    try (var inventory = new StockStateSpool(keys, temporaryDirectory)) {
      assertEquals(initialMemory + 2 * 1024 * 1024, TaskOffHeapMemory.reservedBytes());
      for (int i = 0; i < 4099; i++) {
        var owner = key("owner" + i);
        inventory.addOwner(owner);
        inventory.addOwner(owner);
        for (long time : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE})
          assertTrue(inventory.add(1, owner, time));
        assertFalse(inventory.add(1, owner, 0));
        assertTrue(inventory.add(2, owner, 100));
      }
      for (int pass = 0; pass < 2; pass++) {
        int owners = 0;
        try (var source = inventory.owners()) {
          while (source.hasNext()) {
            var owner = source.next();
            assertTrue(inventory.hasAny(1, owner));
            assertFalse(inventory.hasAny(3, owner));
            try (var times = inventory.times(1, owner)) {
              for (long expected : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
                assertTrue(times.hasNext());
                assertEquals(expected, times.next());
              }
              assertFalse(times.hasNext());
            }
            assertTrue(inventory.contains(2, owner, 100));
            assertFalse(inventory.contains(1, owner, 100));
            owners++;
          }
        }
        assertEquals(4099, owners);
      }
    }
    assertEquals(initialMemory, TaskOffHeapMemory.reservedBytes());
    assertEmptyDirectory();
  }

  @Test
  void prefixOwnersAndEarlyFailureReleaseIteratorsAndDeleteScratchState() throws Exception {
    long initialMemory = TaskOffHeapMemory.reservedBytes();
    var failure =
        assertThrows(
            IllegalStateException.class,
            () -> {
              try (var inventory = new StockStateSpool(keys, temporaryDirectory)) {
                for (var name : List.of("", "a", "aa", "a" + (char) 0, "ab")) {
                  inventory.addOwner(key(name));
                  inventory.add(1, key(name), name.length());
                }
                try (var owners = inventory.owners()) {
                  assertTrue(owners.hasNext());
                  try (var timers = inventory.times(1, key("a"))) {
                    assertEquals(1L, timers.next());
                    assertFalse(timers.hasNext());
                  }
                  throw new IllegalStateException("receiver failed");
                }
              }
            });
    assertEquals("receiver failed", failure.getMessage());
    assertEquals(initialMemory, TaskOffHeapMemory.reservedBytes());
    assertEmptyDirectory();
  }

  @Test
  void deniedMemoryAndFailedOpenLeaveNoScratchFilesOrReservations() throws Exception {
    long initialMemory = TaskOffHeapMemory.reservedBytes();
    try (var held =
        TaskOffHeapMemory.reserve("test", "inventory denial", TaskOffHeapMemory.availableBytes())) {
      long reserved = TaskOffHeapMemory.reservedBytes();
      assertThrows(
          NativeMemoryLimitException.class, () -> new StockStateSpool(keys, temporaryDirectory));
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      assertEmptyDirectory();
    }
    assertThrows(
        java.nio.file.NoSuchFileException.class,
        () -> new StockStateSpool(keys, temporaryDirectory.resolve("missing")));
    assertEquals(initialMemory, TaskOffHeapMemory.reservedBytes());
    assertEmptyDirectory();
  }

  private void assertEmptyDirectory() throws Exception {
    try (var files = Files.list(temporaryDirectory)) {
      assertEquals(0, files.count());
    }
  }
}
