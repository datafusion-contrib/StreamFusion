package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.TaskOffHeapMemory;

class StockMapListSpoolTest {
  @TempDir Path directory;
  private final RowDataSerializer owners = new RowDataSerializer(RowType.of(new BigIntType()));
  private final RowDataSerializer rows =
      new RowDataSerializer(RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)));

  @BeforeAll
  static void memoryAuthority() {
    if (TaskOffHeapMemory.capacityBytes() == 0) {
      var config = new Configuration();
      config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
      TaskOffHeapMemory.initialize(config);
    }
  }

  private StockMapListSpool<Long> spool() throws Exception {
    return new StockMapListSpool<>(owners, LongSerializer.INSTANCE, rows, directory);
  }

  private byte[] encode(List<RowData> values) throws Exception {
    var output = new DataOutputSerializer(64);
    new ListSerializer<>(rows).serialize(values, output);
    return output.getCopyOfBuffer();
  }

  @Test
  void interleavedBucketsKeepRowOrderAndUseTheReleasedListSerializer() throws Exception {
    long initial = TaskOffHeapMemory.reservedBytes();
    var expected = new HashMap<String, List<RowData>>();
    var retained = new ArrayList<List<RowData>>();
    try (var spool = spool()) {
      var owner = new GenericRowData(1);
      var row = new GenericRowData(2);
      for (int i = 0; i < 8195; i++) {
        long key = i % 4;
        long time = i % 3 == 0 ? -10 : Long.MAX_VALUE;
        owner.setField(0, key);
        row.setField(0, (long) i);
        row.setField(1, StringData.fromString("row-" + i));
        var values = expected.computeIfAbsent(key + ":" + time, ignored -> new ArrayList<>());
        assertEquals(values.isEmpty(), spool.append(owner, time, row));
        values.add(rows.toBinaryRow(row).copy());
        row.setField(0, -1L);
      }
      int[] buckets = {0};
      spool.visit(
          true,
          (ownerKey, time, values) -> {
            var reference = expected.get(ownerKey.getLong(0) + ":" + time);
            assertEquals(reference.size(), values.size());
            assertFalse(values instanceof ArrayList);
            assertEquals(reference.get(reference.size() - 1), values.get(values.size() - 1));
            var left = values.iterator();
            var right = values.iterator();
            assertEquals(reference.get(0), left.next());
            assertEquals(reference.get(0), right.next());
            byte[] encoded = encode(values);
            assertArrayEquals(encode(reference), encoded);
            var decoded =
                new ListSerializer<>(rows).deserialize(new DataInputDeserializer(encoded));
            assertEquals(reference, decoded);
            retained.add(values);
            buckets[0]++;
          });
      assertEquals(expected.size(), buckets[0]);
      for (var closed : retained) assertThrows(IllegalStateException.class, () -> closed.get(0));
    }
    assertEquals(initial, TaskOffHeapMemory.reservedBytes());
    assertNoScratch();
  }

  @Test
  void heapReceiversOwnTheirListsAfterTheSpoolClosesAndLargeRowsStayExact() throws Exception {
    var retained = new ArrayList<List<RowData>>();
    var expected =
        List.<RowData>of(
            GenericRowData.of(0L, StringData.fromString("first")),
            GenericRowData.of(1L, StringData.fromString("x".repeat(2 * 1024 * 1024))),
            GenericRowData.of(2L, StringData.fromString("last")));
    try (var spool = spool()) {
      for (var row : expected) spool.append(GenericRowData.of((Object) null), 100L, row);
      spool.visit(
          false,
          (owner, time, values) -> {
            assertTrue(owner.isNullAt(0));
            assertEquals(100L, time);
            assertInstanceOf(ArrayList.class, values);
            retained.add(values);
          });
    }
    assertEquals(1, retained.size());
    assertArrayEquals(encode(expected), encode(retained.get(0)));
    assertNoScratch();
  }

  @Test
  void receiverFailureClosesPartiallyConsumedReadersAndReleasesTheDiskBudget() throws Exception {
    long initial = TaskOffHeapMemory.reservedBytes();
    var failure =
        assertThrows(
            java.io.IOException.class,
            () -> {
              try (var spool = spool()) {
                for (int i = 0; i < 20; i++)
                  spool.append(
                      GenericRowData.of(1L),
                      100L,
                      GenericRowData.of((long) i, StringData.fromString("row")));
                spool.visit(
                    true,
                    (owner, time, values) -> {
                      assertEquals(0L, values.iterator().next().getLong(0));
                      throw new java.io.IOException("receiver failed");
                    });
              }
            });
    assertEquals("receiver failed", failure.getMessage());
    assertEquals(initial, TaskOffHeapMemory.reservedBytes());
    assertNoScratch();
  }

  private void assertNoScratch() throws Exception {
    try (var files = Files.list(directory)) {
      assertEquals(0, files.count());
    }
  }
}
