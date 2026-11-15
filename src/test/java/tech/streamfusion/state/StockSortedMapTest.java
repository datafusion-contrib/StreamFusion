package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.TaskOffHeapMemory;

class StockSortedMapTest {
  @TempDir Path directory;

  @BeforeAll
  static void memory() {
    StockMapListSpoolTest.memoryAuthority();
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void orderedDiskEntriesSerializeExactlyLikeReleasedSortedMapsWithIndependentTtl()
      throws Exception {
    var keys = new RowDataSerializer(RowType.of(new BigIntType()));
    var tuple =
        new TupleSerializer<Tuple2<RowData, Long>>(
            (Class) Tuple2.class, new TypeSerializer<?>[] {keys, LongSerializer.INSTANCE});
    Comparator<RowData> order = (a, b) -> Long.compare(a.getLong(0), b.getLong(0));
    long baseline = TaskOffHeapMemory.reservedBytes();
    try (var spool =
        new StockMapListSpool<Long, Tuple2<RowData, Long>>(
            keys, LongSerializer.INSTANCE, tuple, directory)) {
      var owner = GenericRowData.of(7L);
      assertTrue(spool.start(owner, 0L, Long.MIN_VALUE));
      assertFalse(spool.start(owner, 0L, 42));
      var expected = new TreeMap<RowData, Long>(order);
      for (long i = 0; i < 2051; i++) {
        var key = keys.toBinaryRow(GenericRowData.of(i)).copy();
        long value = i % 3 == 0 ? -i : i % 3 == 1 ? 0 : Long.MAX_VALUE;
        expected.put(key, value);
        spool.append(owner, 0L, Tuple2.of(key, value));
      }
      spool.visitWithMetadata(
          true,
          (key, unused, values, stamp) -> {
            assertEquals(Long.MIN_VALUE, stamp);
            var actual = new StockSortedMap<>(values, order);
            assertEquals(expected.firstKey(), actual.firstKey());
            assertEquals(expected.lastKey(), actual.lastKey());
            var lower = keys.toBinaryRow(GenericRowData.of(1024L)).copy();
            assertEquals(1024, actual.headMap(lower).size());
            assertEquals(1027, actual.tailMap(lower).size());
            assertEquals(
                1, actual.subMap(lower, keys.toBinaryRow(GenericRowData.of(1025L)).copy()).size());
            var serializer =
                new TtlSerializer<>(
                    LongSerializer.INSTANCE,
                    new SortedMapSerializer<>(order, keys, LongSerializer.INSTANCE));
            var left = new DataOutputSerializer(64);
            var right = new DataOutputSerializer(64);
            serializer.serialize(new TtlValue<>(expected, stamp), left);
            serializer.serialize(new TtlValue<>(actual, stamp), right);
            assertArrayEquals(left.getCopyOfBuffer(), right.getCopyOfBuffer());
          });
    }
    assertEquals(baseline, TaskOffHeapMemory.reservedBytes());
    try (var files = java.nio.file.Files.list(directory)) {
      assertEquals(0, files.count());
    }
  }

  @Test
  void unorderedOrDuplicateCountersCannotReachTheReleasedSerializer() {
    for (var rows :
        List.of(
            List.of(Tuple2.of(2L, 1L), Tuple2.of(1L, 2L)),
            List.of(Tuple2.of(1L, 1L), Tuple2.of(1L, 2L)))) {
      var map = new StockSortedMap<>(rows, Comparator.<Long>naturalOrder());
      assertThrows(IllegalStateException.class, () -> new ArrayList<>(map.entrySet()));
    }
    var empty = new StockSortedMap<Long, Long>(List.of(), Comparator.naturalOrder());
    assertTrue(empty.isEmpty());
    assertThrows(java.util.NoSuchElementException.class, empty::firstKey);
  }
}
