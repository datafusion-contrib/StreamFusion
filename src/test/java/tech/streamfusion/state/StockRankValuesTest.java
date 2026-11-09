package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.runtime.typeutils.SortedMapSerializer;
import org.junit.jupiter.api.Test;

class StockRankValuesTest {
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void releasedPlainAndTtlListAndSortedMapBytesDecodeWithoutWholeCollections() throws Exception {
    for (boolean ttl : new boolean[] {false, true}) {
      var expected = new ArrayList<Long>();
      for (long i = 0; i < 2051; i++) expected.add(i);
      TypeSerializer list = new ListSerializer<>(LongSerializer.INSTANCE);
      TypeSerializer map =
          new SortedMapSerializer<>(
              Comparator.<Long>naturalOrder(), LongSerializer.INSTANCE, LongSerializer.INSTANCE);
      if (ttl) {
        list = new TtlSerializer<>(LongSerializer.INSTANCE, list);
        map = new TtlSerializer<>(LongSerializer.INSTANCE, map);
      }
      long age = ttl ? Long.MIN_VALUE : 0;
      var out = new DataOutputSerializer(64);
      out.writeBoolean(false);
      list.serialize(ttl ? new TtlValue<>(expected, age) : expected, out);
      var rows = StockRankValues.list(out.getCopyOfBuffer(), ttl, LongSerializer.INSTANCE);
      assertEquals(age, rows.written);
      assertEquals(2051, rows.size);
      var actual = new ArrayList<Long>();
      rows.forEach(actual::add);
      assertEquals(expected, actual);
      assertEquals(0L, rows.iterator().next());
      var counts = new TreeMap<Long, Long>();
      counts.put(Long.MIN_VALUE, -2L);
      counts.put(0L, null);
      counts.put(Long.MAX_VALUE, 0L);
      out.clear();
      map.serialize(ttl ? new TtlValue<>(counts, age) : counts, out);
      var decoded = StockRankValues.counters(out.getCopyOfBuffer(), ttl, LongSerializer.INSTANCE);
      assertEquals(age, decoded.written);
      assertEquals(3, decoded.size);
      var observed = new TreeMap<Long, Long>();
      decoded.forEach(pair -> observed.put(pair.f0, pair.f1));
      assertEquals(counts, observed);
    }
  }

  @Test
  void truncatedAndTrailingValuesRejectAndEmptyHeadersStayPresent() throws Exception {
    var out = new DataOutputSerializer(64);
    out.writeBoolean(false);
    new ListSerializer<>(LongSerializer.INSTANCE).serialize(List.of(7L), out);
    byte[] valid = out.getCopyOfBuffer();
    var trailing = java.util.Arrays.copyOf(valid, valid.length + 1);
    assertThrows(
        IllegalArgumentException.class,
        () -> StockRankValues.list(trailing, false, LongSerializer.INSTANCE).iterator().next());
    var shortValue = java.util.Arrays.copyOf(valid, valid.length - 1);
    assertThrows(
        java.io.UncheckedIOException.class,
        () -> StockRankValues.list(shortValue, false, LongSerializer.INSTANCE).iterator().next());
    out.clear();
    out.writeBoolean(false);
    out.writeInt(0);
    assertEquals(
        0, StockRankValues.list(out.getCopyOfBuffer(), false, LongSerializer.INSTANCE).size);
    out.clear();
    out.writeInt(0);
    assertEquals(
        0, StockRankValues.counters(out.getCopyOfBuffer(), false, LongSerializer.INSTANCE).size);
  }
}
