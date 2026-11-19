package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.operator.window.stream.WindowTimeDomain;
import tech.streamfusion.state.FlinkRocksDBOptions;

class SessionAdmissionWatermarkTest {
  private static final long BASE = Instant.parse("2024-03-31T00:59:57Z").toEpochMilli();
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean rocks : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (long watermark : new long[] {Long.MAX_VALUE - 1, Long.MAX_VALUE})
            cases.add(Arguments.of(zone, rocks, keyless, watermark));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void admissionDoesNotAdvanceFiringAndRejectedMergesReleaseTheirInput(
      String zoneId, boolean rocks, boolean keyless, long watermark) {
    var zone = ZoneId.of(zoneId);
    long handle = create(rocks, keyless);
    try {
      if (!zone.getRules().isFixedOffset())
        Native.setSessionWindowCalendar(handle, new WindowCalendar(zoneId, true));
      Native.configureSessionCanonicalAdmission(
          handle, Long.MIN_VALUE, new WindowCalendar(zoneId), zone.getRules().isFixedOffset());
      update(handle, keyless, zone, -1, 5, 100);
      assertEquals(List.of(), flush(handle, BASE + 200));
      Native.advanceSessionCanonicalAdmission(handle, watermark);
      Native.advanceSessionCanonicalAdmission(handle, Long.MIN_VALUE);
      if (!keyless) update(handle, false, zone, 2, 99, 200);
      var failure =
          assertThrows(
              UnsupportedOperationException.class, () -> update(handle, keyless, zone, -1, 7, 200));
      assertTrue(failure.getMessage().contains("event-time window"), failure.getMessage());
      assertTrue(
          failure.getMessage().contains("Current watermark: " + watermark), failure.getMessage());
      assertEquals(List.of(), flush(handle, BASE + 1098));
      assertEquals(List.of(5L), flush(handle, BASE + 1099));
      assertEquals(List.of(), flush(handle, Long.MAX_VALUE));
    } finally {
      if (rocks) Native.closeRocksDBSessionAggregator(handle);
      else Native.closeSessionAggregator(handle);
    }
  }

  @Test
  void anUnboundAdmissionCannotBeAdvancedAndMustBeReboundAfterRawRestore() {
    long handle = Native.createSessionAggregator(1000, new int[] {0}, new int[] {0}, 1 << 26);
    byte[] snapshot;
    try {
      assertThrows(
          IllegalStateException.class, () -> Native.advanceSessionCanonicalAdmission(handle, 1));
      Native.configureSessionCanonicalAdmission(handle, 123, new WindowCalendar("UTC"), true);
      snapshot = Native.snapshotSessionAggregator(handle);
    } finally {
      Native.closeSessionAggregator(handle);
    }
    long restored =
        Native.restoreSessionAggregator(1000, new int[] {0}, new int[] {0}, snapshot, 1 << 26);
    try {
      assertThrows(
          IllegalStateException.class,
          () -> Native.advanceSessionCanonicalAdmission(restored, 124));
      Native.configureSessionCanonicalAdmission(restored, 123, new WindowCalendar("UTC"), true);
      Native.advanceSessionCanonicalAdmission(restored, 124);
    } finally {
      Native.closeSessionAggregator(restored);
    }
  }

  private long create(boolean rocks, boolean keyless) {
    return rocks
        ? Native.createRocksDBSessionAggregator(
            1000,
            new int[] {0},
            new int[] {0},
            keyless ? new int[0] : new int[] {0},
            keyless
                ? new int[] {Native.STOCK_KEY_PROJECTION}
                : new int[] {Native.STOCK_KEY_PROJECTION, -1},
            1 << 26,
            directory.resolve("native").toString(),
            128,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            new String[0],
            new String[0],
            0,
            127,
            false,
            new byte[0][],
            Long.MIN_VALUE)
        : Native.createSessionAggregator(1000, new int[] {0}, new int[] {0}, 1 << 26);
  }

  private static void update(
      long handle, boolean keyless, ZoneId zone, long key, long value, long time) {
    var fields = new ArrayList<RowType.RowField>();
    if (!keyless) fields.add(new RowType.RowField("key0", new BigIntType()));
    fields.add(new RowType.RowField("value0", new BigIntType()));
    fields.add(new RowType.RowField("ts", new BigIntType(false)));
    var row = new GenericRowData(fields.size());
    int column = 0;
    if (!keyless) row.setField(column++, key);
    row.setField(column++, value);
    row.setField(
        column,
        zone.getRules().isFixedOffset()
            ? BASE + time
            : TimeWindowUtil.toUtcTimestampMills(BASE + time, zone));
    try (var root =
            RowDataArrowConverter.write(
                List.<RowData>of(row), new RowType(fields), NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.updateSessionAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<Long> flush(long handle, long clock) {
    var values = new ArrayList<Long>();
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Native.flushSessionAggregator(
          handle,
          WindowTimeDomain.closeThreshold(clock, 0),
          array.memoryAddress(),
          schema.memoryAddress());
      try (var root =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
        var result = (BigIntVector) root.getVector("result0");
        for (int i = 0; i < root.getRowCount(); i++) values.add(result.get(i));
      }
    }
    return values;
  }
}
