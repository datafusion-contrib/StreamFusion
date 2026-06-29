package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The columnar temporal-table-join operator buffers a probe (left) stream and a versioned build
 * (right) changelog, and on a watermark emits, for each probe row the watermark has passed, the build
 * version valid at the probe row's event time (the latest accumulate version with rightTime &lt;=
 * probe time). A {@code -D}/{@code -U} version means "no row at that time" — a LEFT join then
 * null-pads. Emission is watermark-gated, so the result is deterministic and value-checked here.
 */
class NativeTemporalJoinOperatorTest {

  // Probe: [k BIGINT, amount BIGINT, rt TIMESTAMP_LTZ(3)]; build: [k BIGINT, rate BIGINT, rt ...].
  private static final RowType PROBE =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "amount", "rt"});
  private static final RowType BUILD =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "rate", "rt"});
  // Output: probe columns then build columns.
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3)
          },
          new String[] {"k", "amount", "rt", "k0", "rate", "rt0"});

  private static NativeTemporalJoinOperator operator(int joinType) {
    return new NativeTemporalJoinOperator(
        new int[] {0}, new int[] {0}, 2, 2, joinType, PROBE, BUILD, EncodedPredicate.NONE);
  }

  @Test
  void joinsTheVersionValidAtTheProbeTime() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator(0))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Build versions: key 1 rate 10@100 then rate 20@300 (+U); key 2 rate 99@100.
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.INSERT, 1, 10, 100))));
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.UPDATE_AFTER, 1, 20, 300))));
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.INSERT, 2, 99, 100))));

      // Probe rows pick the version valid at their time.
      harness.processElement1(
          new StreamRecord<>(probe(allocator, prow(1, 1, 200), prow(1, 2, 500), prow(2, 3, 150))));
      assertEquals(List.of(), collect(harness)); // nothing emitted before a watermark

      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      assertEquals(
          sorted(
              List.of(
                  row(RowKind.INSERT, 1L, 1L, 200L, 1L, 10L, 100L), // 200 -> version @100
                  row(RowKind.INSERT, 1L, 2L, 500L, 1L, 20L, 300L), // 500 -> version @300
                  row(RowKind.INSERT, 2L, 3L, 150L, 2L, 99L, 100L))),
          sorted(collect(harness)));
      closeForwarded(harness);
    }
  }

  @Test
  void leftJoinNullPadsWhenNoValidVersion() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator(1))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // key 1: rate 10@100, rate 20@300. key 2: rate 99@100 then deleted @400.
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.INSERT, 1, 10, 100))));
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.UPDATE_AFTER, 1, 20, 300))));
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.INSERT, 2, 99, 100))));
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.DELETE, 2, 99, 400))));

      harness.processElement1(
          new StreamRecord<>(
              probe(
                  allocator,
                  prow(1, 1, 50), // before any version -> null pad
                  prow(1, 2, 350), // -> version @300
                  prow(2, 3, 500)))); // latest version @400 is a delete -> null pad

      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      assertEquals(
          sorted(
              List.of(
                  row(RowKind.INSERT, 1L, 1L, 50L, null, null, null),
                  row(RowKind.INSERT, 1L, 2L, 350L, 1L, 20L, 300L),
                  row(RowKind.INSERT, 2L, 3L, 500L, null, null, null))),
          sorted(collect(harness)));
      closeForwarded(harness);
    }
  }

  @Test
  void buffersProbeRowsUntilTheWatermarkPasses() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator(0))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.INSERT, 1, 10, 100))));
      harness.processElement1(new StreamRecord<>(probe(allocator, prow(1, 1, 500))));

      // Watermark 200 has not passed the probe row's time (500); nothing emitted, row stays buffered.
      harness.processBothWatermarks(new Watermark(200));
      assertEquals(List.of(), collect(harness));

      // A newer version (rate 20 @300) arrives; at end of input the probe row resolves to it.
      harness.processElement2(
          new StreamRecord<>(build(allocator, brow(RowKind.UPDATE_AFTER, 1, 20, 300))));
      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      assertEquals(
          List.of(row(RowKind.INSERT, 1L, 1L, 500L, 1L, 20L, 300L)), collect(harness));
      closeForwarded(harness);
    }
  }

  private static RowData prow(long k, long amount, long rtMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, k);
    row.setField(1, amount);
    row.setField(2, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static RowData brow(RowKind kind, long k, long rate, long rtMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setRowKind(kind);
    row.setField(0, k);
    row.setField(1, rate);
    row.setField(2, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch probe(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), PROBE, allocator));
  }

  private static ArrowBatch build(BufferAllocator allocator, RowData... rows) {
    // Carry the changelog kind: the build side is a versioned (+I/+U/-D) stream.
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), BUILD, allocator, true));
  }

  private static List<Object> row(
      RowKind kind, Long lk, Long la, Long lrt, Long rk, Long rate, Long rrt) {
    List<Object> r = new ArrayList<>();
    r.add(kind);
    r.add(lk);
    r.add(la);
    r.add(lrt);
    r.add(rk);
    r.add(rate);
    r.add(rrt);
    return r;
  }

  private static List<List<Object>> collect(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(
                row(
                    r.getRowKind(),
                    r.getLong(0),
                    r.getLong(1),
                    r.getTimestamp(2, 3).getMillisecond(),
                    r.isNullAt(3) ? null : r.getLong(3),
                    r.isNullAt(4) ? null : r.getLong(4),
                    r.isNullAt(5) ? null : r.getTimestamp(5, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    List<List<Object>> copy = new ArrayList<>(rows);
    copy.sort(Comparator.comparing(Object::toString));
    return copy;
  }

  private static void closeForwarded(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }
}
