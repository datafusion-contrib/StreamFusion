package tech.streamfusion;

import tech.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Isolates where the {@code RowData → Arrow} build cost goes (wontdos/28 follow-up): the current
 * converter creates fresh Arrow vectors per batch and lets {@code setSafe} grow them as it writes.
 * This times three build strategies over the same 4096-row batch — fresh + grow (current), fresh +
 * pre-sized (one allocation, no growth reallocs; still a fresh batch so no buffer sharing hazard),
 * and reused vectors ({@code reset} + refill, what Comet does, but unsafe if a downstream retains
 * the batch across the FFI boundary). Tells us how much of the win pre-sizing captures safely.
 * Opt-in: {@code SF_BENCHMARK=true}.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TransposeBenchmark {

  private static final int ROWS = 4096;
  private static final int WARMUP = 2000;
  private static final int RUNS = 5000;

  @Test
  void transposeBuildStrategies() {
    List<RowData> rows = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      GenericRowData row = new GenericRowData(5);
      row.setField(0, (long) (i % 64));
      row.setField(1, (long) i);
      row.setField(2, i * 1.5);
      row.setField(3, StringData.fromString("row-" + i));
      row.setField(4, TimestampData.fromEpochMillis(1_700_000_000_000L + i));
      rows.add(row);
    }

    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new BigIntType(), new BigIntType(), new DoubleType(),
              new VarCharType(VarCharType.MAX_LENGTH), new TimestampType(3)
            },
            new String[] {"k", "v", "d", "s", "ts"});

    try (RootAllocator allocator = new RootAllocator();
        BigIntVector kr = new BigIntVector("k", allocator);
        BigIntVector vr = new BigIntVector("v", allocator);
        Float8Vector dr = new Float8Vector("d", allocator);
        VarCharVector sr = new VarCharVector("s", allocator);
        TimeStampNanoVector tr = new TimeStampNanoVector("ts", allocator)) {

      for (int i = 0; i < WARMUP; i++) {
        sink(converterWrite(rows, rowType, allocator));
        sink(buildFresh(rows, allocator, false));
        sink(buildFresh(rows, allocator, true));
        sink(fillReused(rows, kr, vr, dr, sr, tr));
      }

      long converterNs = 0;
      long growNs = 0;
      long presizedNs = 0;
      long reusedNs = 0;
      for (int i = 0; i < RUNS; i++) {
        long t0 = System.nanoTime();
        sink(converterWrite(rows, rowType, allocator));
        long t1 = System.nanoTime();
        sink(buildFresh(rows, allocator, false));
        long t2 = System.nanoTime();
        sink(buildFresh(rows, allocator, true));
        long t3 = System.nanoTime();
        sink(fillReused(rows, kr, vr, dr, sr, tr));
        long t4 = System.nanoTime();
        converterNs += t1 - t0;
        growNs += t2 - t1;
        presizedNs += t3 - t2;
        reusedNs += t4 - t3;
      }

      System.out.printf("%n=== Row→Arrow build strategies, %d rows (mixed schema) ===%n", ROWS);
      System.out.printf("RowDataArrowConverter.write (operator path): %.1f µs/batch%n", converterNs / 1_000.0 / RUNS);
      System.out.printf("fresh + grow, inlined fill:                  %.1f µs/batch%n", growNs / 1_000.0 / RUNS);
      System.out.printf("fresh + pre-sized (safe):                    %.1f µs/batch%n", presizedNs / 1_000.0 / RUNS);
      System.out.printf("reused vectors (Comet, hazardous):           %.1f µs/batch%n", reusedNs / 1_000.0 / RUNS);
    }
  }

  /** The actual operator path: builds a VectorSchemaRoot via the production converter, then closes
   * it (a fresh batch per call, as RowDataToArrowOperator does today). */
  private static long converterWrite(List<RowData> rows, RowType rowType, RootAllocator allocator) {
    try (VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator)) {
      return root.getRowCount();
    }
  }

  /** Builds a fresh batch of vectors; when {@code presize}, allocates to row count up front so
   * {@code setSafe} never reallocates mid-fill. Closes the vectors (fresh allocation per batch). */
  private static long buildFresh(List<RowData> rows, RootAllocator allocator, boolean presize) {
    int n = rows.size();
    try (BigIntVector k = new BigIntVector("k", allocator);
        BigIntVector v = new BigIntVector("v", allocator);
        Float8Vector d = new Float8Vector("d", allocator);
        VarCharVector s = new VarCharVector("s", allocator);
        TimeStampNanoVector t = new TimeStampNanoVector("ts", allocator)) {
      if (presize) {
        k.allocateNew(n);
        v.allocateNew(n);
        d.allocateNew(n);
        s.setInitialCapacity(n);
        s.allocateNew();
        t.allocateNew(n);
      }
      fill(rows, k, v, d, s, t);
      return k.getValueCount();
    }
  }

  /** Reused vectors: reset and refill in place, no per-batch allocation (Comet's ArrowWriter way). */
  private static long fillReused(
      List<RowData> rows, BigIntVector k, BigIntVector v, Float8Vector d, VarCharVector s,
      TimeStampNanoVector t) {
    k.reset();
    v.reset();
    d.reset();
    s.reset();
    t.reset();
    fill(rows, k, v, d, s, t);
    return k.getValueCount();
  }

  private static void fill(
      List<RowData> rows, BigIntVector k, BigIntVector v, Float8Vector d, VarCharVector s,
      TimeStampNanoVector t) {
    int n = rows.size();
    for (int i = 0; i < n; i++) {
      RowData row = rows.get(i);
      k.setSafe(i, row.getLong(0));
      v.setSafe(i, row.getLong(1));
      d.setSafe(i, row.getDouble(2));
      s.setSafe(i, row.getString(3).toBytes());
      TimestampData ts = row.getTimestamp(4, 3);
      t.setSafe(i, ts.getMillisecond() * 1_000_000L + ts.getNanoOfMillisecond());
    }
    k.setValueCount(n);
    v.setValueCount(n);
    d.setValueCount(n);
    s.setValueCount(n);
    t.setValueCount(n);
  }

  private static long blackhole;

  private static void sink(long value) {
    blackhole += value;
  }
}
