package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Production emitFinal with a prebuilt flush: C Data export/import and shaping, no aggregation. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowOutputShapingBenchmark {
  private static final int WARMUP = Integer.getInteger("shaping.warmup", 20);
  private static final int RUNS = Integer.getInteger("shaping.runs", 31);

  static final class Fixture extends NativeRowWindowOperatorCore
      implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
    VectorSchemaRoot prepared;
    final int keyCode;

    Fixture(RowType output, int keyCode, int precision) {
      super("window-output-fixture", 1000, 1000, new int[] {0}, new int[] {0},
          "UTC", true, "UTC", output, new int[] {precision}, 128);
      this.keyCode = keyCode;
    }

    @Override protected long createHandle() { return 0; }
    @Override protected void closeHandle() {}
    @Override protected long stateBytesHandle() { return 0; }
    @Override protected void flushPending() {}
    @Override protected void emitClosedWindows(long watermark) { emitFinal(watermark, new int[] {keyCode}); }
    @Override public void processElement(StreamRecord<ArrowBatch> record) { throw new UnsupportedOperationException(); }

    @Override protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
      try (ArrowArray array = ArrowArray.wrap(arrayAddress); ArrowSchema schema = ArrowSchema.wrap(schemaAddress)) {
        Data.exportVectorSchemaRoot(allocator, prepared, dictionaries, array, schema);
      }
    }

    void prepare(List<RowData> rows, RowType type) {
      prepared = RowDataArrowConverter.write(rows, type, allocator);
    }
  }

  private static boolean selected(String property, String value) {
    String filter = System.getProperty(property);
    if (filter == null) return true;
    return Arrays.stream(filter.split(",")).map(String::trim).anyMatch(value::equals);
  }

  @Test
  void productionEmitFinalWithoutAggregateCompute() throws Exception {
    int fixtures = 0;
    for (int properties : new int[] {0, 2}) {
      if (!selected("shaping.properties", Integer.toString(properties))) continue;
      for (var shape : WindowOutputHandoffBenchmark.Shape.values()) {
        if (!selected("shaping.shapes", shape.name())) continue;
        for (int rows : new int[] {1, 1024, 16384}) {
          if (!selected("shaping.rows", Integer.toString(rows))) continue;
          fixtures++;
          double[] samples = new double[RUNS];
          long sharedResultSamples = 0;
          for (int trial = 0; trial < WARMUP + RUNS; trial++) {
            RowType output = properties == 0
                ? RowType.of(new LogicalType[] {shape.type, new BigIntType()},
                    new String[] {"group_key", "total"})
                : RowType.of(new LogicalType[] {
                    shape.type, new BigIntType(), new TimestampType(3), new TimestampType(3)},
                    new String[] {"group_key", "total", "window_start", "window_end"});
            LogicalType nativeKey = shape == WindowOutputHandoffBenchmark.Shape.INT ? new BigIntType() : shape.type;
            RowType nativeType = RowType.of(new LogicalType[] {
                nativeKey, new BigIntType(), new BigIntType(), new BigIntType()},
                new String[] {"key0", "result0", "window_start", "window_end"});
            var operator = new Fixture(output, shape.code, shape.precision);
            try (var harness = new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
                operator, batch -> 0, Types.INT, 128, 1, 0)) {
              harness.setup(new ArrowBatchSerializer());
              harness.open();
              List<RowData> expected = new ArrayList<>(rows);
              List<RowData> nativeRows = new ArrayList<>(rows);
              for (int row = 0; row < rows; row++) {
                Object key = shape.key(row);
                if (shape == WindowOutputHandoffBenchmark.Shape.STRING && row % 7 == 0) key = null;
                Object value = row % 5 == 0 ? null : (long) row;
                expected.add(properties == 0 ? GenericRowData.of(key, value)
                    : GenericRowData.of(key, value, TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(1000)));
                Object carriedKey = shape == WindowOutputHandoffBenchmark.Shape.INT ? (long) row : key;
                nativeRows.add(GenericRowData.of(carriedKey, value, 0L, 1000L));
              }
              operator.prepare(nativeRows, nativeType);
              long sourceAddress = operator.prepared.getVector("result0").getDataBuffer().memoryAddress();
              long start = System.nanoTime();
              operator.emitClosedWindows(999);
              double nanos = System.nanoTime() - start;
              if (trial >= WARMUP) samples[trial - WARMUP] = nanos;
              // Source roots close before output is read, proving the consumer owns surviving buffers.
              operator.prepared.close();
              int count = 0;
              while (!harness.getOutput().isEmpty()) {
                Object event = harness.getOutput().poll();
                if (!(event instanceof StreamRecord<?>)) continue;
                try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
                  assertEquals(tech.streamfusion.arrow.ArrowConversion.toArrowSchema(output), root.getSchema());
                  if (trial >= WARMUP && root.getVector(1).getDataBuffer().memoryAddress() == sourceAddress) sharedResultSamples++;
                  List<RowData> actual = RowDataArrowConverter.read(root, output);
                  assertEquals(expected.size(), actual.size());
                  for (int row = 0; row < actual.size(); row++) {
                    RowData wanted = expected.get(row);
                    RowData got = actual.get(row);
                    assertEquals(wanted.isNullAt(0), got.isNullAt(0));
                    if (!wanted.isNullAt(0)) {
                      switch (shape) {
                        case STRING: assertEquals(wanted.getString(0), got.getString(0)); break;
                        case INT: assertEquals(wanted.getInt(0), got.getInt(0)); break;
                        case TIMESTAMP: assertEquals(wanted.getTimestamp(0, 9), got.getTimestamp(0, 9)); break;
                        default: assertEquals(wanted.getLong(0), got.getLong(0));
                      }
                    }
                    assertEquals(wanted.isNullAt(1), got.isNullAt(1));
                    if (!wanted.isNullAt(1)) assertEquals(wanted.getLong(1), got.getLong(1));
                    if (properties == 2) {
                      assertEquals(wanted.getTimestamp(2, 3), got.getTimestamp(2, 3));
                      assertEquals(wanted.getTimestamp(3, 3), got.getTimestamp(3, 3));
                    }
                  }
                  count += actual.size();
                }
              }
              assertEquals(rows, count);
            }
          }
          double[] sorted = samples.clone();
          Arrays.sort(sorted);
          System.out.printf(Locale.ROOT,
              "[window-output-shaping] properties=%d shape=%s rows=%d median_ns=%.0f ns_per_row=%.2f shared_result_samples=%d/%d samples_ns=%s%n",
              properties, shape, rows, sorted[sorted.length / 2], sorted[sorted.length / 2] / rows,
              sharedResultSamples, RUNS, Arrays.toString(samples));
        }
      }
    }
    assertTrue(fixtures > 0, "shaping selectors must match at least one fixture");
  }
}
