package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Measures the production native final flush, C Data import, and Java output shaping. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowOutputHandoffBenchmark {
  private static final int WARMUP = Integer.getInteger("handoff.warmup", 3);
  private static final int RUNS = Integer.getInteger("handoff.runs", 9);

  enum Shape {
    BIGINT(new BigIntType(), 0, -1),
    NULLABLE_BIGINT(new BigIntType(), 0, -1),
    STRING(new VarCharType(VarCharType.MAX_LENGTH), 3, -1),
    INT(new IntType(), 2, -1),
    TIMESTAMP(new TimestampType(9), 1009, 9);

    final LogicalType type;
    final int code;
    final int precision;

    Shape(LogicalType type, int code, int precision) {
      this.type = type;
      this.code = code;
      this.precision = precision;
    }

    Object key(int row) {
      switch (this) {
        case STRING: return StringData.fromString("group-" + row + "-" + "x".repeat(64));
        case INT: return row;
        case TIMESTAMP: return TimestampData.fromEpochMillis(row, 123456);
        default: return (long) row;
      }
    }
  }

  private static int stateKey() {
    for (int key = 0; ; key++) {
      if (KeyGroupRangeAssignment.assignToKeyGroup(key, 128) == 0) return key;
    }
  }

  @Test
  void flushAndShapeFinalWindows() throws Exception {
    for (Shape shape : Shape.values()) {
      for (int rows : new int[] {1, 1024, 16384}) {
        double[] samples = new double[RUNS];
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          RowType input = RowType.of(shape.type, new BigIntType(), new LocalZonedTimestampType(3));
          RowType output = RowType.of(
              new LogicalType[] {shape.type, new BigIntType(), new TimestampType(3), new TimestampType(3)},
              new String[] {"group_key", "total", "window_start", "window_end"});
          var operator = new NativeColumnarWindowAggregateOperator(
              false, 1000, 1000, 2, new int[] {1}, new int[] {0}, new int[] {shape.code},
              new int[] {0}, new int[] {0}, "UTC", output, false,
              new int[] {shape.precision}, 128);
          try (BufferAllocator allocator = new RootAllocator();
              var harness = new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
                  operator, batch -> stateKey(), Types.INT, 128, 1, 0)) {
            harness.setup(new ArrowBatchSerializer());
            harness.open();
            List<RowData> data = new ArrayList<>(rows);
            for (int row = 0; row < rows; row++) {
              Object value = shape == Shape.NULLABLE_BIGINT && row % 7 == 0 ? null : (long) row;
              data.add(GenericRowData.of(shape.key(row), value, TimestampData.fromEpochMillis(1)));
            }
            harness.processElement(new StreamRecord<>(new ArrowBatch(
                RowDataArrowConverter.write(data, input, allocator))));
            // Input extraction/state construction belong outside the measured boundary.
            operator.flushPending();
            long start = System.nanoTime();
            harness.processWatermark(new Watermark(999));
            double nanos = System.nanoTime() - start;
            if (trial >= WARMUP) samples[trial - WARMUP] = nanos;
            int count = 0;
            while (!harness.getOutput().isEmpty()) {
              Object event = harness.getOutput().poll();
              if (!(event instanceof StreamRecord<?>)) continue;
              try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
                assertEquals(tech.streamfusion.arrow.ArrowConversion.toArrowSchema(output), root.getSchema());
                for (RowData result : RowDataArrowConverter.read(root, output)) {
                  int key;
                  switch (shape) {
                    case STRING: key = Integer.parseInt(result.getString(0).toString().split("-")[1]); break;
                    case INT: key = result.getInt(0); break;
                    case TIMESTAMP:
                      var timestamp = result.getTimestamp(0, 9);
                      assertEquals(123456, timestamp.getNanoOfMillisecond());
                      key = (int) timestamp.getMillisecond(); break;
                    default: key = (int) result.getLong(0);
                  }
                  boolean isNull = shape == Shape.NULLABLE_BIGINT && key % 7 == 0;
                  assertEquals(isNull, result.isNullAt(1));
                  if (!isNull) assertEquals(key, result.getLong(1));
                  assertEquals(0, result.getTimestamp(2, 3).getMillisecond());
                  assertEquals(1000, result.getTimestamp(3, 3).getMillisecond());
                  count++;
                }
              }
            }
            assertEquals(rows, count);
          }
        }
        double[] ordered = samples.clone();
        Arrays.sort(ordered);
        System.out.printf(Locale.ROOT,
            "[window-output-handoff] shape=%s rows=%d median_ns=%.0f ns_per_row=%.2f samples_ns=%s%n",
            shape, rows, ordered[ordered.length / 2], ordered[ordered.length / 2] / rows,
            Arrays.toString(samples));
      }
    }
  }
}
