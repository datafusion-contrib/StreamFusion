package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator;
import org.apache.flink.table.runtime.operators.sort.SortOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkOutput;

/** Small-functionality gate; timings exclude preparation, JNI entry/import and output export. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BatchSortAdmissionBenchmark {
  @Test
  void comparePreparedRowsAndArrow() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int count = Integer.getInteger("sf.batch.sort.rows", 65536);
    int width = Integer.getInteger("sf.batch.sort.width", 4);
    int cardinality = Integer.getInteger("sf.batch.sort.cardinality", count);
    int offset = Integer.getInteger("sf.batch.sort.offset", 0);
    int fetch = Integer.getInteger("sf.batch.sort.fetch", -1);
    long end = fetch < 0 ? -1 : (long) offset + fetch;
    boolean nullsFirst =
        Boolean.parseBoolean(
            System.getProperty("sf.batch.sort.nullsFirst", fetch < 0 ? "true" : "false"));
    int warmups = Integer.getInteger("sf.batch.sort.warmups", 3);
    int trials = Integer.getInteger("sf.batch.sort.trials", 7);
    LogicalType[] types = new LogicalType[width];
    Arrays.fill(types, new BigIntType());
    RowType type = RowType.of(types);
    var serializer = new RowDataSerializer(type);
    List<RowData> rows = new ArrayList<>(count);
    Random random = new Random(724719);
    for (int index = 0; index < count; index++) {
      var row = new GenericRowData(width);
      for (int column = 0; column < width; column++) {
        Long value = index % 17 == 0 ? null : Math.floorMod(random.nextLong(), (long) cardinality);
        row.setField(column, value);
      }
      rows.add(serializer.toBinaryRow(row).copy());
    }
    List<Long> sortedKeys = new ArrayList<>();
    for (RowData row : rows) sortedKeys.add(row.isNullAt(0) ? null : row.getLong(0));
    sortedKeys.sort(
        nullsFirst
            ? java.util.Comparator.nullsFirst(Long::compareTo)
            : java.util.Comparator.nullsLast(Long::compareTo));
    int from = Math.min(count, offset), to = fetch < 0 ? count : (int) Math.min(count, end);
    long expectedRows = to - from;
    long checksum = 0;
    for (Long key : sortedKeys.subList(from, to))
      checksum = checksum * 31 + (key == null ? Long.MIN_VALUE : key);
    List<Long> stockTimes = new ArrayList<>();
    List<Long> nativeTimes = new ArrayList<>();
    long previous = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(32L << 20);
    try {
      for (int trial = -warmups; trial < trials; trial++) {
        for (boolean nativeRun :
            trial % 2 == 0 ? new boolean[] {false, true} : new boolean[] {true, false}) {
          long[] consumed = {0, 0};
          var generator =
              new SortCodeGenerator(
                  new Configuration(),
                  getClass().getClassLoader(),
                  type,
                  SortSpec.builder().addField(0, true, !nullsFirst).build());
          OneInputStreamOperator<RowData, RowData> operator =
              fetch >= 0
                  ? new SortLimitOperator(
                      true,
                      offset,
                      end,
                      generator.generateRecordComparator("AdmissionLimitCompare"))
                  : new SortOperator(
                      generator.generateNormalizedKeyComputer("AdmissionSortKey"),
                      generator.generateRecordComparator("AdmissionSortCompare"),
                      128,
                      false,
                      64 * 1024,
                      false);
          try (var environment =
                  new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                      .setManagedMemorySize(32L << 20)
                      .build();
              var harness =
                  new OneInputStreamOperatorTestHarness<RowData, RowData>(
                      operator, serializer, environment)) {
            assertEquals(32L << 20, harness.getEnvironment().getMemoryManager().getMemorySize());
            assertEquals(32L << 20, TaskOffHeapMemory.capacityBytes());
            harness
                .getStreamConfig()
                .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            harness.setup(serializer);
            var output = AbstractStreamOperator.class.getDeclaredField("output");
            output.setAccessible(true);
            output.set(
                operator,
                new FlinkOutput<RowData>(null) {
                  @Override
                  public void collect(StreamRecord<RowData> record) {
                    RowData row = record.getValue();
                    consumed[0]++;
                    consumed[1] =
                        consumed[1] * 31 + (row.isNullAt(0) ? Long.MIN_VALUE : row.getLong(0));
                  }
                });
            harness.open();
            long nanos;
            if (nativeRun) {
              long handle = 0;
              try (var root =
                      RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, false);
                  var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
                  var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
                Data.exportSchema(
                    NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
                try {
                  handle =
                      Native.createBoundedLimitedSorter(
                          schema.memoryAddress(),
                          new int[] {0},
                          new int[] {nullsFirst ? 2 : 0},
                          harness.getEnvironment().getIOManager().getSpillingDirectoriesPaths(),
                          1024,
                          4L << 20,
                          "sort-admission",
                          offset,
                          end);
                } finally {
                  schema.release();
                }
                Data.exportVectorSchemaRoot(
                    NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array);
                try {
                  Native.pushBoundedSorter(handle, array.memoryAddress());
                } finally {
                  array.release();
                }
                Native.finishBoundedSorter(handle);
                long[] result = Native.consumeBoundedSorterTimed(handle);
                nanos = result[0];
                consumed[0] = result[1];
                consumed[1] = result[2];
              } finally {
                if (handle != 0) Native.closeBoundedSorter(handle);
              }
            } else {
              long started = System.nanoTime();
              for (RowData row : rows) operator.processElement(new StreamRecord<>(row));
              ((BoundedOneInput) operator).endInput();
              nanos = System.nanoTime() - started;
            }
            assertEquals(expectedRows, consumed[0]);
            assertEquals(checksum, consumed[1], nativeRun ? "native" : "stock");
            if (trial >= 0) (nativeRun ? nativeTimes : stockTimes).add(nanos);
          }
        }
      }
      System.out.printf(
          "[batch-sort-admission] rows=%d width=%d cardinality=%d offset=%d fetch=%d nullEvery=17"
              + " nullsFirst=%s poolBytes=%d warmups=%d trials=%d stockNanos=%s nativeNanos=%s%n",
          count,
          width,
          cardinality,
          offset,
          fetch,
          nullsFirst,
          32L << 20,
          warmups,
          trials,
          stockTimes,
          nativeTimes);
    } finally {
      TaskOffHeapMemory.resetForTests(previous == 0 ? -1 : previous);
    }
  }
}
