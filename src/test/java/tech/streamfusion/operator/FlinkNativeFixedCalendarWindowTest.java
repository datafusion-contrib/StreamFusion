package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class FlinkNativeFixedCalendarWindowTest {
  private static final long DAY = 86_400_000;
  static final RowType INPUT = RowType.of(new BigIntType(), new LocalZonedTimestampType(3));
  static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new BigIntType(),
          new TimestampType(3),
          new TimestampType(3),
          new LocalZonedTimestampType(3));

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    List<Arguments> cases = new ArrayList<>();
    for (String zone : List.of("GMT+08:00", "GMT-08:00", "GMT+05:45")) {
      for (String shape : List.of("tumble", "hop", "cumulate")) {
        for (boolean proctime : new boolean[] {false, true}) {
          for (boolean negative : new boolean[] {false, true}) {
            if (proctime && negative) continue;
            for (boolean writer : new boolean[] {false, true}) {
              for (boolean reader : new boolean[] {false, true})
                cases.add(Arguments.of(zone, shape, negative, proctime, writer, reader));
            }
          }
        }
      }
    }
    return cases.stream();
  }

  static Stream<Arguments> tvfCases() {
    var cases = new ArrayList<Arguments>();
    for (String zone : List.of("GMT+08:00", "GMT-08:00", "GMT+05:45")) {
      for (String shape : List.of("tumble", "hop", "cumulate")) {
        cases.add(Arguments.of(zone, shape, false, false));
        cases.add(Arguments.of(zone, shape, true, false));
        cases.add(Arguments.of(zone, shape, false, true));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("tvfCases")
  void releasedTvfAssignsTheSameCalendarBounds(
      String zone, String shape, boolean negative, boolean proctime) throws Exception {
    long base = negative ? -2 * DAY : Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    var tvfOutput =
        RowType.of(
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new TimestampType(3),
            new TimestampType(3),
            new LocalZonedTimestampType(3));
    var shift = ZoneId.of(zone);
    String name =
        shape.equals("tumble")
            ? "TumblingWindowAssigner"
            : shape.equals("hop") ? "SlidingWindowAssigner" : "CumulativeWindowAssigner";
    Class<?> assigners =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.groupwindow.assigners." + name,
            "org.apache.flink.table.runtime.operators.window.assigners." + name);
    Object assigner =
        shape.equals("tumble")
            ? assigners.getMethod("of", Duration.class).invoke(null, Duration.ofMillis(DAY))
            : assigners
                .getMethod("of", Duration.class, Duration.class)
                .invoke(null, Duration.ofMillis(DAY), Duration.ofMillis(DAY / 4));
    if (proctime) assigner = assigners.getMethod("withProcessingTime").invoke(assigner);
    Class<?> operators =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.tvf.operator.AlignedWindowTableFunctionOperator",
            "org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator");
    Object[] arguments =
        operators.getConstructors()[0].getParameterCount() == 4
            ? new Object[] {assigner, 1, 3, shift}
            : new Object[] {assigner, 1, shift};
    @SuppressWarnings("unchecked")
    var stockOperator =
        (OneInputStreamOperator<RowData, RowData>)
            operators.getConstructors()[0].newInstance(arguments);
    var nativeOperator =
        new NativeWindowTableFunctionOperator(
            1,
            DAY,
            shape.equals("tumble") ? DAY : DAY / 4,
            shape.equals("cumulate"),
            proctime,
            WindowTimeDomain.offsetMillis(zone));
    try (var allocator = new RootAllocator();
        var stock = new OneInputStreamOperatorTestHarness<>(stockOperator);
        var nativeTvf = new OneInputStreamOperatorTestHarness<>(nativeOperator)) {
      stock.setup(new RowDataSerializer(tvfOutput));
      stock.open();
      nativeTvf.setup(new ArrowBatchSerializer());
      nativeTvf.open();
      for (long timestamp : new long[] {base + 1, base + DAY / 3, base + DAY - 1}) {
        if (proctime) {
          stock.setProcessingTime(timestamp);
          nativeTvf.setProcessingTime(timestamp);
        }
        var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(timestamp));
        stock.processElement(new StreamRecord<>(row));
        nativeTvf.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
        var expected = new ArrayList<List<Long>>();
        for (var record : stock.extractOutputStreamRecords())
          expected.add(tvfValues(record.getValue()));
        stock.getOutput().clear();
        var actual = new ArrayList<List<Long>>();
        for (var record : nativeTvf.extractOutputStreamRecords()) {
          try (var root = record.getValue().root()) {
            for (var output : RowDataArrowConverter.read(root, tvfOutput))
              actual.add(tvfValues(output));
          }
        }
        nativeTvf.getOutput().clear();
        expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        assertEquals(expected, actual, "assignment=" + timestamp);
      }
    }
  }

  private static List<Long> tvfValues(RowData row) {
    return List.of(
        row.getLong(0),
        row.getTimestamp(1, 3).getMillisecond(),
        row.getTimestamp(2, 3).getMillisecond(),
        row.getTimestamp(3, 3).getMillisecond(),
        row.getTimestamp(4, 3).getMillisecond());
  }

  static Stream<Arguments> twoPhaseCases() {
    return cases().filter(arguments -> !(boolean) arguments.get()[3]);
  }

  @ParameterizedTest
  @MethodSource("twoPhaseCases")
  void releasedFixedCalendarWindowsMatchLocalGlobalRecovery(
      String zone, String shape, boolean negative, boolean proctime, boolean writer, boolean reader)
      throws Exception {
    compareCalendarRecovery(zone, shape, negative, proctime, writer, reader, true);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedFixedCalendarWindowsMatchNativeFiringAndBackendRecovery(
      String zone, String shape, boolean negative, boolean proctime, boolean writer, boolean reader)
      throws Exception {
    compareCalendarRecovery(zone, shape, negative, proctime, writer, reader, false);
  }

  private static void compareCalendarRecovery(
      String zone,
      String shape,
      boolean negative,
      boolean proctime,
      boolean writer,
      boolean reader,
      boolean twoPhase)
      throws Exception {
    long base = negative ? -2 * DAY : Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    var first = GenericRowData.of(1L, TimestampData.fromEpochMillis(base + 1000));
    var second = GenericRowData.of(1L, TimestampData.fromEpochMillis(base + DAY / 3));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    var expected = new ArrayList<List<List<Long>>>();
    try (var allocator = new RootAllocator();
        var stock = stock(zone, shape, proctime);
        var before =
            twoPhase
                ? globalHarness(zone, shape, writer)
                : nativeHarness(zone, shape, proctime, writer);
        var local = twoPhase ? localHarness(zone, shape) : null) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      before.setup(new ArrowBatchSerializer());
      before.open();
      if (local != null) {
        local.setup(new ArrowBatchSerializer());
        local.open();
      }
      var input = local == null ? before : local;
      if (proctime) {
        stock.setProcessingTime(base + 1000);
        before.setProcessingTime(base + 1000);
      }
      stock.processElement(new StreamRecord<>(first));
      input.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(first), INPUT, allocator))));
      if (proctime) {
        stock.setProcessingTime(base + DAY / 3);
        before.setProcessingTime(base + DAY / 3);
      }
      stock.processElement(new StreamRecord<>(second));
      input.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(second), INPUT, allocator))));
      if (local != null) {
        local.prepareSnapshotPreBarrier(1);
        for (var record : local.extractOutputStreamRecords())
          before.processElement(new StreamRecord<>(record.getValue()));
        local.getOutput().clear();
      }
      // Processing-time timers may have closed earlier slices between the two inputs.
      assertEquals(drainStock(stock), drainNative(before));
      snapshot =
          writer == reader
              ? before.snapshot(1, 1)
              : before
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      // Flink's window buffers need a barrier flush before these controlled timer steps.
      stock.prepareSnapshotPreBarrier(1);
      for (long watermark = base + DAY / 3 + 1;
          watermark <= base + 2 * DAY;
          watermark += DAY / 12) {
        if (proctime) stock.setProcessingTime(watermark);
        else stock.processWatermark(new Watermark(watermark));
        var step = drainStock(stock);
        step.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        expected.add(step);
      }
    }
    try (var after =
        twoPhase
            ? globalHarness(zone, shape, reader)
            : nativeHarness(zone, shape, proctime, reader)) {
      after.setup(new ArrowBatchSerializer());
      after.initializeState(snapshot);
      if (proctime) after.setProcessingTime(base + DAY / 3);
      after.open();
      int step = 0;
      for (long watermark = base + DAY / 3 + 1;
          watermark <= base + 2 * DAY;
          watermark += DAY / 12) {
        if (proctime) after.setProcessingTime(watermark);
        else after.processWatermark(new Watermark(watermark));
        var actual = drainNative(after);
        actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        assertEquals(expected.get(step++), actual, "progress=" + watermark);
      }
    }
  }

  static int compareRows(List<Long> left, List<Long> right) {
    for (int i = 0; i < left.size(); i++) {
      int cmp = java.util.Comparator.nullsFirst(Long::compareTo).compare(left.get(i), right.get(i));
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(String zone, String shape, boolean proctime, boolean rocks) throws Exception {
    return nativeHarness(zone, shape, proctime, rocks, DAY, shape.equals("tumble") ? DAY : DAY / 4);
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      String zone, String shape, boolean proctime, boolean rocks, long windowSize, long windowSlide)
      throws Exception {
    return nativeCountHarness(zone, shape, proctime, rocks, windowSize, windowSlide, 3);
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeCountHarness(
      String zone,
      String shape,
      boolean proctime,
      boolean rocks,
      long windowSize,
      long windowSlide,
      int kind)
      throws Exception {
    var operator =
        new NativeColumnarWindowAggregateOperator(
            shape.equals("cumulate"),
            windowSize,
            windowSlide,
            1,
            new int[] {-1},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new int[] {kind},
            zone,
            true,
            zone,
            OUTPUT,
            proctime,
            new int[] {-1},
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeFixedCalendarWindowTest.class.getClassLoader()));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      localHarness(String zone, String shape) throws Exception {
    return localHarness(zone, shape.equals("tumble") ? DAY : DAY / 4);
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> localHarness(
      String zone, long slice) throws Exception {
    var operator =
        new NativeColumnarLocalWindowAggregateOperator(
            slice,
            slice,
            1,
            -1,
            -1,
            new int[] {-1},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new int[] {3},
            zone,
            new int[] {-1},
            128);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, ignored -> 0, Types.INT, 128, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      globalHarness(String zone, String shape, boolean rocks) throws Exception {
    return globalHarness(zone, shape, rocks, DAY, shape.equals("tumble") ? DAY : DAY / 4);
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> globalHarness(
      String zone, String shape, boolean rocks, long size, long slide) throws Exception {
    var operator =
        new NativeColumnarGlobalWindowAggregateOperator(
            size,
            slide,
            shape.equals("cumulate"),
            new int[] {0},
            new int[] {0},
            new int[] {3},
            zone,
            true,
            zone,
            OUTPUT,
            new int[] {-1},
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeFixedCalendarWindowTest.class.getClassLoader()));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      String zone, String shape, boolean proctime) throws Exception {
    return stock(zone, shape, proctime, DAY, shape.equals("tumble") ? DAY : DAY / 4);
  }

  @SuppressWarnings("unchecked")
  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      String zone, String shape, boolean proctime, long windowSize, long windowSlide)
      throws Exception {
    Class<?> builders =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.aggregate.window.WindowAggOperatorBuilder",
            "org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder");
    Object builder = builders.getMethod("builder").invoke(null);
    Class<?> assigners =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners",
            "org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners");
    ZoneId shift = ZoneId.of(zone);
    String factory =
        shape.equals("tumble") ? "tumbling" : shape.equals("hop") ? "hopping" : "cumulative";
    Object assigner =
        shape.equals("tumble")
            ? assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class)
                .invoke(null, proctime ? -1 : 1, shift, Duration.ofMillis(windowSize))
            : assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class, Duration.class)
                .invoke(
                    null,
                    proctime ? -1 : 1,
                    shift,
                    Duration.ofMillis(windowSize),
                    Duration.ofMillis(windowSlide));
    configure(builder, "inputSerializer", new RowDataSerializer(INPUT));
    configure(builder, "keySerializer", new BinaryRowDataSerializer(1));
    configure(builder, "shiftTimeZone", shift);
    configure(builder, "assigner", assigner);
    configure(builder, "countStarIndex", 0);
    var generated =
        new GeneratedNamespaceAggsHandleFunction<Long>("", "", new Object[0]) {
          @Override
          public NamespaceAggsHandleFunction<Long> newInstance(ClassLoader loader) {
            return new Count(assigner, shift, proctime);
          }
        };
    configure(builder, "aggregate", generated, new RowDataSerializer(RowType.of(new BigIntType())));
    var operator =
        (OneInputStreamOperator<RowData, RowData>) builders.getMethod("build").invoke(builder);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        row -> serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
        InternalTypeInfo.of(keyType));
  }

  @SuppressWarnings("unchecked")
  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stockGlobal(
      String zone, String shape, long size, long slide) throws Exception {
    Class<?> builders =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.aggregate.window.WindowAggOperatorBuilder",
            "org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder");
    Class<?> assigners =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners",
            "org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners");
    Object builder = builders.getMethod("builder").invoke(null);
    ZoneId shift = ZoneId.of(zone);
    String factory =
        shape.equals("tumble") ? "tumbling" : shape.equals("hop") ? "hopping" : "cumulative";
    Object inner =
        shape.equals("tumble")
            ? assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class)
                .invoke(null, 1, shift, Duration.ofMillis(size))
            : assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class, Duration.class)
                .invoke(null, 1, shift, Duration.ofMillis(size), Duration.ofMillis(slide));
    Object sliced = null;
    for (var method : assigners.getMethods())
      if (method.getName().equals("sliced")) sliced = method.invoke(null, 2, inner);
    if (sliced == null) throw new NoSuchMethodException("sliced");
    Object assigner = sliced;
    configure(
        builder,
        "inputSerializer",
        new RowDataSerializer(
            RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3))));
    configure(builder, "keySerializer", new BinaryRowDataSerializer(1));
    configure(builder, "shiftTimeZone", shift);
    configure(builder, "assigner", assigner);
    configure(builder, "countStarIndex", 0);
    var handlers = new ArrayList<GeneratedNamespaceAggsHandleFunction<Long>>();
    for (int offset : new int[] {1, 0, 0})
      handlers.add(
          new GeneratedNamespaceAggsHandleFunction<Long>("", "", new Object[0]) {
            @Override
            public NamespaceAggsHandleFunction<Long> newInstance(ClassLoader loader) {
              return new Count(assigner, shift, false, offset);
            }
          });
    configure(
        builder,
        "globalAggregate",
        handlers.get(0),
        handlers.get(1),
        handlers.get(2),
        new RowDataSerializer(RowType.of(new BigIntType())));
    var operator =
        (OneInputStreamOperator<RowData, RowData>) builders.getMethod("build").invoke(builder);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        row -> serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
        InternalTypeInfo.of(keyType));
  }

  @SuppressWarnings("unchecked")
  static OneInputStreamOperatorTestHarness<RowData, RowData> stockLocal(
      String zone, String shape, long size, long slide) throws Exception {
    var assigners =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners",
            "org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners");
    var shift = ZoneId.of(zone);
    String factory =
        shape.equals("tumble") ? "tumbling" : shape.equals("hop") ? "hopping" : "cumulative";
    Object assigner =
        shape.equals("tumble")
            ? assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class)
                .invoke(null, 1, shift, Duration.ofMillis(size))
            : assigners
                .getMethod(factory, int.class, ZoneId.class, Duration.class, Duration.class)
                .invoke(null, 1, shift, Duration.ofMillis(size), Duration.ofMillis(slide));
    var generated =
        new GeneratedNamespaceAggsHandleFunction<Long>("", "", new Object[0]) {
          @Override
          public NamespaceAggsHandleFunction<Long> newInstance(ClassLoader loader) {
            return new Count(assigner, shift, false);
          }
        };
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    var keySelector =
        new org.apache.flink.table.runtime.keyselector.RowDataKeySelector() {
          @Override
          public RowData getKey(RowData row) {
            return serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy();
          }

          @Override
          public InternalTypeInfo<RowData> getProducedType() {
            return InternalTypeInfo.of(keyType);
          }

          public org.apache.flink.table.runtime.keyselector.RowDataKeySelector copy() {
            return this;
          }
        };
    var buffer =
        new org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer
            .LocalFactory(
            new RowDataSerializer(keyType),
            new RowDataSerializer(INPUT),
            new org.apache.flink.table.runtime.operators.aggregate.window.combines.LocalAggCombiner
                .Factory(generated));
    var local =
        Class.forName(
            "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator");
    var operator =
        (OneInputStreamOperator<RowData, RowData>)
            local.getConstructors()[0].newInstance(keySelector, assigner, buffer, shift);
    var harness = new OneInputStreamOperatorTestHarness<>(operator);
    harness
        .getStreamConfig()
        .setManagedMemoryFractionOperatorOfUseCase(
            org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR, 1.0);
    return harness;
  }

  private static Class<?> compatibleClass(String current, String legacy) throws Exception {
    try {
      return Class.forName(current);
    } catch (ClassNotFoundException missing) {
      return Class.forName(legacy);
    }
  }

  private static void configure(Object builder, String name, Object... arguments) throws Exception {
    for (var method : builder.getClass().getMethods()) {
      if (method.getName().equals(name) && method.getParameterCount() == arguments.length) {
        method.invoke(builder, arguments);
        return;
      }
    }
    throw new NoSuchMethodException(name);
  }

  private static List<Long> values(RowData row) {
    return java.util.Arrays.asList(
        row.getLong(0),
        row.getLong(1),
        row.getTimestamp(2, 3).getMillisecond(),
        row.getTimestamp(3, 3).getMillisecond(),
        row.isNullAt(4) ? null : row.getTimestamp(4, 3).getMillisecond());
  }

  static List<List<Long>> drainStock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    var rows = new ArrayList<List<Long>>();
    Object event;
    while ((event = harness.getOutput().poll()) != null)
      if (event instanceof StreamRecord)
        rows.add(values((RowData) ((StreamRecord<?>) event).getValue()));
    return rows;
  }

  static List<List<Long>> drainNative(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    var rows = new ArrayList<List<Long>>();
    Object event;
    while ((event = harness.getOutput().poll()) != null)
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) rows.add(values(row));
        }
      }
    return rows;
  }

  private static final class Count implements NamespaceAggsHandleFunction<Long> {
    private final Object assigner;
    private final ZoneId zone;
    private final boolean proctime;
    private final int mergeOffset;
    private long count;

    private Count(Object assigner, ZoneId zone, boolean proctime) {
      this(assigner, zone, proctime, 0);
    }

    private Count(Object assigner, ZoneId zone, boolean proctime, int mergeOffset) {
      this.mergeOffset = mergeOffset;
      this.assigner = assigner;
      this.zone = zone;
      this.proctime = proctime;
    }

    @Override
    public void open(StateDataViewStore views) {}

    @Override
    public void setAccumulators(Long namespace, RowData accumulators) {
      count = accumulators.getLong(0);
    }

    @Override
    public void accumulate(RowData row) {
      count++;
    }

    @Override
    public void retract(RowData row) {
      count--;
    }

    @Override
    public void merge(Long namespace, RowData accumulators) {
      count += accumulators.getLong(mergeOffset);
    }

    @Override
    public RowData createAccumulators() {
      return GenericRowData.of(0L);
    }

    @Override
    public RowData getAccumulators() {
      return GenericRowData.of(count);
    }

    @Override
    public RowData getValue(Long end) throws Exception {
      long start =
          (long) assigner.getClass().getMethod("getWindowStart", long.class).invoke(assigner, end);
      return GenericRowData.of(
          count,
          TimestampData.fromEpochMillis(start),
          TimestampData.fromEpochMillis(end),
          proctime
              ? null
              : TimestampData.fromEpochMillis(TimeWindowUtil.toEpochMills(end - 1, zone)));
    }

    @Override
    public void cleanup(Long namespace) {}

    @Override
    public void close() {}
  }
}
