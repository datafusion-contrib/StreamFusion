package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class FlinkNativeRegionalSessionWindowTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new LocalZonedTimestampType(3));
  private static final RowType OUTPUT =
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
    var cases = new ArrayList<Arguments>();
    for (String scenario : List.of("overlap", "gap", "history", "negative", "staggered")) {
      for (boolean writer : new boolean[] {false, true}) {
        for (boolean reader : new boolean[] {false, true}) {
          cases.add(Arguments.of(scenario, writer, reader, false));
          if (!scenario.equals("history") && !scenario.equals("negative"))
            cases.add(Arguments.of(scenario, writer, reader, true));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void regionalSessionConnectivityFiringAndRecoveryMatchReleasedFlink(
      String scenario, boolean writer, boolean reader, boolean proctime) throws Exception {
    var sequence = sequence(scenario);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(sequence.zone, sequence.gap, writer, proctime);
        var nativeOperator = nativeHarness(sequence.zone, sequence.gap, writer, proctime)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (int i = 0; i < sequence.times.length; i++) {
        if (proctime) compareClock(stock, nativeOperator, sequence.times[i]);
        long key = scenario.equals("history") || scenario.equals("staggered") ? i + 1 : 1;
        stock.processElement(new StreamRecord<>(GenericRowData.of(key, sequence.times[i])));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        List.of(
                            GenericRowData.of(
                                key, TimestampData.fromEpochMillis(sequence.times[i]))),
                        INPUT,
                        allocator))));
      }
      compareOutput(stock, nativeOperator, "input");
      stockSnapshot =
          writer == reader
              ? stock.snapshot(1, 1)
              : stock
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      nativeSnapshot =
          writer == reader
              ? nativeOperator.snapshot(1, 1)
              : nativeOperator
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      for (long watermark : sequence.watermarks) {
        if (proctime) {
          if (watermark >= sequence.times[sequence.times.length - 1])
            compareClock(stock, nativeOperator, watermark);
        } else compareWatermark(stock, nativeOperator, watermark);
      }
    }
    try (var stock = stock(sequence.zone, sequence.gap, reader, proctime);
        var nativeOperator = nativeHarness(sequence.zone, sequence.gap, reader, proctime)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (long watermark : sequence.watermarks) {
        if (proctime) {
          if (watermark >= sequence.times[sequence.times.length - 1])
            compareClock(stock, nativeOperator, watermark);
        } else compareWatermark(stock, nativeOperator, watermark);
      }
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void historicalMergingPreservesTheReleasedFailure(boolean rocks) throws Exception {
    long first = Instant.parse("1883-11-18T19:59:59.999Z").toEpochMilli();
    long second = Instant.parse("1883-11-18T20:07:02Z").toEpochMilli();
    try (var allocator = new RootAllocator();
        var stock = stock("America/Los_Angeles", 1, rocks, false);
        var nativeOperator = nativeHarness("America/Los_Angeles", 1, rocks, false)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, first)));
      nativeOperator.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, TimestampData.fromEpochMillis(first))),
                      INPUT,
                      allocator))));
      compareWatermark(stock, nativeOperator, second);
      var expected =
          org.junit.jupiter.api.Assertions.assertThrows(
              UnsupportedOperationException.class,
              () -> stock.processElement(new StreamRecord<>(GenericRowData.of(1L, second))));
      var actual =
          org.junit.jupiter.api.Assertions.assertThrows(
              UnsupportedOperationException.class,
              () ->
                  nativeOperator.processElement(
                      new StreamRecord<>(
                          new ArrowBatch(
                              RowDataArrowConverter.write(
                                  List.of(
                                      GenericRowData.of(1L, TimestampData.fromEpochMillis(second))),
                                  INPUT,
                                  allocator)))));
      assertEquals(expected.getMessage(), actual.getMessage());
    }
  }

  private static void compareWatermark(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      long watermark)
      throws Exception {
    stock.processWatermark(new Watermark(watermark));
    nativeOperator.processWatermark(new Watermark(watermark));
    compareOutput(stock, nativeOperator, "watermark=" + watermark);
  }

  private static void compareClock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      long time)
      throws Exception {
    stock.setProcessingTime(time);
    nativeOperator.setProcessingTime(time);
    compareOutput(stock, nativeOperator, "clock=" + time);
  }

  private static void compareOutput(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      String stage) {
    var expected = new ArrayList<List<Object>>();
    for (var record : stock.extractOutputStreamRecords()) expected.add(values(record.getValue()));
    stock.getOutput().clear();
    var actual = new ArrayList<List<Object>>();
    for (var record : nativeOperator.extractOutputStreamRecords()) {
      try (var root = record.getValue().root()) {
        for (var row : RowDataArrowConverter.read(root, OUTPUT)) actual.add(values(row));
      }
    }
    nativeOperator.getOutput().clear();
    expected.sort(java.util.Comparator.comparing(Object::toString));
    actual.sort(java.util.Comparator.comparing(Object::toString));
    assertEquals(expected, actual, stage);
  }

  private static List<Object> values(RowData row) {
    return Arrays.asList(
        row.getRowKind(),
        row.getLong(0),
        row.getLong(1),
        row.getTimestamp(2, 3).getMillisecond(),
        row.getTimestamp(3, 3).getMillisecond(),
        row.isNullAt(4) ? null : row.getTimestamp(4, 3).getMillisecond());
  }

  private static Sequence sequence(String scenario) {
    if (scenario.equals("staggered"))
      return new Sequence("UTC", 500, new long[] {100, 300}, new long[] {598, 599, 798, 799});
    if (scenario.equals("overlap"))
      return new Sequence(
          "America/Los_Angeles",
          15 * 60_000,
          times("2024-11-03T08:20:00Z", "2024-11-03T09:25:00Z"),
          times(
              "2024-11-03T08:34:59.999Z", "2024-11-03T09:39:59.998Z", "2024-11-03T09:39:59.999Z"));
    if (scenario.equals("gap"))
      return new Sequence(
          "America/Los_Angeles",
          20 * 60_000,
          times("2024-03-10T09:50:00Z", "2024-03-10T10:05:00Z"),
          times("2024-03-10T09:59:59.999Z", "2024-03-10T10:00:00Z", "2024-03-10T10:24:59.999Z"));
    if (scenario.equals("history"))
      return new Sequence(
          "America/Los_Angeles",
          1,
          times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z"),
          times("1883-11-18T20:07:01.999Z", "1883-11-18T20:07:02Z", "1883-11-18T20:59:59.999Z"));
    return new Sequence(
        "Asia/Shanghai",
        500,
        times("1969-12-31T23:59:59Z", "1969-12-31T23:59:59.200Z"),
        times("1969-12-31T23:59:59.698Z", "1969-12-31T23:59:59.699Z"));
  }

  private static long[] times(String... times) {
    return Arrays.stream(times).mapToLong(time -> Instant.parse(time).toEpochMilli()).toArray();
  }

  private static final class Sequence {
    final String zone;
    final long gap;
    final long[] times;
    final long[] watermarks;

    Sequence(String zone, long gap, long[] times, long[] watermarks) {
      this.zone = zone;
      this.gap = gap;
      this.times = times;
      this.watermarks = watermarks;
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(String zone, long gap, boolean rocks, boolean proctime) throws Exception {
    var operator =
        new NativeColumnarSessionWindowAggregateOperator(
            gap,
            1,
            new int[] {-1},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new int[] {3},
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
                  new Configuration(),
                  FlinkNativeRegionalSessionWindowTest.class.getClassLoader()));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      String zone, long gap, boolean rocks, boolean proctime) throws Exception {
    Class<?> builders;
    try {
      builders =
          Class.forName(
              "org.apache.flink.table.runtime.operators.window.groupwindow.operator.WindowOperatorBuilder");
    } catch (ClassNotFoundException legacy) {
      builders =
          Class.forName("org.apache.flink.table.runtime.operators.window.WindowOperatorBuilder");
    }
    Object builder = builders.getConstructor().newInstance();
    configure(
        builder,
        "withInputFields",
        (Object) new LogicalType[] {new BigIntType(), new BigIntType()});
    configure(builder, "withShiftTimezone", ZoneId.of(zone));
    configure(builder, "session", Duration.ofMillis(gap));
    if (proctime) configure(builder, "withProcessingTime");
    else configure(builder, "withEventTime", 1);
    RecordEqualiser equaliser = (left, right) -> left.getLong(0) == right.getLong(0);
    Object aggregateBuilder = null;
    for (var method : builders.getMethods()) {
      if (method.getName().equals("aggregate")
          && method.getParameterCount() == 5
          && method.getParameterTypes()[0].isAssignableFrom(Count.class)) {
        aggregateBuilder =
            method.invoke(
                builder,
                new Count(ZoneId.of(zone), proctime),
                equaliser,
                new LogicalType[] {new BigIntType()},
                new LogicalType[] {new BigIntType()},
                new LogicalType[] {
                  new TimestampType(3), new TimestampType(3), new LocalZonedTimestampType(3)
                });
        break;
      }
    }
    if (aggregateBuilder == null) throw new NoSuchMethodException("session aggregate");
    var operator =
        (OneInputStreamOperator<RowData, RowData>)
            aggregateBuilder.getClass().getMethod("build").invoke(aggregateBuilder);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator,
            row -> serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
            InternalTypeInfo.of(keyType));
    if (rocks) {
      Class<?> backend;
      try {
        backend = Class.forName("org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend");
      } catch (ClassNotFoundException legacy) {
        backend =
            Class.forName("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend");
      }
      harness.setStateBackend(
          (org.apache.flink.runtime.state.StateBackend) backend.getConstructor().newInstance());
    }
    return harness;
  }

  private static Object configure(Object builder, String name, Object... arguments)
      throws Exception {
    for (var method : builder.getClass().getMethods()) {
      if (method.getName().equals(name) && method.getParameterCount() == arguments.length)
        return method.invoke(builder, arguments);
    }
    throw new NoSuchMethodException(name);
  }

  private static final class Count implements NamespaceAggsHandleFunction<Object> {
    private final ZoneId zone;
    private final boolean proctime;
    private long count;

    Count(ZoneId zone, boolean proctime) {
      this.zone = zone;
      this.proctime = proctime;
    }

    @Override
    public void open(StateDataViewStore store) {}

    @Override
    public void accumulate(RowData input) {
      count++;
    }

    @Override
    public void retract(RowData input) {
      count--;
    }

    @Override
    public void merge(Object namespace, RowData other) {
      count += other.getLong(0);
    }

    @Override
    public void setAccumulators(Object namespace, RowData accumulators) {
      count = accumulators.getLong(0);
    }

    @Override
    public RowData getAccumulators() {
      return GenericRowData.of(count);
    }

    @Override
    public RowData createAccumulators() {
      return GenericRowData.of(0L);
    }

    @Override
    public RowData getValue(Object window) throws Exception {
      long start = (long) window.getClass().getMethod("getStart").invoke(window);
      long end = (long) window.getClass().getMethod("getEnd").invoke(window);
      return GenericRowData.of(
          count,
          TimestampData.fromEpochMillis(start),
          TimestampData.fromEpochMillis(end),
          proctime
              ? null
              : TimestampData.fromEpochMillis(TimeWindowUtil.toEpochMills(end - 1, zone)));
    }

    @Override
    public void cleanup(Object namespace) {}

    @Override
    public void close() {}
  }
}
