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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeRegionalWindowTvfTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new LocalZonedTimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new LocalZonedTimestampType(3),
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
    for (String scenario :
        List.of("gap", "overlap", "negative", "history", "hour_gap", "hour_overlap"))
      for (String shape : List.of("tumble", "hop", "cumulate"))
        for (boolean batch : new boolean[] {false, true}) {
          cases.add(Arguments.of(scenario, shape, batch, false));
          if (!scenario.equals("negative") && !scenario.equals("history"))
            cases.add(Arguments.of(scenario, shape, batch, true));
        }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void regionalTvfKeepsPayloadKindsNullsAndDistinctWindowTimeConversion(
      String scenario, String shape, boolean batch, boolean proctime) throws Exception {
    String zone = scenario.equals("negative") ? "Asia/Shanghai" : "America/Los_Angeles";
    long size =
        scenario.equals("history") ? 1 : scenario.startsWith("hour") ? 3_600_000 : 86_400_000;
    long slide = shape.equals("tumble") || size == 1 ? size : size / 4;
    long[] times =
        scenario.equals("history")
            ? times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z")
            : scenario.contains("gap")
                ? times("2024-03-10T09:50:00Z", "2024-03-10T10:10:00Z")
                : scenario.contains("overlap")
                    ? times("2024-11-03T08:30:00Z", "2024-11-03T09:30:00Z")
                    : times("1969-12-31T23:30:00Z", "1969-12-31T23:45:00Z");
    var rows =
        List.<RowData>of(
            row(1, times[0], RowKind.INSERT),
            row(2, times[1], RowKind.UPDATE_BEFORE),
            row(3, null, RowKind.UPDATE_AFTER),
            row(4, times[0], RowKind.DELETE));
    try (var allocator = new RootAllocator();
        var stock = stock(zone, shape, size, slide, proctime);
        var nativeOperator =
            new OneInputStreamOperatorTestHarness<>(
                new NativeWindowTableFunctionOperator(
                    1, size, slide, shape.equals("cumulate"), proctime, zone))) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      if (proctime) {
        stock.setProcessingTime(times[0]);
        nativeOperator.setProcessingTime(times[0]);
      }
      var groups =
          batch
              ? List.of(rows)
              : rows.stream().map(List::of).collect(java.util.stream.Collectors.toList());
      for (var group : groups) {
        for (var row : group) stock.processElement(new StreamRecord<>(row));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(group, INPUT, allocator, true))));
        var expected = new ArrayList<List<Object>>();
        for (var record : stock.extractOutputStreamRecords())
          expected.add(values(record.getValue()));
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
        assertEquals(expected, actual);
      }
    }
  }

  private static RowData row(long key, Long time, RowKind kind) {
    var row = GenericRowData.of(key, time == null ? null : TimestampData.fromEpochMillis(time));
    row.setRowKind(kind);
    return row;
  }

  private static List<Object> values(RowData row) {
    return Arrays.asList(
        row.getRowKind(),
        row.getLong(0),
        row.isNullAt(1) ? null : row.getTimestamp(1, 3).getMillisecond(),
        row.getTimestamp(2, 3).getMillisecond(),
        row.getTimestamp(3, 3).getMillisecond(),
        row.getTimestamp(4, 3).getMillisecond());
  }

  private static long[] times(String... values) {
    return Arrays.stream(values).mapToLong(value -> Instant.parse(value).toEpochMilli()).toArray();
  }

  @SuppressWarnings("unchecked")
  private static OneInputStreamOperatorTestHarness<RowData, RowData> stock(
      String zone, String shape, long size, long slide, boolean proctime) throws Exception {
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
            ? assigners.getMethod("of", Duration.class).invoke(null, Duration.ofMillis(size))
            : assigners
                .getMethod("of", Duration.class, Duration.class)
                .invoke(null, Duration.ofMillis(size), Duration.ofMillis(slide));
    if (proctime) assigner = assigners.getMethod("withProcessingTime").invoke(assigner);
    Class<?> operators =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.window.tvf.operator.AlignedWindowTableFunctionOperator",
            "org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator");
    Object[] arguments =
        operators.getConstructors()[0].getParameterCount() == 4
            ? new Object[] {assigner, 1, 3, ZoneId.of(zone)}
            : new Object[] {assigner, 1, ZoneId.of(zone)};
    return new OneInputStreamOperatorTestHarness<>(
        (OneInputStreamOperator<RowData, RowData>)
            operators.getConstructors()[0].newInstance(arguments));
  }

  private static Class<?> compatibleClass(String current, String legacy) throws Exception {
    try {
      return Class.forName(current);
    } catch (ClassNotFoundException missing) {
      return Class.forName(legacy);
    }
  }
}
