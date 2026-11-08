package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class CanonicalProcessingTimeWindowOperatorTest {
  private static final long BASE = CanonicalSharedWindowInteropTest.BASE;

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean distinct : new boolean[] {false, true})
            cases.add(Arguments.of(shape, zone, rocks, distinct));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} {1} rocks={2} distinct={3}")
  @MethodSource("cases")
  void automaticImportRearmsIdleTimersAndSurvivesANativeCheckpoint(
      String shape, String zone, boolean rocks, boolean distinct) throws Exception {
    verifyAutomaticImport(shape, zone, rocks, distinct, false);
  }

  static Stream<Arguments> intCases() {
    return Stream.concat(
        cases(),
        Stream.of("UTC", "+05:45", "Europe/Berlin", "America/New_York")
            .flatMap(
                zone ->
                    Stream.of(false, true)
                        .flatMap(
                            rocks ->
                                Stream.of(false, true)
                                    .map(
                                        distinct ->
                                            Arguments.of("SESSION", zone, rocks, distinct)))));
  }

  @ParameterizedTest
  @MethodSource("intCases")
  void stockIntKeysKeepTheirWidthAcrossCanonicalAndNativeRecovery(
      String shape, String zone, boolean rocks, boolean distinct) throws Exception {
    verifyAutomaticImport(shape, zone, rocks, distinct, true);
  }

  private void verifyAutomaticImport(
      String shape, String zone, boolean rocks, boolean distinct, boolean integerKey)
      throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(
            shape, zone, false, false, distinct, 0, true, integerKey);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      for (long time : new long[] {100, 200, 800}) {
        source.setProcessingTime(BASE + time);
        plan.send(source, input(-1L, time == 200 ? null : 5L, time));
        plan.send(source, input(null, 7L, time));
      }
      source.processWatermark(
          new Watermark(
              plan.session ? BASE + 500 : CanonicalSharedWindowInteropTest.processingWatermark()));
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(BASE + 800);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(BASE + 800);
      nativeRun.open();
      assertEquals(List.of(), output(plan, nativeRun));
      assertTrue(nativeRun.getProcessingTimeService().getNumActiveTimers() > 0);
      // A canonical re-export before input must remain readable by a stock operator.
      nativeRun.prepareSnapshotPreBarrier(2);
      var immediate =
          nativeRun
              .snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(immediate);
        stockAgain.setProcessingTime(BASE + 800);
        stockAgain.open();
        for (long time : new long[] {999, 1200, 1999}) {
          reference.setProcessingTime(BASE + time);
          stockAgain.setProcessingTime(BASE + time);
          nativeRun.setProcessingTime(BASE + time);
          var expected = plan.output(reference);
          assertEquals(expected, plan.output(stockAgain));
          assertEquals(expected, output(plan, nativeRun));
          if (time == 1200) {
            var row = input(-1L, 5L, time);
            plan.send(reference, row);
            plan.send(stockAgain, row);
            send(plan, nativeRun, row);
          }
        }
      }
      nativeRun.prepareSnapshotPreBarrier(3);
      var checkpoint = nativeRun.snapshot(3, 0);
      assertTrue(
          checkpoint.getManagedKeyedState().stream()
              .noneMatch(
                  handle ->
                      handle instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
      try (var resumed = nativeHarness(plan, rocks)) {
        resumed.initializeState(checkpoint);
        resumed.setProcessingTime(BASE + 1999);
        resumed.open();
        for (long time : new long[] {2999, 3999, 7999}) {
          reference.setProcessingTime(BASE + time);
          resumed.setProcessingTime(BASE + time);
          var expected = plan.output(reference);
          assertEquals(expected, output(plan, resumed), "checkpoint continuation " + time);
        }
        resumed.prepareSnapshotPreBarrier(4);
        var drained =
            resumed
                .snapshotWithLocalState(
                    4, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState();
        try (var stockDrained = plan.stock(!rocks)) {
          stockDrained.initializeState(drained);
          stockDrained.setProcessingTime(BASE + 7999);
          stockDrained.open();
          stockDrained.setProcessingTime(BASE + 9999);
          assertEquals(List.of(), plan.output(stockDrained));
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void overdueImportedTimersFireWithoutInput(
      String shape, String zone, boolean rocks, boolean distinct) throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(
            shape, zone, false, false, distinct, 0, true);
    try (var source = plan.stock(rocks);
        var reference = plan.stock(!rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      source.setProcessingTime(BASE + 100);
      plan.send(source, input(-1L, 5L, 100));
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(BASE + 9999);
      reference.open();
      reference.setProcessingTime(BASE + 9999);
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(BASE + 9999);
      nativeRun.open();
      var expected = plan.output(reference);
      assertFalse(expected.isEmpty(), "The oracle must fire retained state");
      assertEquals(expected, output(plan, nativeRun));
      nativeRun.setProcessingTime(BASE + 19999);
      assertEquals(List.of(), output(plan, nativeRun));
    }
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      CanonicalSharedWindowInteropTest.StockPlan plan, boolean rocks) throws Exception {
    int[] values = new int[plan.kinds.length];
    for (int i = 0; i < values.length; i++)
      values[i] = plan.starValues[i] ? -1 : plan.input.getFieldIndex("v");
    int[] keys = plan.keyless ? new int[0] : new int[] {plan.input.getFieldIndex("k")};
    int[] keyTypes = plan.keyless ? new int[0] : new int[] {plan.integerKey ? 2 : 0};
    int[] keyLayout =
        plan.keyless
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    NativeRowWindowOperatorCore core =
        plan.session
            ? new NativeColumnarSessionWindowAggregateOperator(
                1000,
                plan.processingTime ? -1 : plan.input.getFieldIndex("rt"),
                values,
                keys,
                keyTypes,
                new int[plan.kinds.length],
                plan.kinds,
                plan.zone.getId(),
                true,
                plan.zone.getId(),
                plan.output,
                plan.processingTime,
                keyLayout,
                128)
            : new NativeColumnarWindowAggregateOperator(
                plan.cumulative,
                3000,
                plan.tumbling ? 3000 : 1000,
                plan.processingTime ? -1 : plan.input.getFieldIndex("rt"),
                values,
                keys,
                keyTypes,
                new int[plan.kinds.length],
                plan.kinds,
                plan.zone.getId(),
                true,
                plan.zone.getId(),
                plan.output,
                plan.processingTime,
                keyLayout,
                128);
    @SuppressWarnings("unchecked")
    var operator =
        (org.apache.flink.streaming.api.operators.OneInputStreamOperator<ArrowBatch, ArrowBatch>)
            core;
    var contract = plan.contract();
    core.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        StreamExecutionEnvironment.getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalProcessingTimeWindowOperatorTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(
                config, CanonicalProcessingTimeWindowOperatorTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  static CanonicalSharedWindowInteropTest.Input input(Long key, Long value, long time) {
    return new CanonicalSharedWindowInteropTest.Input(key, value, time);
  }

  static void send(
      CanonicalSharedWindowInteropTest.StockPlan plan,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      CanonicalSharedWindowInteropTest.Input row)
      throws Exception {
    var fields = new Object[plan.input.getFieldCount()];
    for (int i = 0; i < fields.length; i++) {
      String name = plan.input.getFieldNames().get(i);
      fields[i] =
          name.equals("k")
              ? plan.keyValue(row.key)
              : name.equals("v") ? row.value : TimestampData.fromEpochMillis(BASE + row.timestamp);
    }
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(GenericRowData.of(fields)), plan.input, NativeAllocator.SHARED))));
  }

  static List<String> output(
      CanonicalSharedWindowInteropTest.StockPlan plan,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    var values = new ArrayList<String>();
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (!(event instanceof StreamRecord<?>)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        var reader = ArrowConversion.createArrowReader(root, plan.output);
        for (int i = 0; i < root.getRowCount(); i++) {
          RowData row = reader.read(i);
          int keyFields = plan.keyless ? 0 : 1;
          var text = new StringBuilder(plan.keyless ? "global" : plan.keyText(row));
          for (int j = 0; j < plan.visibleAggregates; j++)
            text.append(':')
                .append(row.isNullAt(keyFields + j) ? "null" : row.getLong(keyFields + j));
          for (int j = keyFields + plan.visibleAggregates; j < row.getArity(); j++)
            text.append(':').append(row.getTimestamp(j, 3).getMillisecond());
          values.add(text.toString());
        }
      }
    }
    values.sort(String::compareTo);
    return values;
  }
}
