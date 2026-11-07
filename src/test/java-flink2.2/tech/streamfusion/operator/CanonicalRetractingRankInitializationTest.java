package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.bytes;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class CanonicalRetractingRankInitializationTest {
  private static final RowType INPUT = CanonicalRetractingRankStateInteropTest.INPUT;

  static Stream<Arguments> traces() {
    return Stream.of("offset", "missing", "empty", "ttl", "ttl-offset", "bound", "bound-ttl")
        .flatMap(
            shape ->
                Stream.of("heap", "rocks")
                    .flatMap(
                        store ->
                            Stream.of("hidden", "rank")
                                .flatMap(
                                    rank ->
                                        Stream.of(false, true)
                                            .map(
                                                first ->
                                                    Arguments.of(
                                                        shape + "-" + store + "-" + rank,
                                                        first)))));
  }

  @ParameterizedTest(name = "{0}, native-first={1}")
  @MethodSource("traces")
  void actualOperatorInitializationAndSuccessiveSwitchesPreserveContinuedChangelog(
      String name, boolean nativeFirst) throws Exception {
    JsonNode trace;
    try (var source = getClass().getResourceAsStream("/retracting-rank/" + name + ".json")) {
      assertNotNull(source);
      trace = new ObjectMapper().readTree(source);
    }
    boolean rocks = trace.path("serializes_values").asBoolean(),
        projected = trace.path("output_rank").asBoolean();
    long ttl = trace.path("ttl_ms").asLong(), start = trace.path("rank_start").asLong();
    boolean variable = trace.path("variable_bound").asBoolean();
    RowType output = projected ? CanonicalRetractingRankStateInteropTest.OUTPUT : INPUT;
    var steps = trace.path("steps");
    int first = trace.path("restore_after").asInt();
    int second = first + (steps.size() - first) / 2;
    try (var reference =
        RetractingRankStockStateTest.harness(true, rocks, projected, ttl, start, variable)) {
      reference.open();
      OperatorSubtaskState snapshot;
      try (var stock =
              RetractingRankStockStateTest.harness(true, rocks, projected, ttl, start, variable);
          var nativeOperator = nativeHarness(trace)) {
        if (nativeFirst) nativeOperator.open();
        else stock.open();
        for (int i = 0; i < first; i++) {
          var step = steps.get(i);
          var expected = stockStep(reference, step);
          assertEquals(
              bytes(output, expected),
              bytes(
                  output,
                  nativeFirst ? nativeStep(nativeOperator, step, output) : stockStep(stock, step)),
              name + " first " + i);
        }
        snapshot = nativeFirst ? snapshot(nativeOperator) : snapshot(stock);
      }
      try (var stock =
              RetractingRankStockStateTest.harness(true, rocks, projected, ttl, start, variable);
          var nativeOperator = nativeHarness(trace)) {
        if (nativeFirst) {
          stock.initializeState(snapshot);
          stock.open();
        } else {
          nativeOperator.initializeState(snapshot);
          nativeOperator.open();
        }
        for (int i = first; i < second; i++) {
          var step = steps.get(i);
          var expected = stockStep(reference, step);
          assertEquals(
              bytes(output, expected),
              bytes(
                  output,
                  nativeFirst ? stockStep(stock, step) : nativeStep(nativeOperator, step, output)),
              name + " second " + i);
        }
        snapshot = nativeFirst ? snapshot(stock) : snapshot(nativeOperator);
      }
      try (var stock =
              RetractingRankStockStateTest.harness(true, rocks, projected, ttl, start, variable);
          var nativeOperator = nativeHarness(trace)) {
        if (nativeFirst) {
          nativeOperator.initializeState(snapshot);
          nativeOperator.open();
        } else {
          stock.initializeState(snapshot);
          stock.open();
        }
        for (int i = second; i < steps.size(); i++) {
          var step = steps.get(i);
          var expected = stockStep(reference, step);
          assertEquals(
              bytes(output, expected),
              bytes(
                  output,
                  nativeFirst ? nativeStep(nativeOperator, step, output) : stockStep(stock, step)),
              name + " third " + i);
        }
      }
    }
  }

  private static RowData row(JsonNode step) {
    var fields = step.path("input").path("fields");
    var row =
        GenericRowData.of(fields.get(0).asLong(), fields.get(1).asLong(), fields.get(2).asLong());
    row.setRowKind(RowKind.fromByteValue((byte) step.path("input").path("kind").asInt()));
    return row;
  }

  private static List<RowData> stockStep(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, JsonNode step)
      throws Exception {
    RetractingRankStockStateTest.time(harness, step.path("now").asLong());
    harness.getOutput().clear();
    RetractingRankStockStateTest.push(harness, row(step));
    return harness.extractOutputValues();
  }

  private static List<RowData> nativeStep(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      JsonNode step,
      RowType output)
      throws Exception {
    CanonicalAppendRankTypedTest.time(harness, step.path("now").asLong());
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(row(step)), INPUT, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(0);
    return CanonicalAppendRankTypedTest.collect(harness, output);
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    return harness
        .snapshotWithLocalState(
            1,
            0,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      JsonNode trace) throws Exception {
    boolean variable = trace.path("variable_bound").asBoolean();
    var contract = CanonicalRetractingRankStateInteropTest.contract(trace);
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            INPUT,
            new int[] {1},
            new int[] {1},
            new int[] {1},
            trace.path("rank_start").asLong() - 1,
            trace.path("rank_end").asLong(),
            trace.path("output_rank").asBoolean(),
            true,
            null,
            null,
            true,
            false,
            -1,
            trace.path("ttl_ms").asLong(),
            128,
            variable ? 2 : -1,
            variable);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (trace.path("serializes_values").asBoolean())
      config.setString(
          "state.backend.type",
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        getClass().getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }
}
