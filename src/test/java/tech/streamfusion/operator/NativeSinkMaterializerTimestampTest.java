package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeSinkMaterializerTimestampTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  static Stream<Arguments> modes() {
    var modes = new ArrayList<Arguments>();
    for (String strategy : List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE"))
      for (boolean rocks : new boolean[] {false, true}) modes.add(Arguments.of(strategy, rocks));
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void sampledClockAndCandidateAgesSurviveOrdinaryNativeRecovery(String strategy, boolean rocks)
      throws Exception {
    OperatorSubtaskState checkpoint;
    try (var before = harness(strategy, rocks)) {
      before.open();
      push(before, 100, RowKind.INSERT, 10, 100);
      push(before, 200, RowKind.INSERT, 20, 200);
      push(before, 300, RowKind.INSERT, 30, 300);
      push(before, 400, RowKind.UPDATE_AFTER, 30, 310);
      assertEquals(List.of(100L, 200L, 400L), timestamps(before));
      checkpoint = before.snapshot(1, 400);
    }
    try (var restored = harness(strategy, rocks)) {
      restored.initializeState(checkpoint);
      restored.open();
      restored.setProcessingTime(10000);
      assertEquals(List.of(100L, 200L, 400L), timestamps(restored));
      assertEquals(List.of(100L, 200L, 400L), timestamps(restored), "No-input re-export");
      push(restored, 10100, RowKind.DELETE, 999, 0);
      assertEquals(List.of(100L, 200L, 400L), timestamps(restored));
      push(restored, 10200, RowKind.DELETE, 30, 310);
      assertEquals(List.of(100L, 200L), timestamps(restored));
      push(restored, 10300, RowKind.INSERT, 40, 400);
      assertEquals(List.of(100L, 200L, 10300L), timestamps(restored));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      String strategy, boolean rocks) throws Exception {
    var operator =
        new NativeSinkMaterializer(new int[] {0}, new int[] {-1}, new int[] {1}, 0, 128) {
          @Override
          protected int strategyMode() {
            switch (strategy) {
              case "LEGACY":
                return 0;
              case "VALUE":
                return 1;
              case "MAP":
                return rocks ? 4 : 2;
              case "ADAPTIVE":
                return rocks ? 5 : 3;
              default:
                throw new AssertionError(strategy);
            }
          }

          @Override
          protected int[] matchTimestampPrecisions() {
            return new int[] {-1};
          }

          @Override
          protected long adaptiveHighThreshold() {
            return 3;
          }

          @Override
          protected long adaptiveLowThreshold() {
            return 1;
          }
        };
    int stateKey = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> stateKey, Types.INT, 128, 1, 0);
    if (rocks)
      h.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeSinkMaterializerTimestampTest.class.getClassLoader()));
    h.setup(new ArrowBatchSerializer());
    return h;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
      long clock,
      RowKind kind,
      long inputKey,
      long value)
      throws Exception {
    h.setProcessingTime(clock);
    List<RowData> rows = List.of(GenericRowData.ofKind(kind, 1L, inputKey, value));
    h.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, TYPE, NativeAllocator.SHARED, true))));
    while (!h.getOutput().isEmpty()) {
      Object event = h.getOutput().poll();
      if (event instanceof StreamRecord)
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
    }
  }

  private static List<Long> timestamps(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) throws Exception {
    var operator = (NativeSinkMaterializer) h.getOperator();
    var result = new ArrayList<Long>();
    try (var allocator = new RootAllocator()) {
      for (byte[] bytes : Native.snapshotSinkMaterializer(operator.handle, 128)) {
        try (var reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(bytes, 4, bytes.length - 4), allocator)) {
          while (reader.loadNextBatch()) {
            var root = reader.getVectorSchemaRoot();
            assertEquals(
                "3",
                root.getSchema().getCustomMetadata().get("streamfusion.sink-materializer.version"));
            var shapes = (VarCharVector) root.getVector("__sink_shape__");
            for (int i = 0; i < root.getRowCount(); i++) {
              if (shapes.isNull(i)) continue;
              var shape = new ObjectMapper().readTree(shapes.get(i));
              assertEquals(shape.get("count").intValue(), shape.get("timestamps").size());
              for (var time : shape.get("timestamps")) result.add(time.longValue());
            }
          }
        }
      }
    }
    Collections.sort(result);
    return result;
  }
}
