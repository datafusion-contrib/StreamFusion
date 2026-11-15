package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalMaterializerV2AsyncTest {
  static Stream<Arguments> modes() {
    return Stream.of("VALUE", "MAP", "ADAPTIVE_SMALL", "ADAPTIVE_LARGE")
        .flatMap(
            strategy ->
                Stream.of(false, true)
                    .flatMap(
                        key ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(strategy, key, rocks))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void overlappingViewsOutliveTheNativeOwner(String strategy, boolean key, boolean rocks)
      throws Exception {
    var expected = new OperatorSubtaskState[2];
    var pending = new OperatorSnapshotFutures[2];
    var rows = new RowDataSerializer(CanonicalSinkMaterializerProtocolTest.TYPE);
    try (var reference =
            CanonicalSinkMaterializerProtocolTest.harness(strategy, key, false, false);
        var source = CanonicalSinkMaterializerInteropTest.nativeHarness(strategy, key, rocks)) {
      reference.open();
      CanonicalSinkMaterializerProtocolTest.prefix(reference, false);
      expected[0] = CanonicalSinkMaterializerProtocolTest.snapshot(reference);
      source.initializeState(expected[0]);
      source.open();
      var owner = (NativeSinkMaterializer) source.getOperator();
      assertEquals(rocks, owner.directRocksDBState());
      assertEquals(key ? 4 : 5, CanonicalLegacyMaterializerInteropTest.headerRows(owner));
      try {
        pending[0] = CanonicalDistinctWindowAsyncTest.capture(source, 2);
        var bridge = CanonicalLegacyMaterializerInteropTest.row(RowKind.INSERT, 1L, 60, 600);
        source.setProcessingTime(200);
        CanonicalLegacyMaterializerInteropTest.push(source, bridge);
        reference.processElement(new StreamRecord<>(rows.copy(bridge), 200));
        expected[1] = CanonicalSinkMaterializerProtocolTest.snapshot(reference);
        pending[1] = CanonicalDistinctWindowAsyncTest.capture(source, 3);
        CanonicalLegacyMaterializerInteropTest.push(
            source, CanonicalLegacyMaterializerInteropTest.row(RowKind.INSERT, 1L, 70, 700));
        source.prepareSnapshotPreBarrier(4);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(pending[stage]);
          try (var stock =
                  CanonicalSinkMaterializerProtocolTest.harness(strategy, key, false, false);
              var returned =
                  CanonicalSinkMaterializerProtocolTest.harness(strategy, key, true, false)) {
            stock.initializeState(expected[stage]);
            returned.initializeState(saved);
            stock.open();
            returned.open();
            assertEquals(candidates(stock, strategy), candidates(returned, strategy));
            CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, key, false);
            CanonicalSinkMaterializerProtocolTest.suffix(returned, strategy, key, false);
            var output = CanonicalSinkMaterializerProtocolTest.output(stock);
            assertFalse(output.isEmpty());
            assertEquals(output, CanonicalSinkMaterializerProtocolTest.output(returned));
          } finally {
            saved.discardState();
          }
        }
      } finally {
        for (var future : pending) if (future != null) future.cancel();
        CanonicalDistinctWindowAsyncTest.discardOutput(source);
      }
    } finally {
      for (var state : expected) if (state != null) state.discardState();
    }
  }

  static List<String> candidates(
      org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              org.apache.flink.table.data.RowData,
              org.apache.flink.table.data.RowData,
              org.apache.flink.table.data.RowData>
          harness,
      String strategy)
      throws Exception {
    return CanonicalSinkMaterializerProtocolTest.candidates(harness, strategy).stream()
        .map(row -> row.substring(row.indexOf(':')))
        .collect(java.util.stream.Collectors.toList());
  }
}
