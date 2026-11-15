package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalSinkMaterializerCaptureFailureTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String strategy : List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE_SMALL", "ADAPTIVE_LARGE"))
      for (boolean rocks : new boolean[] {false, true})
        for (boolean imported : new boolean[] {false, true})
          for (int section = 0; section < 3; section++) {
            if (section == 2 && !List.of("MAP", "ADAPTIVE_LARGE").contains(strategy)) continue;
            cases.add(Arguments.of(strategy, rocks, imported, section));
          }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void failedJvmCaptureReleasesArrowAndLeavesLiveStateUsable(
      String strategy, boolean rocks, boolean imported, int section) throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var nativeOp = CanonicalSinkMaterializerInteropTest.nativeHarness(strategy, true, rocks)) {
      nativeOp.open();
      nativeOp.setProcessingTime(100);
      push(nativeOp, GenericRowData.of(1L, 10L, 100L));
      push(nativeOp, GenericRowData.of(1L, 40L, 400L));
      push(nativeOp, GenericRowData.of(1L, 50L, 500L));
      push(nativeOp, GenericRowData.of(1L, 20L, 200L));
      push(nativeOp, GenericRowData.of(2L, 30L, 300L));
      output(nativeOp);
      var before = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
      var digest = digest(before, strategy, rocks);
      long handle = ((NativeSinkMaterializer) nativeOp.getOperator()).handle;
      var error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  Native.exportSinkMaterializerCanonical(
                      handle,
                      section,
                      (a, s) -> {
                        if (imported) {
                          try (var array = ArrowArray.wrap(a);
                              var schema = ArrowSchema.wrap(s);
                              var root =
                                  Data.importVectorSchemaRoot(
                                      NativeAllocator.SHARED,
                                      array,
                                      schema,
                                      NativeAllocator.DICTIONARIES)) {
                            assertTrue(root.getRowCount() > 0);
                            throw new IllegalStateException("capture fault:" + section);
                          }
                        }
                        throw new IllegalStateException("capture fault:" + section);
                      }));
      assertEquals("capture fault:" + section, error.getMessage());
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(
          digest, digest(CanonicalTemporalSortInteropTest.snapshot(nativeOp), strategy, rocks));

      var retract = GenericRowData.ofKind(RowKind.DELETE, 1L, 20L, 200L);
      List<String> expected;
      OperatorSubtaskState continued;
      try (var stock =
          CanonicalSinkMaterializerProtocolTest.harness(strategy, true, rocks, false)) {
        stock.initializeState(before);
        stock.open();
        stock.processElement(
            new StreamRecord<>(
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                        CanonicalSinkMaterializerProtocolTest.TYPE)
                    .toBinaryRow(retract)
                    .copy()));
        expected = CanonicalSinkMaterializerProtocolTest.output(stock);
        continued = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
      }
      push(nativeOp, retract);
      assertFalse(expected.isEmpty());
      assertEquals(expected, output(nativeOp));
      var restored = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
      try (var expectedStock =
              CanonicalSinkMaterializerProtocolTest.harness(strategy, true, rocks, false);
          var actualStock =
              CanonicalSinkMaterializerProtocolTest.harness(strategy, true, rocks, false)) {
        expectedStock.initializeState(continued);
        actualStock.initializeState(restored);
        expectedStock.open();
        actualStock.open();
        for (var row :
            List.of(
                GenericRowData.ofKind(RowKind.DELETE, 1L, 10L, 100L),
                GenericRowData.ofKind(RowKind.DELETE, 1L, 40L, 400L),
                GenericRowData.ofKind(RowKind.DELETE, 1L, 50L, 500L),
                GenericRowData.ofKind(RowKind.DELETE, 2L, 30L, 300L))) {
          var serializer =
              new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                  CanonicalSinkMaterializerProtocolTest.TYPE);
          expectedStock.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
          actualStock.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
        }
        assertEquals(
            CanonicalSinkMaterializerProtocolTest.output(expectedStock),
            CanonicalSinkMaterializerProtocolTest.output(actualStock));
        assertEquals(
            CanonicalSinkMaterializerProtocolTest.digest(expectedStock),
            CanonicalSinkMaterializerProtocolTest.digest(actualStock));
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static CanonicalSinkMaterializerProtocolTest.Digest digest(
      OperatorSubtaskState state, String strategy, boolean rocks) throws Exception {
    try (var stock = CanonicalSinkMaterializerProtocolTest.harness(strategy, true, rocks, false)) {
      stock.initializeState(state);
      stock.open();
      return CanonicalSinkMaterializerProtocolTest.digest(stock);
    }
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowData row)
      throws Exception {
    h.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(row),
                    CanonicalSinkMaterializerProtocolTest.TYPE,
                    NativeAllocator.SHARED,
                    true))));
  }

  private static List<String> output(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) {
    var result = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      var event = h.getOutput().poll();
      if (!(event instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        for (var row : RowDataArrowConverter.read(root, CanonicalSinkMaterializerProtocolTest.TYPE))
          result.add(
              row.getRowKind().shortString()
                  + ":"
                  + row.getLong(0)
                  + ":"
                  + row.getLong(1)
                  + ":"
                  + (row.isNullAt(2) ? "null" : row.getLong(2)));
      }
    }
    return result;
  }
}
