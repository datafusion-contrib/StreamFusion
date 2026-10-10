package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonLocalMergeNativeTest {
  static Stream<Map<String, String>> modes() {
    return Stream.of(
        Map.of(),
        Map.of("sequence.field", "seq,seq2"),
        Map.of("sequence.field", "seq", "sequence.field.sort-order", "descending"),
        Map.of("merge-engine", "first-row", "ignore-delete", "true"),
        Map.of("ignore-delete", "true"),
        Map.of("ignore-update-before", "true"),
        Map.of("rowkind.field", "op"));
  }

  static List<InternalRow> input(int count, int keys) {
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RowKind kind = RowKind.values()[i % 4];
      rows.add(
          GenericRow.ofKind(
              kind,
              i % keys,
              i % 5 == 0 ? null : (long) (i % 7),
              i % 3,
              (long) i,
              i % 3 == 0 ? null : BinaryString.fromString("α🙂-" + i),
              i % 2 == 0,
              Decimal.fromUnscaledLong(i % 100, 6, 2),
              new GenericArray(new Integer[] {i, null, -i}),
              BinaryString.fromString(kind.shortString())));
    }
    return rows;
  }

  static FileStoreTable table(Map<String, String> extra) throws Exception {
    var options = new java.util.HashMap<>(extra);
    options.putIfAbsent("local-merge-buffer-size", "10 mb");
    return PaimonMergeEngineTest.table(options);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void nativeSelectionMatchesJavaKindsAndNestedValuesAcrossCheckpointRestore(
      Map<String, String> options) throws Exception {
    var table = table(options);
    assertTrue(NativePaimonLocalMergeOperator.usesNativeMerge(table));
    var type = LogicalTypeConversion.toLogicalType(table.rowType());
    List<List<String>> results = new ArrayList<>();
    for (boolean nativeMerge : new boolean[] {false, true}) {
      List<String> output = new ArrayList<>();
      OperatorSubtaskState checkpoint = null;
      try (var allocator = new RootAllocator()) {
        for (int run = 0; run < 2; run++) {
          var operator = new NativePaimonLocalMergeOperator(table, nativeMerge);
          List<Object> events;
          try (var harness =
              new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
            harness.setup(new ArrowBatchSerializer());
            if (checkpoint != null) harness.initializeState(checkpoint);
            harness.open();
            var rows = input(180, 17);
            if (options.containsKey("rowkind.field")) {
              rows.forEach(row -> row.setRowKind(RowKind.INSERT));
            }
            for (int start = run * 90; start < (run + 1) * 90; start += 17) {
              List<RowData> batch =
                  rows.subList(start, Math.min(start + 17, (run + 1) * 90)).stream()
                      .<RowData>map(FlinkRowData::new)
                      .collect(ListCollectors.toList());
              harness.processElement(
                  new StreamRecord<>(
                      new ArrowBatch(RowDataArrowConverter.write(batch, type, allocator, true))));
            }
            harness.processWatermark(new Watermark(run + 10));
            assertTrue(harness.getOutput().isEmpty());
            harness.prepareSnapshotPreBarrier(run + 1);
            checkpoint = harness.snapshot(run + 1, 0);
            operator.endInput();
            harness.prepareSnapshotPreBarrier(run + 2);
            events = new ArrayList<>(harness.getOutput());
            harness.getOutput().clear();
          }
          // Emitted Arrow buffers must remain readable after the producing operator closes.
          assertEquals(new Watermark(run + 10), events.remove(events.size() - 1));
          for (Object event : events) {
            try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
              assertEquals(type.getFieldCount() + 1, root.getFieldVectors().size());
              var kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
              int rowId = 0;
              for (InternalRow row : new ArrowBatchBundle(root, type)) {
                output.add(
                    run
                        + ":"
                        + RowKind.fromByteValue(kinds.get(rowId++)).shortString()
                        + PaimonTestTables.render(row, table.rowType()));
              }
            }
          }
          assertEquals(0, allocator.getAllocatedMemory());
        }
      }
      output.sort(String::compareTo);
      results.add(output);
    }
    assertEquals(results.get(0), results.get(1));
  }

  @Test
  void budgetFlushAndCancellationReleaseRetainedInputBuffers() throws Exception {
    for (String budget : List.of("64 kb", "10 mb")) {
      var table = table(Map.of("local-merge-buffer-size", budget));
      var type = LogicalTypeConversion.toLogicalType(table.rowType());
      try (var allocator = new RootAllocator()) {
        var operator = new NativePaimonLocalMergeOperator(table);
        try (var harness =
            new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
          harness.setup(new ArrowBatchSerializer());
          harness.open();
          var rows = input(2, 2);
          ((GenericRow) rows.get(0)).setField(4, BinaryString.fromString("α🙂".repeat(30000)));
          harness.processElement(
              new StreamRecord<>(
                  new ArrowBatch(
                      RowDataArrowConverter.write(
                          rows.stream()
                              .<RowData>map(FlinkRowData::new)
                              .collect(ListCollectors.toList()),
                          type,
                          allocator,
                          true))));
          assertEquals(budget.equals("10 mb"), harness.getOutput().isEmpty());
          for (Object event : harness.getOutput()) {
            ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
          }
          harness.getOutput().clear();
          if (budget.equals("10 mb")) assertTrue(allocator.getAllocatedMemory() > 0);
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }

  @Test
  void changelogAndFieldMergeModesRetainJavaGrouping() throws Exception {
    for (var options :
        List.of(
            Map.of("changelog-producer", "input"),
            Map.of("changelog-producer", "lookup"),
            Map.of("changelog-producer", "full-compaction"),
            Map.of("merge-engine", "partial-update"),
            Map.of("merge-engine", "aggregation"))) {
      assertFalse(NativePaimonLocalMergeOperator.usesNativeMerge(table(options)));
    }
  }
}
