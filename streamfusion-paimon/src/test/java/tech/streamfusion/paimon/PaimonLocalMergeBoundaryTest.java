package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.LocalMergeOperator;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released LocalMergeOperator's row-kind and buffer cases, including Arrow batch boundaries. */
class PaimonLocalMergeBoundaryTest {
  @ParameterizedTest
  @ValueSource(strings = {"hash", "sort", "ignore-delete", "ignore-update-before", "rowkind"})
  void matchesReleasedOperatorThroughBufferCheckpointAndEndInputFlushes(String mode)
      throws Exception {
    boolean fixed = mode.equals("hash");
    var options =
        new java.util.HashMap<>(
            Map.of(
                "bucket",
                "1",
                "changelog-producer",
                "input",
                "local-merge-buffer-size",
                fixed ? "2 mb" : "64 kb",
                "page-size",
                "4 kb"));
    if (mode.startsWith("ignore-")) options.put(mode, "true");
    if (mode.equals("rowkind")) options.put("rowkind.field", "op");
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.STRING().notNull())
            .column("v", fixed ? DataTypes.INT() : DataTypes.STRING());
    if (!fixed) schema.column("op", DataTypes.STRING());
    var path = new Path(Files.createTempDirectory("paimon-local-boundary").toUri());
    new SchemaManager(LocalFileIO.create(), path)
        .createTable(
            schema.primaryKey("id").options(PaimonTestTables.fileOptions(options)).build());
    var table = FileStoreTableFactory.create(LocalFileIO.create(), path);
    var type = table.rowType();
    var flinkType = LogicalTypeConversion.toLogicalType(type);
    List<List<String>> results = new ArrayList<>();
    for (boolean nativeMode : new boolean[] {false, true}) {
      List<String> output = new ArrayList<>();
      try (var allocator = new RootAllocator();
          var stock =
              new OneInputStreamOperatorTestHarness<InternalRow, InternalRow>(
                  new LocalMergeOperator.Factory(table.schema()));
          var ours =
              new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
                  new NativePaimonLocalMergeOperator(table))) {
        stock.setup(new InternalRowTypeSerializer(type));
        ours.setup(new ArrowBatchSerializer());
        if (nativeMode) ours.open();
        else stock.open();
        int count = fixed ? 30000 : 180;
        for (int offset = 0; offset < count; offset += 17) {
          List<InternalRow> input = new ArrayList<>();
          for (int i = offset; i < Math.min(offset + 17, count); i++) {
            RowKind kind =
                i % 11 == 0
                    ? RowKind.DELETE
                    : i % 7 == 0
                        ? RowKind.UPDATE_BEFORE
                        : i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER;
            Object value =
                fixed ? i : BinaryString.fromString(i % 23 == 0 ? "α🙂".repeat(30000) : "v" + i);
            GenericRow row =
                fixed
                    ? GenericRow.ofKind(kind, BinaryString.fromString("key" + i), value)
                    : GenericRow.ofKind(
                        mode.equals("rowkind") ? RowKind.INSERT : kind,
                        BinaryString.fromString("key" + i % 3),
                        value,
                        BinaryString.fromString(kind.shortString()));
            input.add(row);
          }
          if (nativeMode) {
            ours.processElement(
                new StreamRecord<>(
                    new ArrowBatch(
                        RowDataArrowConverter.write(
                            input.stream()
                                .<org.apache.flink.table.data.RowData>map(FlinkRowData::new)
                                .collect(ListCollectors.toList()),
                            flinkType,
                            allocator,
                            true))));
          } else {
            for (var row : input) stock.processElement(new StreamRecord<>(row));
          }
          if (offset == 34) {
            if (nativeMode) {
              ours.processWatermark(new Watermark(123));
              ours.prepareSnapshotPreBarrier(1);
            } else {
              stock.processWatermark(new Watermark(123));
              stock.prepareSnapshotPreBarrier(1);
            }
          }
        }
        if (nativeMode) {
          ((BoundedOneInput) ours.getOneInputOperator()).endInput();
          ours.prepareSnapshotPreBarrier(2);
          for (Object event : ours.getOutput()) {
            if (event instanceof StreamRecord<?>) {
              StreamRecord<?> record = ((StreamRecord<?>) event);

              try (var root = ((ArrowBatch) record.getValue()).root()) {
                var kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
                int i = 0;
                for (InternalRow row : new ArrowBatchBundle(root, flinkType)) {
                  output.add(
                      RowKind.fromByteValue(kinds.get(i++)).shortString()
                          + PaimonTestTables.render(row, type));
                }
              }
            } else output.add(event.toString());
          }
          ours.getOutput().clear();
        } else {
          ((BoundedOneInput) stock.getOneInputOperator()).endInput();
          stock.prepareSnapshotPreBarrier(2);
          for (Object event : stock.getOutput()) {
            if (event instanceof StreamRecord<?>) {
              StreamRecord<?> record = ((StreamRecord<?>) event);

              var row = (InternalRow) record.getValue();
              output.add(row.getRowKind().shortString() + PaimonTestTables.render(row, type));
            } else output.add(event.toString());
          }
        }
      }
      output.sort(String::compareTo);
      results.add(output);
    }
    assertEquals(results.get(0), results.get(1));
  }
}
