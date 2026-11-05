package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonLocalMergeTest {
  @ParameterizedTest
  @CsvSource({"-1,deduplicate", "-1,first-row", "-2,deduplicate", "-2,first-row"})
  void premergeRoutesDynamicAndPostponedBuckets(int bucket, String engine) throws Exception {
    var warehouse = Files.createTempDirectory("paimon-local-buckets");
    String options =
        "'bucket' = '"
            + bucket
            + "', 'merge-engine' = '"
            + engine
            + "', 'ignore-delete' = 'true', 'sink.parallelism' = '2',"
            + " 'local-merge-buffer-size' = '64 kb', 'page-size' = '4 kb'";
    for (int run = 0; run < 2; run++) {
      var changes =
          PaimonTestTables.changelog((run + 1) * 1500, 120).subList(run * 1500, (run + 1) * 1500);
      var ours =
          PaimonSinkParityTest.writePrimaryKeyFixture(
              warehouse, "native_local", options, 1, true, run == 0, true, changes);
      var stock =
          PaimonSinkParityTest.writePrimaryKeyFixture(
              warehouse, "stock_local", options, 1, false, run == 0, true, changes);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()),
          "run " + run);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        ", 'sequence.field' = 'price'",
        ", 'sequence.field' = 'price', 'sequence.field.sort-order' = 'descending'",
        ", 'merge-engine' = 'first-row', 'ignore-delete' = 'true'",
        ", 'changelog-producer' = 'input'",
        ", 'merge-engine' = 'partial-update', 'ignore-delete' = 'true'",
        ", 'merge-engine' = 'aggregation'"
      })
  void premergePreservesTableResultsAcrossJobsAndBufferFlushes(String extra) throws Exception {
    var warehouse = Files.createTempDirectory("paimon-local-merge");
    String options =
        "'bucket' = '2', 'sink.parallelism' = '2',"
            + " 'local-merge-buffer-size' = '64 kb', 'page-size' = '4 kb'"
            + extra;
    for (int run = 0; run < 2; run++) {
      List<Object[]> changes =
          PaimonTestTables.changelog((run + 1) * 3000, 120).subList(run * 3000, (run + 1) * 3000);
      var ours =
          PaimonSinkParityTest.writePrimaryKeyFixture(
              warehouse, "native_local", options, 1, true, run == 0, true, changes);
      var stock =
          PaimonSinkParityTest.writePrimaryKeyFixture(
              warehouse, "stock_local", options, 1, false, run == 0, true, changes);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()),
          "run " + run);
    }
  }

  @Test
  void partitionKeysRemainDistinctAndWatermarksFollowCheckpointFlushes() throws Exception {
    var type =
        new org.apache.paimon.types.RowType(
            List.of(
                new org.apache.paimon.types.DataField(0, "id", DataTypes.INT().notNull()),
                new org.apache.paimon.types.DataField(1, "v", DataTypes.INT()),
                new org.apache.paimon.types.DataField(2, "pt", DataTypes.STRING().notNull())));
    var schema =
        org.apache.paimon.schema.Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("v", DataTypes.INT())
            .column("pt", DataTypes.STRING().notNull())
            .primaryKey("id", "pt")
            .partitionKeys("pt")
            .options(
                PaimonTestTables.fileOptions(
                    Map.of("bucket", "1", "local-merge-buffer-size", "10 mb")))
            .build();
    var path = new org.apache.paimon.fs.Path(Files.createTempDirectory("local-partitions").toUri());
    PaimonTestCompat.schemaManager(
            org.apache.paimon.fs.local.LocalFileIO.create(), path)
        .createTable(schema);
    var table =
        org.apache.paimon.table.FileStoreTableFactory.create(
            org.apache.paimon.fs.local.LocalFileIO.create(), path);
    RowType flinkType = org.apache.paimon.flink.LogicalTypeConversion.toLogicalType(type);
    var operator = new NativePaimonLocalMergeOperator(table);
    try (var allocator = new RootAllocator();
        var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      List<RowData> rows =
          List.of(
              GenericRowData.of(1, 1, StringData.fromString("a")),
              GenericRowData.of(1, 2, StringData.fromString("b")),
              GenericRowData.of(1, 3, StringData.fromString("a")));
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, flinkType, allocator))));
      harness.processWatermark(new Watermark(10));
      assertTrue(harness.getOutput().isEmpty());
      harness.prepareSnapshotPreBarrier(1);
      Object output = harness.getOutput().poll();
      var batch = (ArrowBatch) ((StreamRecord<?>) output).getValue();
      try (var root = batch.root()) {
        assertEquals(2, root.getRowCount());
        Map<String, Integer> values = new java.util.HashMap<>();
        for (int i = 0; i < root.getRowCount(); i++) {
          values.put(
              root.getVector(2).getObject(i).toString(), (Integer) root.getVector(1).getObject(i));
        }
        assertEquals(Map.of("a", 3, "b", 2), values);
      }
      assertEquals(new Watermark(10), harness.getOutput().poll());
      harness.prepareSnapshotPreBarrier(2);
      assertTrue(harness.getOutput().isEmpty());
    }
  }
}
