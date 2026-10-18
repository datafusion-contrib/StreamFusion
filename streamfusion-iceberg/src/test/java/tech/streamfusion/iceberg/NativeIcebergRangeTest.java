package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;

class NativeIcebergRangeTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void physicalRowGroupMidpointsMatchJavaForEveryTaskBoundary(boolean deletes) throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(
                    TableProperties.FORMAT_VERSION,
                    "2",
                    "write.parquet.row-group-size-bytes",
                    "1024",
                    "write.parquet.page-size-bytes",
                    "512",
                    "write.parquet.page-size-row-limit",
                    "100",
                    "write.parquet.min-row-count-for-page-size-check",
                    "100",
                    "write.parquet.max-row-count-for-page-size-check",
                    "100"),
                temp.resolve("ranges").toUri().toString());
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/data.parquet")),
            table.spec(),
            null);
    try (writer) {
      var record = GenericRecord.create(schema);
      for (int id = 0; id < 1001; id++) {
        record.setField("id", id);
        record.setField("payload", "row-" + id + "é🐍".repeat(30));
        writer.write(record);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    if (deletes) {
      var deleteFactory =
          new GenericFileWriterFactory.Builder(table)
              .dataFileFormat(FileFormat.PARQUET)
              .deleteFileFormat(FileFormat.PARQUET)
              .equalityFieldIds(new int[] {1})
              .equalityDeleteRowSchema(new Schema(schema.findField("id")))
              .build();
      var equality =
          deleteFactory.newEqualityDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/eq.parquet")),
              table.spec(),
              null);
      try (equality) {
        var row = GenericRecord.create(new Schema(schema.findField("id")));
        row.setField("id", 177);
        equality.write(row);
      }
      var position =
          deleteFactory.newPositionDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/pos.parquet")),
              table.spec(),
              null);
      try (position) {
        position.write(
            org.apache.iceberg.deletes.PositionDelete.<org.apache.iceberg.data.Record>create()
                .set(writer.toDataFile().location(), 501));
        position.write(
            org.apache.iceberg.deletes.PositionDelete.<org.apache.iceberg.data.Record>create()
                .set(writer.toDataFile().location(), 999));
      }
      table
          .newRowDelta()
          .addDeletes(equality.toDeleteFile())
          .addDeletes(position.toDeleteFile())
          .commit();
    }
    FileScanTask original;
    try (var tasks = table.newScan().planFiles()) {
      original = tasks.iterator().next();
    }
    var boundaries = new ArrayList<Long>();
    boundaries.add(0L);
    try (var file =
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(original.file().location()),
                new org.apache.hadoop.conf.Configuration()))) {
      var groups = file.getFooter().getBlocks();
      assertTrue(groups.size() > 1, "fixture must have several row groups");
      for (var group : groups) {
        long midpoint = group.getStartingPos() + group.getCompressedSize() / 2;
        boundaries.add(midpoint);
        boundaries.add(midpoint + 1);
      }
    }
    boundaries.add(original.file().fileSizeInBytes());
    boundaries.sort(Long::compare);
    var all = new ArrayList<Integer>();
    for (int index = 1; index < boundaries.size(); index++) {
      long start = boundaries.get(index - 1), length = boundaries.get(index) - start;
      var task = range(original, start, length);
      var expected = new ArrayList<Integer>();
      var stock =
          new RowDataReaderFunction(
              new Configuration(),
              schema,
              schema,
              null,
              true,
              table.io(),
              table.encryption(),
              List.of());
      try (var input =
          stock.apply(
              IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(List.of(task))))) {
        while (input.hasNext()) {
          var records = input.next();
          try {
            while (records.nextSplit() != null) {
              org.apache.iceberg.flink.source.reader.RecordAndPosition<
                      org.apache.flink.table.data.RowData>
                  row;
              while ((row = records.nextRecordFromSplit()) != null)
                expected.add(row.record().getInt(0));
            }
          } finally {
            records.recycle();
          }
        }
      }
      var actual = new ArrayList<Integer>();
      try (var reader = new IcebergNativeTaskReader(task, schema, List.of(1), table.io(), 31, 0)) {
        org.apache.arrow.vector.VectorSchemaRoot next;
        while ((next = reader.next()) != null) {
          try (var root = next) {
            for (int row = 0; row < root.getRowCount(); row++)
              actual.add((Integer) root.getVector(0).getObject(row));
          }
        }
      }
      assertEquals(expected, actual, "range=" + start + "+" + length);
      all.addAll(actual);
    }
    assertEquals(
        java.util.stream.IntStream.range(0, 1001)
            .filter(id -> !deletes || (id != 177 && id != 501 && id != 999))
            .boxed()
            .toList(),
        all);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static FileScanTask range(FileScanTask task, long start, long length) {
    return (FileScanTask)
        Proxy.newProxyInstance(
            FileScanTask.class.getClassLoader(),
            new Class<?>[] {FileScanTask.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "start" -> start;
                  case "length" -> length;
                  default -> method.invoke(task, args);
                });
  }
}
