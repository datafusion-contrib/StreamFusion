package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NativeIcebergTaskFallbackTest {
  @TempDir Path temp;

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "AVRO", "ORC"})
  void mixedTasksAndRestoredOffsetsUseTheDeclaredFormatReader(FileFormat format) throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, true);
    var factory = new GenericFileWriterFactory.Builder(table).dataFileFormat(format).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(
                    table
                        .io()
                        .newOutputFile(
                            table.location()
                                + "/later."
                                + format.name().toLowerCase(java.util.Locale.ROOT))),
            table.spec(),
            null);
    try (writer) {
      var row = GenericRecord.create(table.schema());
      for (int id = 101; id < 105; id++) {
        row.setField("id", id);
        row.setField("payload", "later-" + id);
        writer.write(row);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    var tasks = new ArrayList<org.apache.iceberg.FileScanTask>();
    try (var planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    }
    tasks.sort(java.util.Comparator.comparing(task -> task.file().location()));
    var combined = new BaseCombinedScanTask(tasks);
    var split = IcebergSourceSplit.fromCombinedScanTask(combined);
    var function = new IcebergArrowReaderFunction(table.schema(), List.of(1, 2), table.io(), 7);
    var actual = new ArrayList<Integer>();
    try (var input = function.apply(split)) {
      var records = input.next();
      assertEquals(split.splitId(), records.nextSplit());
      var positioned = records.nextRecordFromSplit();
      try (var root = positioned.record().root()) {
        for (int row = 0; row < root.getRowCount(); row++)
          actual.add((Integer) root.getVector("id").getObject(row));
      }
      split.updatePosition(positioned.fileOffset(), positioned.recordOffset());
      records.recycle();
    }
    function = new IcebergArrowReaderFunction(table.schema(), List.of(1, 2), table.io(), 3);
    try (var input = function.apply(split)) {
      while (input.hasNext()) {
        var records = input.next();
        assertEquals(split.splitId(), records.nextSplit());
        var positioned = records.nextRecordFromSplit();
        try (var root = positioned.record().root()) {
          for (int row = 0; row < root.getRowCount(); row++)
            actual.add((Integer) root.getVector("id").getObject(row));
        }
        records.recycle();
      }
    }
    assertEquals(
        java.util.stream.IntStream.range(0, 105)
            .filter(id -> id != 17 && id != 23)
            .boxed()
            .toList(),
        actual);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
