package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class NativeIcebergTaskMetadataTest {
  @TempDir Path temp;

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"group-é🐍"})
  void identityPartitionConstantsReconstructColumnsOmittedFromParquet(String value)
      throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    var spec = PartitionSpec.builderFor(schema).identity("payload").build();
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                spec,
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("partitioned").toUri().toString());
    var physical = table.schema().select("id");
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(physical)
            .dataFileFormat(FileFormat.PARQUET)
            .build();
    var partition = new PartitionData(table.spec().partitionType());
    partition.set(0, value);
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/data.parquet")),
            table.spec(),
            partition);
    try (writer) {
      var record = GenericRecord.create(physical);
      for (int id = 0; id < 3; id++) {
        record.setField("id", id);
        writer.write(record);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(2, 1), table.io(), 2, 0)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          int count = 0;
          while ((next = reader.next()) != null) {
            try (var root = next) {
              for (int row = 0; row < root.getRowCount(); row++) {
                assertEquals(count++, root.getVector("id").getObject(row));
                Object actual = root.getVector("payload").getObject(row);
                assertEquals(value, actual == null ? null : actual.toString());
              }
            }
          }
          assertEquals(3, count);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void nameMappingReadsRenamedLegacyColumnsWithNonOrdinalFieldIds() throws Exception {
    var schema =
        new Schema(
            Types.NestedField.optional(1, "unused", Types.IntegerType.get()),
            Types.NestedField.required(2, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("legacy").toUri().toString());
    table.updateSchema().deleteColumn("unused").commit();
    assertEquals(2, table.schema().findField("id").fieldId());
    String mapping =
        "[{\"field-id\":2,\"names\":[\"id\",\"old_id\"]},"
            + "{\"field-id\":3,\"names\":[\"payload\",\"old_payload\"]}]";
    String path = table.location() + "/legacy.parquet";
    var type =
        org.apache.iceberg.shaded.org.apache.parquet.schema.MessageTypeParser.parseMessageType(
            "message legacy { required int32 old_id; optional binary old_payload (UTF8); }");
    var groups =
        new org.apache.iceberg.shaded.org.apache.parquet.example.data.simple.SimpleGroupFactory(
            type);
    try (var writer =
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                new org.apache.hadoop.fs.Path(path))
            .withConf(new org.apache.hadoop.conf.Configuration())
            .withType(type)
            .build()) {
      writer.write(groups.newGroup().append("old_id", 7).append("old_payload", "mapped-é🐍"));
      writer.write(groups.newGroup().append("old_id", 17));
    }
    var file =
        org.apache.iceberg.DataFiles.builder(table.spec())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(2)
            .withFileSizeInBytes(table.io().newInputFile(path).getLength())
            .build();
    table.newAppend().appendFile(file).commit();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var reader =
            new IcebergNativeTaskReader(
                task, table.schema(), List.of(2, 3), table.io(), 7, 0, List.of(), true, mapping)) {
          try (var root = reader.next()) {
            assertEquals(2, root.getRowCount());
            assertEquals(7, root.getVector("id").getObject(0));
            assertEquals(17, root.getVector("id").getObject(1));
            assertEquals("mapped-é🐍", root.getVector("payload").getObject(0).toString());
            assertNull(root.getVector("payload").getObject(1));
          }
          assertNull(reader.next());
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
