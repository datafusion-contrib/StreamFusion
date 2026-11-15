package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.io.log.block.HoodieFlinkParquetDataBlock;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.NativeParquet;

class HudiMorIOFactoryTest {
  @TempDir Path temporary;
  private static final RowType TYPE =
      (RowType)
          RowType.of(
                  new LogicalType[] {
                    new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new BooleanType()
                  },
                  new String[] {"id", "name", "flag"})
              .copy(false);

  @BeforeAll
  static void memory() {
    var conf = new Configuration();
    conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    TaskOffHeapMemory.initialize(conf);
  }

  @AfterEach
  void closesNativeWritersAndMetadataBuffers() {
    assertTrue(NativeParquet.liveNativeHandles().isBlank(), NativeParquet.liveNativeHandles());
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"snappy", "gzip", "zstd", "uncompressed"})
  void originalHudiParquetLogBlockEncodesBorrowedPayloadsAndRealMetadata(String codec)
      throws Exception {
    var schema = HoodieSchemaConverter.convertToSchema(TYPE);
    var writerSchema = HoodieSchemaUtils.addMetadataFields(schema);
    List<HoodieRecord> records = new ArrayList<>();
    try (var first = batch(10);
        var second = batch(20)) {
      var roots = List.of(first, second);
      for (int b = 0; b < roots.size(); b++) {
        var batch = new HudiAppendIOFactory.Batch(roots.get(b), TYPE);
        for (int index : b == 0 ? new int[] {2, 0} : new int[] {1, 2}) {
          var view = new HudiAppendIOFactory.Cursor(batch, batch.vectors);
          view.setRowId(index);
          String key = Long.toString(((BigIntVector) roots.get(b).getVector("id")).get(index));
          var original =
              new HoodieFlinkRecord(new HoodieKey(key, ""), HoodieOperation.INSERT, view);
          records.add(
              original.prependMetaFields(
                  schema,
                  writerSchema,
                  new MetadataValues()
                      .setCommitTime("20261110000000000")
                      .setCommitSeqno("20261110000000000_0_" + records.size())
                      .setRecordKey(key)
                      .setPartitionPath("")
                      .setFileName("file-group"),
                  new Properties()));
        }
      }
      var hadoop = new org.apache.hadoop.conf.Configuration();
      hadoop.set(
          HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HudiMorIOFactory.class.getName());
      try (var storage = new HoodieHadoopStorage(temporary.toUri().toString(), hadoop)) {
        var block =
            new HoodieFlinkParquetDataBlock(
                records,
                Map.of(HeaderMetadataType.SCHEMA, writerSchema.toString()),
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                codec,
                0.1,
                true);
        var encoded = block.getContentBytes(storage);
        Path file = temporary.resolve("log-block.parquet");
        Files.write(file, encoded.toByteArray());
        var rows = new ParquetUtils().readAvroRecords(storage, new StoragePath(file.toUri()));
        assertEquals(
            List.of(12L, 10L, 21L, 22L),
            rows.stream().map(row -> row.get("id")).collect(java.util.stream.Collectors.toList()));
        assertEquals("payload-12-\u00e9", rows.get(0).get("name").toString());
        assertNull(rows.get(2).get("name"));
        assertEquals(false, rows.get(0).get("flag"));
        assertNull(rows.get(1).get("flag"));
        assertEquals(true, rows.get(2).get("flag"));
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(
              rows.get(i).get("id").toString(), rows.get(i).get("_hoodie_record_key").toString());
          assertEquals("20261110000000000", rows.get(i).get("_hoodie_commit_time").toString());
          assertEquals(
              "20261110000000000_0_" + i, rows.get(i).get("_hoodie_commit_seqno").toString());
        }
        var stats =
            block.getColumnRangeMeta("log-block", org.apache.hudi.metadata.HoodieIndexVersion.V1);
        assertEquals(10L, stats.get("id").getMinValue());
        assertEquals(22L, stats.get("id").getMaxValue());
      }
      assertEquals(10L, ((BigIntVector) first.getVector("id")).get(0));
      assertEquals(20L, ((BigIntVector) second.getVector("id")).get(0));
    }
  }

  @Test
  void rejectsPayloadsThatHaveAlreadyBeenTransposed() throws Exception {
    var hadoop = new org.apache.hadoop.conf.Configuration();
    hadoop.set(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HudiMorIOFactory.class.getName());
    var schema = HoodieSchemaUtils.addMetadataFields(HoodieSchemaConverter.convertToSchema(TYPE));
    try (var storage = new HoodieHadoopStorage(temporary.toUri().toString(), hadoop)) {
      var block =
          new HoodieFlinkParquetDataBlock(
              List.of(
                  new HoodieFlinkRecord(
                      new HoodieKey("key", ""), HoodieOperation.INSERT, new GenericRowData(8))),
              Map.of(HeaderMetadataType.SCHEMA, schema.toString()),
              HoodieRecord.RECORD_KEY_METADATA_FIELD,
              "snappy",
              0.1,
              true);
      assertThrows(java.io.IOException.class, () -> block.getContentBytes(storage));
    }
  }

  @Test
  void closesTheNativeEncoderWhenTheHostIteratorFailsBeforeItsFirstRecord() throws Exception {
    var hadoop = new org.apache.hadoop.conf.Configuration();
    hadoop.set(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HudiMorIOFactory.class.getName());
    var schema = HoodieSchemaUtils.addMetadataFields(HoodieSchemaConverter.convertToSchema(TYPE));
    var failed =
        new java.util.Iterator<HoodieRecord>() {
          @Override
          public boolean hasNext() {
            throw new IllegalStateException("injected iterator failure");
          }

          @Override
          public HoodieRecord next() {
            throw new AssertionError("unreachable");
          }
        };
    try (var storage = new HoodieHadoopStorage(temporary.toUri().toString(), hadoop)) {
      var format =
          org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory(storage)
              .getFileFormatUtils(org.apache.hudi.common.model.HoodieFileFormat.PARQUET);
      var failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  format.serializeRecordsToLogBlock(
                      storage,
                      failed,
                      HoodieRecord.HoodieRecordType.FLINK,
                      schema,
                      schema,
                      HoodieRecord.RECORD_KEY_METADATA_FIELD,
                      Map.of(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy")));
      assertEquals("injected iterator failure", failure.getMessage());
    }
  }

  private static VectorSchemaRoot batch(long start) {
    var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(TYPE), NativeAllocator.SHARED);
    root.allocateNew();
    var ids = (BigIntVector) root.getVector("id");
    var names = (VarCharVector) root.getVector("name");
    var flags = (BitVector) root.getVector("flag");
    for (int i = 0; i < 3; i++) {
      ids.setSafe(i, start + i);
      if (i == 1) names.setNull(i);
      else
        names.setSafe(
            i,
            ("payload-" + (start + i) + "-\u00e9")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      if (i == 0) flags.setNull(i);
      else flags.setSafe(i, i == 1 ? 1 : 0);
    }
    root.setRowCount(3);
    return root;
  }
}
