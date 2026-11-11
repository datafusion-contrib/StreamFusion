package tech.streamfusion.hudi;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.util.HoodieSchemaConverter;

/** Verified physical schemas and merge policies for the Java Hudi Arrow reader. */
final class HudiMorReadPlan {
  private HudiMorReadPlan() {}

  static String validate(
      HoodieTableSource source, org.apache.flink.table.types.logical.RowType output) {
    var conf = source.getConf();
    var table = source.getMetaClient().getTableConfig();
    if (table.getTableVersion().versionCode() != 9
        || !table.populateMetaFields()
        || table.getPartialUpdateMode().isPresent()
        || table.getOrderingFields().size() > 1
        || !java.util.Set.of("EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING")
            .contains(table.getRecordMergeMode().name())
        || !"payload_combine".equals(conf.get(FlinkOptions.MERGE_TYPE))
        || org.apache.hudi.util.StreamerUtil.getMaxCompactionMemoryInBytes(conf) <= 0)
      throw new IllegalArgumentException(
          "Only full-row version-9 MOR reads with metadata are admitted");
    if (!java.util.List.of(
            "org.apache.hudi.common.model.EventTimeAvroPayload",
            "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload")
        .contains(String.valueOf(table.getLegacyPayloadClass())))
      throw new IllegalArgumentException("Custom MOR payload behavior is not admitted");
    try {
      var schema = new TableSchemaResolver(source.getMetaClient()).getTableSchema();
      var physical =
          HoodieSchemaConverter.convertToRowType(HoodieSchemaUtils.removeMetadataFields(schema));
      validatePhysicalSchema(schema, physical);
      NativeHudiSource.validateProjection(physical, output);
      for (String ordering : table.getOrderingFields()) {
        int field = physical.getFieldNames().indexOf(ordering);
        if (field < 0
            || !java.util.Set.of(
                    org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT,
                    org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER)
                .contains(physical.getTypeAt(field).getTypeRoot()))
          throw new IllegalArgumentException("MOR ordering requires a verified integer field");
      }
      var storage = source.getMetaClient().getStorage();
      var parquet =
          org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory(storage)
              .getFileFormatUtils(org.apache.hudi.common.model.HoodieFileFormat.PARQUET);
      for (var split : source.buildHoodieSplits(source.getMetaClient(), conf)) {
        if (split.getBasePath().isPresent())
          validatePhysicalSchema(
              parquet.readSchema(
                  storage, new org.apache.hudi.storage.StoragePath(split.getBasePath().get())),
              physical);
        for (String path : split.getLogPaths().orElse(java.util.List.of())) {
          try (var reader =
              org.apache.hudi.common.table.log.HoodieLogFormat.newReader(
                  storage,
                  new org.apache.hudi.common.model.HoodieLogFile(
                      new org.apache.hudi.storage.StoragePath(path)),
                  schema)) {
            while (reader.hasNext()) {
              var block = reader.next();
              switch (block.getBlockType()) {
                case PARQUET_DATA_BLOCK:
                  if (((org.apache.hudi.common.table.log.block.HoodieDataBlock) block)
                      .containsPartialUpdates())
                    throw new IllegalArgumentException(
                        "Partial MOR log records require the stock reader");
                  var header =
                      block
                          .getLogBlockHeader()
                          .get(
                              org.apache.hudi.common.table.log.block.HoodieLogBlock
                                  .HeaderMetadataType.SCHEMA);
                  validatePhysicalSchema(HoodieSchema.parse(header), physical);
                  break;
                case DELETE_BLOCK:
                case COMMAND_BLOCK:
                  break;
                default:
                  throw new IllegalArgumentException(
                      "MOR log block is not admitted: " + block.getBlockType());
              }
            }
          }
        }
      }
      return schema.toString();
    } catch (Exception failure) {
      throw new IllegalArgumentException("MOR file/schema admission failed", failure);
    }
  }

  private static void validatePhysicalSchema(
      HoodieSchema schema, org.apache.flink.table.types.logical.RowType physical) {
    if (schema
        .getField(org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD)
        .isPresent())
      throw new IllegalArgumentException(
          "Stored MOR intermediate operations require the stock reader");
    NativeHudiSource.validateProjection(HoodieSchemaConverter.convertToRowType(schema), physical);
  }
}
