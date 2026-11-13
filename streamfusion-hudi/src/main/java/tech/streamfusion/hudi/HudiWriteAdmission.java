package tech.streamfusion.hudi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.parquet.NativeParquet;

/** Shared file/schema admission and constraints for columnar Hudi writers. */
final class HudiWriteAdmission {
  private HudiWriteAdmission() {}

  static org.apache.hudi.config.HoodieWriteConfig validateFormat(
      HoodieTableSink table, RowType type) {
    var conf = table.getConf();
    if (table.getClass() != HoodieTableSink.class
        || !HudiRelease.supported(HoodieTableSink.class)
        || conf.get(FlinkOptions.IGNORE_FAILED)
        || !"PARQUET"
            .equalsIgnoreCase(
                conf.toMap().getOrDefault("hoodie.table.base.file.format", "PARQUET")))
      throw new IllegalArgumentException("Unsupported Hudi version, file format or failure policy");
    if (type.getFieldCount() == 0)
      throw new IllegalArgumentException("Empty Hudi write schemas are not admitted");
    var parquetOptions =
        java.util.Set.of(
            "hoodie.parquet.max.file.size",
            "hoodie.parquet.block.size",
            "hoodie.parquet.page.size",
            "hoodie.parquet.compression.codec",
            "hoodie.parquet.dictionary.enabled");
    for (String key : conf.toMap().keySet())
      if (key.startsWith("parquet.")
          || key.startsWith("hadoop.parquet.")
          || key.startsWith("hoodie.parquet.") && !parquetOptions.contains(key))
        throw new IllegalArgumentException("Hudi Parquet writer option is not admitted: " + key);
    for (var field : type.getFields()) HudiTypes.validate(field.getType(), true);
    var expected = declaredType(table);
    if (expected.getFieldCount() != type.getFieldCount())
      throw new IllegalArgumentException("Partial Hudi write schemas are not admitted");
    for (int i = 0; i < type.getFieldCount(); i++)
      if (!expected.getTypeAt(i).copy(true).equals(type.getTypeAt(i).copy(true)))
        throw new IllegalArgumentException("Hudi sink schema conversion is not admitted");
    var hadoop = org.apache.hudi.configuration.HadoopConfigurations.getHadoopConf(conf);
    var planning = new org.apache.flink.configuration.Configuration(conf);
    org.apache.hudi.configuration.OptionsInference.setupSinkTasks(planning, 1);
    org.apache.hudi.configuration.OptionsInference.setupIndexConfigs(planning);
    var effective = org.apache.hudi.util.FlinkWriteClients.getHoodieClientConfig(planning);
    if (effective.isMetadataTableEnabled()
        || effective.shouldDropPartitionColumns()
        || !java.util.Set.of("SNAPPY", "UNCOMPRESSED", "GZIP", "ZSTD")
            .contains(effective.getParquetCompressionCodec().toUpperCase(java.util.Locale.ROOT))
        || effective.getParquetBlockSize() <= 0
        || effective.getParquetPageSize() <= 0)
      throw new IllegalArgumentException("Hudi effective write settings are not admitted");
    var configured =
        org.apache.hudi.util.HoodieSchemaConverter.convertToRowType(
            org.apache.hudi.common.schema.HoodieSchema.parse(effective.getSchema())
                .getNonNullType());
    if (!canonicalType(expected).copy(true).equals(configured.copy(true)))
      throw new IllegalArgumentException("Hudi write schema does not match the declared columns");
    for (var entry : hadoop)
      if (entry.getKey().startsWith("parquet."))
        throw new IllegalArgumentException(
            "Hadoop Parquet overrides are not admitted: " + entry.getKey());
    if (!NativeParquet.isLoaded())
      throw new IllegalArgumentException("Native Parquet is unavailable");
    return effective;
  }

  static RowType canonicalType(RowType type) {
    return org.apache.hudi.util.HoodieSchemaConverter.convertToRowType(
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(type).getNonNullType());
  }

  static RowType declaredType(HoodieTableSink table) {
    try {
      var overwrite = HoodieTableSink.class.getDeclaredField("overwrite");
      overwrite.setAccessible(true);
      if (overwrite.getBoolean(table))
        throw new IllegalArgumentException("Hudi overwrite is not admitted");
      var field = HoodieTableSink.class.getDeclaredField("schema");
      field.setAccessible(true);
      return (RowType)
          ((org.apache.flink.table.catalog.ResolvedSchema) field.get(table))
              .toSinkRowDataType()
              .getLogicalType();
    } catch (ReflectiveOperationException failure) {
      throw new IllegalArgumentException("Unsupported released Hudi sink schema", failure);
    }
  }

  static DataStream<ArrowBatch> constrainInput(DataStream<ArrowBatch> input, RowType declared) {
    var required = new String[declared.getFieldCount()];
    for (int i = 0; i < required.length; i++)
      if (!declared.getTypeAt(i).isNullable()) required[i] = declared.getFieldNames().get(i);
    if (java.util.Arrays.stream(required).anyMatch(java.util.Objects::nonNull)) {
      var checked =
          new OneInputTransformation<>(
              input.getTransformation(),
              "hudi-not-null-enforcer",
              SimpleOperatorFactory.of(
                  new tech.streamfusion.operator.NativeConstraintEnforcer(required)),
              tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
      input = new DataStream<>(input.getExecutionEnvironment(), checked);
    }
    return input;
  }
}
