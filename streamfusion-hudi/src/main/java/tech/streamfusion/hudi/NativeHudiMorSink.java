package tech.streamfusion.hudi;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieTableSink;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;

/** Admission for Arrow buffering around Hudi's original bucket-index writers. */
public final class NativeHudiMorSink {
  private NativeHudiMorSink() {}

  public static void validate(HoodieTableSink table, RowType type) {
    var conf = table.getConf();
    if (!"MERGE_ON_READ".equals(conf.get(FlinkOptions.TABLE_TYPE))
        || !"upsert".equals(conf.get(FlinkOptions.OPERATION))
        || !"BUCKET".equals(conf.get(FlinkOptions.INDEX_TYPE))
        || !"SIMPLE".equals(conf.get(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE))
        || conf.getOptional(FlinkOptions.WRITE_TASKS).orElse(-1) <= 0
        || conf.get(FlinkOptions.METADATA_ENABLED)
        || conf.get(FlinkOptions.CDC_ENABLED)
        || conf.get(FlinkOptions.CHANGELOG_ENABLED)
        || conf.get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED)
        || conf.get(FlinkOptions.COMPACTION_ASYNC_ENABLED)
        || conf.get(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED)
        || conf.get(FlinkOptions.CLUSTERING_ASYNC_ENABLED)
        || OptionsResolver.isManagedMemoryBufferEnabled(conf)
        || conf.get(FlinkOptions.WRITE_RATE_LIMIT) != 0
        || !"parquet".equalsIgnoreCase(conf.toMap().get("hoodie.logfile.data.block.format"))
        || !Double.isFinite(conf.get(FlinkOptions.WRITE_BATCH_SIZE))
        || conf.get(FlinkOptions.WRITE_BATCH_SIZE) <= 0
        || OptionsResolver.getWriteBufferSizeInBytes(conf) <= 0)
      throw new IllegalArgumentException(
          "MOR Arrow writes require explicit SIMPLE bucket writers, Parquet logs, "
              + "no metadata table, changelog storage or rewrite services");
    var effective = HudiWriteAdmission.validateFormat(table, type);
    for (var field : type.getFields()) HudiTypes.validate(field.getType(), false);
    if (effective.getWriteVersion().versionCode() != 9
        || !effective.populateMetaFields()
        || effective.allowOperationMetadataField()
        || effective.allowDuplicateInserts()
        || effective.shouldWritePartialUpdates()
        || !"SINGLE_WRITER".equals(effective.getWriteConcurrencyMode().name())
        || effective.getBucketIndexNumBuckets() <= 0
        || !Set.of("EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING")
            .contains(effective.getRecordMergeMode().name())
        || !Set.of(
                "org.apache.hudi.client.model.EventTimeFlinkRecordMerger",
                "org.apache.hudi.client.model.CommitTimeFlinkRecordMerger")
            .contains(effective.getRecordMerger().getClass().getName())
        || !standardPayload(conf.get(FlinkOptions.PAYLOAD_CLASS_NAME)))
      throw new IllegalArgumentException(
          "Only standard full-row version-9 MOR writes are admitted");
    var declared = HudiWriteAdmission.declaredType(table);
    if ("EVENT_TIME_ORDERING".equals(effective.getRecordMergeMode().name())) {
      var fields = OptionsResolver.getOrderingFieldsStr(conf);
      var ordering =
          fields == null
              ? List.<String>of()
              : Arrays.stream(fields.split(",")).map(String::trim).collect(Collectors.toList());
      if (ordering.size() != 1)
        throw new IllegalArgumentException("MOR event ordering requires one verified field");
      int index = declared.getFieldNames().indexOf(ordering.get(0));
      if (index < 0
          || declared.getTypeAt(index).isNullable()
          || !Set.of("BIGINT", "INTEGER").contains(declared.getTypeAt(index).getTypeRoot().name()))
        throw new IllegalArgumentException(
            "MOR event ordering requires a NOT NULL BIGINT or INT field");
    }
    if (StreamerUtil.tableExists(
        conf.get(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(conf))) {
      try {
        var meta = StreamerUtil.createMetaClient(conf);
        var actual = meta.getTableConfig();
        var keys =
            Arrays.stream(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        if (!"MERGE_ON_READ".equals(meta.getTableType().name())
            || actual.getTableVersion().versionCode() != 9
            || !actual.populateMetaFields()
            || actual.isCDCEnabled()
            || actual.getBootstrapBasePath().isPresent()
            || actual.getPartialUpdateMode().isPresent()
            || actual.shouldDropPartitionColumns()
            || !"PARQUET".equals(actual.getBaseFileFormat().name())
            || actual.getRecordMergeMode() != effective.getRecordMergeMode()
            || !standardPayload(actual.getLegacyPayloadClass())
            || !keys.equals(Arrays.asList(actual.getRecordKeyFields().orElse(new String[0]))))
          throw new IllegalArgumentException("Existing target does not match MOR Arrow admission");
        var partitions = conf.get(FlinkOptions.PARTITION_PATH_FIELD);
        var expectedPartitions =
            partitions.isEmpty()
                ? List.<String>of()
                : Arrays.stream(partitions.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        if (!expectedPartitions.equals(
            Arrays.asList(actual.getPartitionFields().orElse(new String[0]))))
          throw new IllegalArgumentException("MOR target partition fields do not match");
        var orderingFields = OptionsResolver.getOrderingFieldsStr(conf);
        var expectedOrdering =
            orderingFields == null || orderingFields.isEmpty()
                ? List.<String>of()
                : Arrays.stream(orderingFields.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        if (!expectedOrdering.equals(actual.getOrderingFields()))
          throw new IllegalArgumentException("MOR target ordering fields do not match");
        var stored =
            HoodieSchemaConverter.convertToRowType(
                new TableSchemaResolver(meta).getTableSchema(false));
        if (!stored.copy(true).equals(HudiWriteAdmission.canonicalType(declared).copy(true)))
          throw new IllegalArgumentException("MOR target schema evolution is not admitted");
      } catch (Exception failure) {
        throw new IllegalArgumentException("Cannot verify the existing MOR target", failure);
      }
    }
    Native.liveNativeHandles();
    HudiRowBufferControl.validate();
  }

  private static boolean standardPayload(String payload) {
    return List.of(
            "org.apache.hudi.common.model.EventTimeAvroPayload",
            "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload")
        .contains(String.valueOf(payload));
  }

  public static Plan plan(HoodieTableSink table, RowType type) {
    validate(table, type);
    return new Plan((HoodieTableSink) table.copy(), HudiWriteAdmission.declaredType(table));
  }

  public static DataStreamSink<?> append(
      DataStream<ArrowBatch> input,
      HoodieTableSink table,
      RowType type,
      DynamicTableSink.Context context) {
    return plan(table, type).append(input, context);
  }

  public static final class Plan implements HudiSinkPlan {
    private final HoodieTableSink table;
    private final RowType declared;

    private Plan(HoodieTableSink table, RowType declared) {
      this.table = table;
      this.declared = declared;
    }

    @Override
    public DataStreamSink<?> append(
        DataStream<ArrowBatch> input, DynamicTableSink.Context context) {
      return HudiSinkGraph.appendValidated(
          HudiWriteAdmission.constrainInput(input, declared), table, declared, context, true);
    }

    @Override
    public String writeMode() {
      return "MOR upsert";
    }
  }
}
