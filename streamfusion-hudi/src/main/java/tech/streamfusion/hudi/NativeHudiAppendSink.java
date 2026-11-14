package tech.streamfusion.hudi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.append.AppendWriteFunctions;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.operator.ArrowBatch;

/** Reuses the released append graph and coordinator, changing only its payload writer. */
public final class NativeHudiAppendSink {
  private NativeHudiAppendSink() {}

  public static void validate(HoodieTableSink table, RowType type) {
    var conf = table.getConf();
    if (!OptionsResolver.isAppendMode(conf)
        || !"COPY_ON_WRITE".equals(conf.get(FlinkOptions.TABLE_TYPE))
        || conf.get(FlinkOptions.METADATA_ENABLED)
        || conf.get(FlinkOptions.INDEX_TYPE).contains("BLOOM")
        || conf.toMap().getOrDefault("hoodie.index.type", "").contains("BLOOM")
        || Boolean.parseBoolean(conf.toMap().getOrDefault("hoodie.populate.meta.fields", "true"))
        || OptionsResolver.getInsertPartitioner(conf).isPresent()
        || !"NONE".equalsIgnoreCase(AppendWriteFunctions.resolveBufferType(conf)))
      throw new IllegalArgumentException(
          "Only unbuffered COW append without metadata fields is admitted");
    HudiWriteAdmission.validateFormat(table, type);
    var hadoop = org.apache.hudi.configuration.HadoopConfigurations.getHadoopConf(conf);
    if (org.apache.hudi.util.StreamerUtil.tableExists(conf.get(FlinkOptions.PATH), hadoop)) {
      var actual = org.apache.hudi.util.StreamerUtil.createMetaClient(conf);
      if (actual.getTableConfig().populateMetaFields()
          || actual.getTableConfig().shouldDropPartitionColumns()
          || actual.getTableType() != org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
          || actual.getTableConfig().getBaseFileFormat()
              != org.apache.hudi.common.model.HoodieFileFormat.PARQUET)
        throw new IllegalArgumentException(
            "Existing Hudi target does not match native append admission");
    }
  }

  public static Plan plan(HoodieTableSink table, RowType type) {
    validate(table, type);
    return new Plan((HoodieTableSink) table.copy(), HudiWriteAdmission.declaredType(table));
  }

  public static final class Plan implements HudiSinkPlan {
    private final HoodieTableSink table;
    private final RowType declared;

    private Plan(HoodieTableSink table, RowType declared) {
      this.table = table;
      this.declared = declared;
    }

    public DataStreamSink<?> append(
        DataStream<ArrowBatch> input, DynamicTableSink.Context context) {
      return appendValidated(input, table, declared, context);
    }
  }

  public static DataStreamSink<?> append(
      DataStream<ArrowBatch> input,
      HoodieTableSink table,
      RowType type,
      DynamicTableSink.Context context) {
    return plan(table, type).append(input, context);
  }

  private static DataStreamSink<?> appendValidated(
      DataStream<ArrowBatch> input,
      HoodieTableSink table,
      RowType declared,
      DynamicTableSink.Context context) {
    return HudiSinkGraph.appendValidated(
        HudiWriteAdmission.constrainInput(input, declared), table, declared, context, false);
  }
}
