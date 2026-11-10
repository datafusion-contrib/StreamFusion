package tech.streamfusion.hudi;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.adapter.DataStreamSinkProviderAdapter;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.append.AppendWriteFunctions;
import org.apache.hudi.sink.append.AppendWriteOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;
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
        || !conf.get(FlinkOptions.PARTITION_PATH_FIELD).isEmpty()
        || OptionsResolver.getInsertPartitioner(conf).isPresent()
        || !"NONE".equalsIgnoreCase(AppendWriteFunctions.resolveBufferType(conf)))
      throw new IllegalArgumentException(
          "Only unpartitioned, unbuffered COW append without metadata fields is admitted");
    HudiWriteAdmission.validateFormat(table, type);
    var hadoop = org.apache.hudi.configuration.HadoopConfigurations.getHadoopConf(conf);
    if (org.apache.hudi.util.StreamerUtil.tableExists(conf.get(FlinkOptions.PATH), hadoop)) {
      var actual = org.apache.hudi.util.StreamerUtil.createMetaClient(conf);
      if (actual.getTableConfig().populateMetaFields()
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataStreamSink<?> appendValidated(
      DataStream<ArrowBatch> input,
      HoodieTableSink table,
      RowType declared,
      DynamicTableSink.Context context) {
    input = HudiWriteAdmission.constrainInput(input, declared);
    var copied = (HoodieTableSink) table.copy();
    var provider = (DataStreamSinkProviderAdapter) copied.getSinkRuntimeProvider(context);
    DataStream<RowData> host =
        new DataStream<>(
            input.getExecutionEnvironment(), (Transformation) input.getTransformation());
    var sink = provider.consumeDataStream(host);
    var pending = new ArrayDeque<Transformation<?>>();
    var seen = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
    pending.add(sink.getTransformation());
    int replaced = 0;
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (!seen.add(next)) continue;
      pending.addAll(next.getInputs());
      if (!(next instanceof OneInputTransformation)) continue;
      var factory = ((OneInputTransformation<?, ?>) next).getOperatorFactory();
      if (!(factory instanceof SimpleOperatorFactory)
          || ((SimpleOperatorFactory<?>) factory).getOperator().getClass()
              != AppendWriteOperator.class) continue;
      try {
        var field = OneInputTransformation.class.getDeclaredField("operatorFactory");
        field.setAccessible(true);
        field.set(
            next,
            WriteOperatorFactory.instance(
                copied.getConf(), new HudiAppendOperator(copied.getConf(), declared)));
      } catch (ReflectiveOperationException failure) {
        throw new IllegalArgumentException("Unsupported released Hudi append topology", failure);
      }
      replaced++;
    }
    if (replaced != 1)
      throw new IllegalArgumentException("Expected one Hudi append writer, found " + replaced);
    return sink;
  }
}
