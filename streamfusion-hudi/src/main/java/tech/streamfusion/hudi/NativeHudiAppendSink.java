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
import tech.streamfusion.parquet.NativeParquet;

/** Reuses the released append graph and coordinator, changing only its payload writer. */
public final class NativeHudiAppendSink {
  private NativeHudiAppendSink() {}

  public static void validate(HoodieTableSink table, RowType type) {
    var conf = table.getConf();
    if (table.getClass() != HoodieTableSink.class
        || !"1.2.1".equals(HoodieTableSink.class.getPackage().getImplementationVersion())
        || !OptionsResolver.isAppendMode(conf)
        || !"COPY_ON_WRITE".equals(conf.get(FlinkOptions.TABLE_TYPE))
        || conf.get(FlinkOptions.METADATA_ENABLED)
        || conf.get(FlinkOptions.IGNORE_FAILED)
        || !"PARQUET"
            .equalsIgnoreCase(conf.toMap().getOrDefault("hoodie.table.base.file.format", "PARQUET"))
        || conf.get(FlinkOptions.INDEX_TYPE).contains("BLOOM")
        || conf.toMap().getOrDefault("hoodie.index.type", "").contains("BLOOM")
        || Boolean.parseBoolean(conf.toMap().getOrDefault("hoodie.populate.meta.fields", "true"))
        || !conf.get(FlinkOptions.PARTITION_PATH_FIELD).isEmpty()
        || OptionsResolver.getInsertPartitioner(conf).isPresent()
        || !"NONE".equalsIgnoreCase(AppendWriteFunctions.resolveBufferType(conf)))
      throw new IllegalArgumentException(
          "Only unpartitioned, unbuffered COW append without metadata fields is admitted");
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
    for (var field : type.getFields())
      switch (field.getType().getTypeRoot()) {
        case BIGINT:
        case INTEGER:
        case BOOLEAN:
        case VARCHAR:
          break;
        default:
          throw new IllegalArgumentException(
              "Hudi append type is not admitted: " + field.getType());
      }
    var expected = declaredType(table);
    if (expected.getFieldCount() != type.getFieldCount())
      throw new IllegalArgumentException("Partial Hudi write schemas are not admitted");
    for (int i = 0; i < type.getFieldCount(); i++)
      if (!expected.getTypeAt(i).copy(true).equals(type.getTypeAt(i).copy(true)))
        throw new IllegalArgumentException("Hudi sink schema conversion is not admitted");
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
    var planning = new org.apache.flink.configuration.Configuration(conf);
    org.apache.hudi.configuration.OptionsInference.setupSinkTasks(planning, 1);
    org.apache.hudi.configuration.OptionsInference.setupIndexConfigs(planning);
    var effective = org.apache.hudi.util.FlinkWriteClients.getHoodieClientConfig(planning);
    if (!java.util.Set.of("SNAPPY", "UNCOMPRESSED", "GZIP", "ZSTD")
            .contains(effective.getParquetCompressionCodec().toUpperCase(java.util.Locale.ROOT))
        || effective.getParquetBlockSize() <= 0
        || effective.getParquetPageSize() <= 0)
      throw new IllegalArgumentException("Hudi effective Parquet settings are not admitted");
    for (var entry : hadoop)
      if (entry.getKey().startsWith("parquet."))
        throw new IllegalArgumentException(
            "Hadoop Parquet overrides are not admitted: " + entry.getKey());
    if (!NativeParquet.isLoaded())
      throw new IllegalArgumentException("Native Parquet is unavailable");
  }

  private static RowType declaredType(HoodieTableSink table) {
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

  public static Plan plan(HoodieTableSink table, RowType type) {
    validate(table, type);
    return new Plan((HoodieTableSink) table.copy(), declaredType(table));
  }

  public static final class Plan {
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
