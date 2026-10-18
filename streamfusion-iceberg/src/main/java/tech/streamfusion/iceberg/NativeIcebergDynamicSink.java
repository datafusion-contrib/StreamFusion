package tech.streamfusion.iceberg;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import tech.streamfusion.operator.ArrowBatch;

/** Experimental dynamic streaming writes with a fixed, Arrow-preserving payload layout. */
public final class NativeIcebergDynamicSink {
  private NativeIcebergDynamicSink() {}

  @FunctionalInterface
  public interface Router extends Serializable {
    Destination route(RowData borrowedRow) throws Exception;
  }

  /** Routing metadata only: the router cannot replace or materialize the payload. */
  public record Destination(
      String table,
      String branch,
      PartitionSpec spec,
      DistributionMode distribution,
      int writeParallelism,
      boolean upsert,
      Set<String> equalityFields)
      implements Serializable {}

  static DynamicRecordGenerator<RowData> generator(
      Schema schema, Router router, boolean immediate) {
    return new Generator(schema, router, immediate);
  }

  private static final class Generator implements DynamicRecordGenerator<RowData> {
    private final Schema schema;
    private final Router router;
    private final boolean immediate;
    private transient Map<String, TableIdentifier> identifiers;

    private Generator(Schema schema, Router router, boolean immediate) {
      this.schema = schema;
      this.router = router;
      this.immediate = immediate;
    }

    @Override
    public void generate(RowData row, org.apache.flink.util.Collector<DynamicRecord> out)
        throws Exception {
      var kind = row.getRowKind();
      var destination = router.route(row);
      if (!immediate
          && (destination.upsert()
              || (destination.equalityFields() != null && !destination.equalityFields().isEmpty())))
        throw new IllegalArgumentException(
            "Keyed dynamic Arrow writes require immediate table updates");
      if (row.getRowKind() != kind)
        throw new IllegalArgumentException("Dynamic router changed the borrowed row kind");
      if (identifiers == null) identifiers = new java.util.HashMap<>();
      var identifier = identifiers.get(destination.table());
      if (identifier == null) {
        identifier = TableIdentifier.parse(destination.table());
        if (identifiers.size() == 1024) identifiers.clear();
        identifiers.put(destination.table(), identifier);
      }
      var record =
          new DynamicRecord(
              identifier,
              destination.branch(),
              schema,
              row,
              destination.spec(),
              destination.distribution(),
              destination.writeParallelism());
      record.setUpsertMode(destination.upsert());
      record.setEqualityFields(destination.equalityFields());
      out.collect(record);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static DataStreamSink<?> append(
      DataStream<ArrowBatch> input,
      Schema inputSchema,
      Router router,
      CatalogLoader loader,
      ReadableConfig config,
      Map<String, String> options,
      String uidPrefix) {
    if (!IcebergSinkAdapter.compatible())
      throw new IllegalArgumentException("Dynamic adapter requires released Iceberg 1.12.0");
    var rowType = FlinkSchemaUtil.convert(inputSchema);
    NativeIcebergSink.validateSchema(inputSchema);
    if (new org.apache.iceberg.flink.FlinkWriteConf(options, config).overwriteMode())
      throw new IllegalArgumentException("Dynamic streaming overwrite is not admitted");
    var immediateOption =
        org.apache.iceberg.flink.sink.dynamic.FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE;
    boolean immediate =
        Boolean.parseBoolean(
            options.getOrDefault(
                immediateOption.key(), Boolean.toString(config.get(immediateOption))));
    if (!immediate && !inputSchema.identifierFieldIds().isEmpty())
      throw new IllegalArgumentException(
          "Keyed dynamic Arrow writes require immediate table updates");
    var generator = generator(inputSchema, router, immediate);
    DataStream<RowData> host =
        new DataStream<>(
            input.getExecutionEnvironment(), (Transformation) input.getTransformation());
    var builder =
        DynamicIcebergSink.forInput(host)
            .generator(generator)
            .catalogLoader(loader)
            .flinkConf(config)
            .setAll(options);
    if (uidPrefix != null) builder.uidPrefix(uidPrefix);
    var result = builder.append();
    var sinkTransformation = (SinkTransformation) result.getTransformation();
    var original = sinkTransformation.getSink();
    try {
      DataStream<?> forward =
          (DataStream<?>)
              IcebergSinkAdapter.field(original.getClass(), "forwardWriteResults").get(original);
      var pending = new ArrayDeque<Transformation<?>>();
      pending.add((Transformation<?>) sinkTransformation.getInputs().get(0));
      pending.add(forward.getTransformation());
      var seen = new IdentityHashMap<Transformation<?>, Boolean>();
      int processors = 0, updaters = 0, writers = 0;
      while (!pending.isEmpty()) {
        var transformation = pending.removeFirst();
        if (transformation == input.getTransformation() || seen.put(transformation, true) != null)
          continue;
        pending.addAll(transformation.getInputs());
        if (transformation instanceof OneInputTransformation one) {
          var factory = one.getOperatorFactory();
          if (factory instanceof SinkWriterOperatorFactory writer) {
            IcebergSinkAdapter.field(SinkWriterOperatorFactory.class, "sink")
                .set(writer, new IcebergDynamicSinkBridge(writer.getSink()));
            writers++;
            continue;
          }
          if (!(factory instanceof SimpleOperatorFactory<?> simple)
              || !(simple.getOperator() instanceof AbstractUdfStreamOperator<?, ?> udf))
            throw new IllegalArgumentException("Unverified released dynamic operator");
          String function = udf.getUserFunction().getClass().getName();
          boolean processor =
              function.equals("org.apache.iceberg.flink.sink.dynamic.DynamicRecordProcessor");
          if (!processor
              && !function.equals(
                  "org.apache.iceberg.flink.sink.dynamic.DynamicTableUpdateOperator"))
            throw new IllegalArgumentException("Unverified dynamic function: " + function);
          IcebergSinkAdapter.field(OneInputTransformation.class, "operatorFactory")
              .set(one, new IcebergDynamicOperatorFactory(factory, processor ? rowType : null));
          if (processor) processors++;
          else {
            updaters++;
            one.setStateKeySelector(
                (org.apache.flink.api.java.functions.KeySelector<IcebergDynamicBatch, String>)
                    batch -> batch.target.table());
          }
        } else if (transformation instanceof SideOutputTransformation<?> side) {
          IcebergSinkAdapter.field(SideOutputTransformation.class, "tag")
              .set(side, new OutputTag<>(side.getOutputTag().getId(), IcebergDynamicBatch.TYPE));
        } else if (transformation instanceof PartitionTransformation<?> partition
            && partition.getPartitioner() instanceof KeyGroupStreamPartitioner) {
          IcebergSinkAdapter.field(PartitionTransformation.class, "partitioner")
              .set(
                  partition,
                  new KeyGroupStreamPartitioner<IcebergDynamicBatch, String>(
                      batch -> batch.target.table(),
                      ((KeyGroupStreamPartitioner<?, ?>) partition.getPartitioner())
                          .getMaxParallelism()));
        }
        IcebergSinkAdapter.field(Transformation.class, "outputType")
            .set(transformation, IcebergDynamicBatch.TYPE);
      }
      if (processors != 1 || updaters != 1 || writers != 1)
        throw new IllegalStateException(
            "Unverified dynamic topology: " + processors + "/" + updaters + "/" + writers);
      IcebergSinkAdapter.field(SinkTransformation.class, "sink")
          .set(sinkTransformation, new IcebergDynamicSinkBridge(original));
      return result;
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException("Cannot adapt released dynamic topology", failure);
    }
  }
}
