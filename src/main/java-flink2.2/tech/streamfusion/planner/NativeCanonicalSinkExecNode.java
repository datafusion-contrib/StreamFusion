package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions.SinkUpsertMaterializeStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.ArrowToRowDataOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.operator.SplitByKeyGroupOperator;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.StockSinkMaterializerStateContract;

/** Replaces only the sink's materializer; stock connector translation still owns the sink. */
final class NativeCanonicalSinkExecNode extends StreamExecSink {
  private final OneInputTransformation<?, ?> original;
  private final int maximum;
  private final long ttl;
  private final SinkUpsertMaterializeStrategy strategy;
  private final StreamExecutionEnvironment environment;
  private final Configuration pipeline;
  private final List<Integer> helpers = new ArrayList<>();
  private OneInputTransformation<ArrowBatch, ArrowBatch> materializer;

  NativeCanonicalSinkExecNode(
      StreamExecSink stock,
      ExecNodeContext context,
      ChangelogMode changelog,
      int[] upsertKeys,
      SinkUpsertMaterializeStrategy strategy,
      OneInputTransformation<?, ?> original,
      int maximum,
      long ttl,
      StreamExecutionEnvironment environment,
      Configuration pipeline) {
    super(
        stock.getId(),
        context,
        stock.getPersistedConfig(),
        stock.getTableSinkSpec(),
        changelog,
        true,
        strategy,
        null,
        upsertKeys,
        stock.getInputProperties(),
        stock.getOutputType(),
        stock.getDescription());
    this.original = original;
    this.maximum = maximum;
    this.ttl = ttl;
    this.strategy = strategy;
    this.environment = environment;
    this.pipeline = pipeline;
    // This runtime node keeps the original compiled context, including its plan/state version.
    setCompiled(true);
  }

  @Override
  protected TransformationMetadata createTransformationMeta(String name, ExecNodeConfig config) {
    // This runtime-only subclass has no serde registration. Keep the stock context's UIDs for
    // every unchanged sink helper instead of consulting metadata for the replacement class.
    return config.shouldSetUid()
        ? new TransformationMetadata(
            createTransformationUid(name, config),
            createTransformationName(config),
            createTransformationDescription(config))
        : new TransformationMetadata(
            createTransformationName(config), createTransformationDescription(config));
  }

  @Override
  protected TransformationMetadata createTransformationMeta(
      String name, String detail, String simplified, ExecNodeConfig config) {
    String title = createFormattedTransformationName(detail, simplified, config);
    String description = createFormattedTransformationDescription(detail, config);
    return config.shouldSetUid()
        ? new TransformationMetadata(createTransformationUid(name, config), title, description)
        : new TransformationMetadata(title, description);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> applyUpsertMaterialize(
      Transformation<RowData> input,
      int[] keys,
      int parallelism,
      ExecNodeConfig config,
      ClassLoader loader,
      RowType type,
      int[] upsertKeys) {
    int[] precisions = FlinkKeyGroupUtils.timestampPrecisions(type, keys);
    var entry =
        new OneInputTransformation<>(
            input,
            "RowDataToArrow",
            SimpleOperatorFactory.of(
                new RowDataToArrowOperator(
                    type, NativeConfig.transposeBatchRows(config, 1024), true, null)),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    entry.setMaxParallelism(maximum);
    var split =
        new OneInputTransformation<>(
            entry,
            "sink-materializer-key-split",
            SimpleOperatorFactory.of(new SplitByKeyGroupOperator(keys, precisions, maximum)),
            ArrowBatchTypeInformation.INSTANCE,
            entry.getParallelism(),
            false);
    split.setMaxParallelism(maximum);
    var partition = new PartitionTransformation<>(split, new ColumnarKeyGroupPartitioner(maximum));
    partition.setParallelism(parallelism);
    partition.setMaxParallelism(maximum);
    var operator =
        SinkMaterializerFactory.create(
            type, keys, precisions, upsertKeys, ttl, maximum, strategy, config);
    var keyType = (InternalTypeInfo<RowData>) original.getStateKeyType();
    var contract =
        new StockSinkMaterializerStateContract(
            keyType.createSerializer(environment.getConfig().getSerializerConfig()),
            keyType.toRowType(),
            type,
            upsertKeys,
            StockSinkMaterializerStateContract.Strategy.valueOf(strategy.name()),
            ttl);
    operator.setStockStateContract(contract);
    materializer =
        new OneInputTransformation<>(
            partition,
            "NativeSinkMaterializer",
            SimpleOperatorFactory.of(operator),
            ArrowBatchTypeInformation.INSTANCE,
            parallelism,
            sinkParallelismConfigured);
    materializer.setUid(original.getUid());
    FlinkKeyGroupUtils.applyColumnarKeying(materializer, maximum);
    String operatorId =
        new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(original.getUid())).toString();
    CanonicalStateBackend.install(
        environment, pipeline, operatorId, operator.getClass().getSimpleName(), contract, loader);
    var exit =
        new OneInputTransformation<>(
            materializer,
            "ArrowToRowData",
            SimpleOperatorFactory.of(new ArrowToRowDataOperator(type)),
            InternalTypeInfo.of(type),
            parallelism,
            sinkParallelismConfigured);
    exit.setMaxParallelism(maximum);
    helpers.add(entry.getId());
    helpers.add(split.getId());
    helpers.add(exit.getId());
    return exit;
  }

  int originalMaterializerId() {
    return original.getId();
  }

  int materializerId() {
    if (materializer == null)
      throw new IllegalStateException("Sink materializer was not translated");
    return materializer.getId();
  }

  List<Integer> helperIds() {
    return List.copyOf(helpers);
  }
}
