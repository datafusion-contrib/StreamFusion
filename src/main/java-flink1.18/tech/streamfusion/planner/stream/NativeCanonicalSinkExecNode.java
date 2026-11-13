package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.state.StockStateContract;

/** Retains the released sink's helpers and connector while replacing its materializer. */
final class NativeCanonicalSinkExecNode extends StreamExecSink implements CanonicalSinkIdentities {
  private final CanonicalSinkMaterializerRuntime runtime;

  NativeCanonicalSinkExecNode(
      StreamExecSink stock, ExecNodeContext context, ChangelogMode changelog,
      int[] upsertKeys, String strategy, List<StateMetadata> metadata,
      CanonicalSinkMaterializerRuntime runtime) {
    super(stock.getId(), context, stock.getPersistedConfig(), stock.getTableSinkSpec(), changelog,
        true, metadata, upsertKeys, stock.getInputProperties(),
        stock.getOutputType(), stock.getDescription());
    this.runtime = runtime;
    setCompiled(true);
  }

  @Override
  protected TransformationMetadata createTransformationMeta(String name, ExecNodeConfig config) {
    return config.shouldSetUid()
        ? new TransformationMetadata(createTransformationUid(name, config),
            createTransformationName(config), createTransformationDescription(config))
        : new TransformationMetadata(createTransformationName(config), createTransformationDescription(config));
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
  protected Transformation<RowData> applyUpsertMaterialize(
      Transformation<RowData> input, int[] keys, int parallelism, ExecNodeConfig config,
      ClassLoader loader, RowType type, int[] upsertKeys) {
    return runtime.apply(input, keys, parallelism, config, type, upsertKeys, sinkParallelismConfigured);
  }

  @Override
  public int originalMaterializerId() {
    return runtime.originalMaterializerId();
  }

  @Override
  public OneInputTransformation<ArrowBatch, ArrowBatch> materializerTransformation() {
    return runtime.materializerTransformation();
  }

  @Override
  public StockStateContract materializerContract() {
    return runtime.materializerContract();
  }

  @Override
  public List<Integer> helperIds() {
    return runtime.helperIds();
  }
}
