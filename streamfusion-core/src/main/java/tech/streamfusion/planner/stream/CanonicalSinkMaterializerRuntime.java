package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.ArrowToRowDataOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.operator.SplitByKeyGroupOperator;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.StockSinkMaterializerStateContract;

/** Reuses stock sink translation while projecting its materializer's live native state. */
final class CanonicalSinkMaterializerRuntime implements CanonicalSinkIdentities {
  final StockSinkMaterializerStateContract contract;
  private final OneInputTransformation<?, ?> original;
  private final RowType type;
  private final int maximum;
  private final long ttl;
  private final String strategy;
  private final ReadableConfig strategyConfig;
  private final List<Integer> helpers = new ArrayList<>();
  private OneInputTransformation<ArrowBatch, ArrowBatch> materializer;

  CanonicalSinkMaterializerRuntime(
      OneInputTransformation<?, ?> original,
      RowType type,
      int maximum,
      long ttl,
      String strategy,
      ReadableConfig strategyConfig,
      StockSinkMaterializerStateContract contract) {
    this.original = original;
    this.type = type;
    this.maximum = maximum;
    this.ttl = ttl;
    this.strategy = strategy;
    this.strategyConfig = strategyConfig;
    this.contract = contract;
  }

  Transformation<RowData> apply(
      Transformation<RowData> input,
      int[] keys,
      int parallelism,
      ExecNodeConfig config,
      RowType physicalType,
      int[] upsertKeys,
      boolean parallelismConfigured) {
    if (!type.equals(physicalType))
      throw new IllegalStateException(
          "Captured sink materializer input type changed: " + type + " -> " + physicalType);
    int[] descriptor = FlinkKeyGroupUtils.stockKeyDescriptor(type, keys);
    int inputMaximum =
        Math.max(maximum, Math.max(input.getParallelism(), input.getMaxParallelism()));
    var entry =
        new OneInputTransformation<>(
            input,
            "RowDataToArrow",
            SimpleOperatorFactory.of(
                new RowDataToArrowOperator(
                    type,
                    NativeConfig.transposeBatchRows(config, 1024),
                    true,
                    null,
                    false,
                    contract.inputArrowSchema().serializeAsMessage())),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    entry.setMaxParallelism(inputMaximum);
    var split =
        new OneInputTransformation<>(
            entry,
            "sink-materializer-key-split",
            SimpleOperatorFactory.of(new SplitByKeyGroupOperator(keys, descriptor, maximum)),
            ArrowBatchTypeInformation.INSTANCE,
            entry.getParallelism(),
            false);
    split.setMaxParallelism(inputMaximum);
    var partition = new PartitionTransformation<>(split, new ColumnarKeyGroupPartitioner(maximum));
    partition.setParallelism(parallelism);
    partition.setMaxParallelism(maximum);
    var operator =
        SinkMaterializerFactory.createCanonical(
            type, keys, descriptor, upsertKeys, ttl, maximum, strategy, strategyConfig);
    operator.setStockStateContract(contract);
    materializer =
        new OneInputTransformation<>(
            partition,
            "NativeSinkMaterializer",
            SimpleOperatorFactory.of(operator),
            ArrowBatchTypeInformation.INSTANCE,
            parallelism,
            parallelismConfigured);
    if (original.getUid() != null) materializer.setUid(original.getUid());
    FlinkKeyGroupUtils.applyColumnarKeying(materializer, maximum);
    var exit =
        new OneInputTransformation<>(
            materializer,
            "ArrowToRowData",
            SimpleOperatorFactory.of(new ArrowToRowDataOperator(type)),
            InternalTypeInfo.of(type),
            parallelism,
            parallelismConfigured);
    exit.setMaxParallelism(maximum);
    helpers.add(entry.getId());
    helpers.add(split.getId());
    helpers.add(exit.getId());
    return exit;
  }

  @Override
  public int originalMaterializerId() {
    return original.getId();
  }

  @Override
  public OneInputTransformation<ArrowBatch, ArrowBatch> materializerTransformation() {
    if (materializer == null)
      throw new IllegalStateException("Sink materializer was not translated");
    return materializer;
  }

  @Override
  public StockSinkMaterializerStateContract materializerContract() {
    return contract;
  }

  @Override
  public List<Integer> helperIds() {
    return List.copyOf(helpers);
  }
}
