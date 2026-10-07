package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.predicate.Predicate;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeSourceWatermarks;
import tech.streamfusion.paimon.NativePaimonSource;

final class NativePaimonSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final FileStoreTable table;
  private final RowType output;
  private final ScanWatermarkSpec watermark;
  private final Predicate partitionPredicate;

  NativePaimonSourceExecNode(
      ReadableConfig config,
      RowType output,
      String description,
      FileStoreTable table,
      ScanWatermarkSpec watermark,
      Predicate partitionPredicate) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-paimon-source_1"),
        config,
        Collections.emptyList(),
        output,
        description);
    this.table = table;
    this.output = output;
    this.watermark = watermark;
    this.partitionPredicate = partitionPredicate;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var env = planner.getExecEnv();
    int[] projection =
        output.getFieldNames().stream()
            .mapToInt(table.rowType().getFieldNames()::indexOf)
            .toArray();
    var source =
        new NativePaimonSource(
            table,
            projection,
            4096,
            watermark == null ? -1 : watermark.rowtimeIndex,
            watermark == null ? null : watermark.expression,
            partitionPredicate);
    WatermarkStrategy<ArrowBatch> strategy =
        watermark == null
            ? WatermarkStrategy.noWatermarks()
            : NativeSourceWatermarks.strategy(watermark.idleTimeoutMillis);
    var stream =
        env.fromSource(
            source, strategy, "native-paimon-source", ArrowBatchTypeInformation.INSTANCE);
    Options options = Options.fromMap(table.options());
    String uidSuffix = options.get(FlinkConnectorOptions.SOURCE_OPERATOR_UID_SUFFIX);
    if (!org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(uidSuffix)) {
      stream.uid(FlinkConnectorOptions.generateCustomUid("Source", table.name(), uidSuffix));
    }
    Integer parallelism = options.get(FlinkConnectorOptions.SCAN_PARALLELISM);
    boolean infer =
        Boolean.parseBoolean(
            env.getConfiguration()
                .getOptional(
                    org.apache.flink.configuration.ConfigOptions.key(
                            "paimon.scan.infer-parallelism")
                        .stringType()
                        .noDefaultValue())
                .orElse(String.valueOf(options.get(FlinkConnectorOptions.INFER_SCAN_PARALLELISM))));
    if (parallelism == null
        && env.getParallelism() == -1
        && infer
        && options.get(CoreOptions.BUCKET) != -1) {
      parallelism = Math.max(1, options.get(CoreOptions.BUCKET));
    }
    if (parallelism != null) {
      stream.setParallelism(parallelism);
    }
    return stream.getTransformation();
  }
}
