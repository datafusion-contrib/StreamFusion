package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.IcebergTableSink;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.iceberg.IcebergSinkPlan;
import tech.streamfusion.iceberg.NativeIcebergSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.StreamRule;

/** Optional Iceberg rewrites retain the host's scan and commit topology. */
public class IcebergPlannerExtension implements NativePlannerExtension {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergPlannerExtension.class);

  @Override
  public BatchSource planBatchSource(BatchPhysicalTableSourceScan scan, ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("icebergSource")) return null;
    var table = scan.getTable().unwrap(TableSourceTable.class);
    if (table == null || !(table.tableSource() instanceof IcebergTableSource)) return null;
    try {
      var plan = IcebergSourceMatcher.plan(scan, table, true);
      return new BatchSource() {
        @Override
        public boolean guaranteesUtf8Strings() {
          // Iceberg string fields are UTF-8 in every admitted file format. The Arrow bridge
          // validates string bytes before native text kernels can consume a batch.
          return true;
        }

        @Override
        public Transformation<ArrowBatch> translate(
            PlannerBase planner, Transformation<RowData> input, RowType output, int batchRows) {
          return NativeIcebergSource.readBatch(input, plan, batchRows);
        }
      };
    } catch (IllegalArgumentException | LinkageError failure) {
      LOG.info("StreamFusion batch Iceberg source fallback: {}", failure.getMessage());
      return null;
    }
  }

  @Override
  public BatchSink planBatchSink(BatchPhysicalSink sink, ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("icebergSink")
        || !(sink.tableSink() instanceof IcebergTableSink iceberg)) return null;
    for (var ability : sink.abilitySpecs()) {
      if (!(ability instanceof OverwriteSpec || ability instanceof PartitioningSpec)) return null;
    }
    try {
      var plan =
          IcebergSinkPlan.capture(
              iceberg, FlinkTypeFactory.toLogicalRowType(sink.getInput().getRowType()), true);
      return plan::translateBatch;
    } catch (IllegalArgumentException | LinkageError failure) {
      LOG.info("StreamFusion batch Iceberg sink fallback: {}", failure.getMessage());
      return null;
    }
  }

  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(
                org.apache.flink.table.planner.plan.nodes.physical.stream
                    .StreamPhysicalTableSourceScan.class,
                "icebergSource",
                IcebergSourceMatcher::substitute)
            .matching(IcebergSourceMatcher::appliesTo));
    substitutions.add(
        StreamRule.of(StreamPhysicalSink.class, "icebergSink", IcebergSinkMatcher::substitute)
            .matching(IcebergSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
