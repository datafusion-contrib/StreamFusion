package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.PaimonBatchSource;
import tech.streamfusion.planner.common.PaimonSourceAdmission;
import tech.streamfusion.planner.common.StreamRule;

/** Bounded and streaming Paimon rewrites contributed by the optional Paimon module. */
public class PaimonPlannerExtension implements NativePlannerExtension {
  private static final Logger LOG = LoggerFactory.getLogger(PaimonPlannerExtension.class);

  @Override
  public BatchSource planBatchSource(BatchPhysicalTableSourceScan scan, ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("paimonSource")) return null;
    var resolved = scan.getTable().unwrap(TableSourceTable.class);
    if (resolved == null || !(resolved.tableSource() instanceof DataTableSource)) return null;
    var output = FlinkTypeFactory$.MODULE$.toLogicalRowType(scan.getRowType());
    String fallback = PaimonSourceAdmission.fallback(resolved, output, false);
    if (fallback != null) {
      LOG.info("StreamFusion batch Paimon source fallback: {}", fallback);
      return null;
    }
    var source = (DataTableSource) resolved.tableSource();
    return new PaimonBatchSource((FileStoreTable) source.getTable());
  }

  @Override
  public BatchSink planBatchSink(BatchPhysicalSink sink, ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("paimonSink")
        || !(sink.tableSink() instanceof FlinkTableSink)) return null;
    var planned = PaimonSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      LOG.info("StreamFusion batch Paimon sink fallback: {}", planned.fallbackReason);
      return null;
    }
    return (planner, input, description) ->
        NativePaimonSinkExecNode.translate(planner, input, planned, description);
  }

  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(
                StreamPhysicalTableSourceScan.class,
                "paimonSource",
                PaimonSourceMatcher::substitute)
            .matching(PaimonSourceMatcher::appliesTo)
            .changelogSafe());
    substitutions.add(
        StreamRule.of(StreamPhysicalSink.class, PaimonSinkMatcher::substitute)
            .matching(PaimonSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
