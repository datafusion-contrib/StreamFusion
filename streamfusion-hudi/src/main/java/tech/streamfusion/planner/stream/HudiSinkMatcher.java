package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.UpsertKeyUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.hudi.HudiSinkPlan;
import tech.streamfusion.hudi.NativeHudiAppendSink;
import tech.streamfusion.hudi.NativeHudiMorSink;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.StreamPlanningContext;

final class HudiSinkMatcher {
  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof HoodieTableSink;
  }

  static RelNode substitute(StreamPhysicalSink sink, StreamPlanningContext context) {
    if (sink.upsertMaterialize() && context.keyedStateUnsupportedReason() != null) {
      context.decline(context.keyedStateUnsupportedReason());
      return null;
    }
    String constraint = SinkConstraintGate.fallbackReason(sink, true);
    if (constraint != null) {
      context.decline(constraint);
      return null;
    }
    var table = (HoodieTableSink) sink.tableSink();
    var type = FlinkTypeFactory$.MODULE$.toLogicalRowType(sink.getInput().getRowType());
    HudiSinkPlan plan;
    try {
      if (OptionsResolver.isAppendMode(table.getConf())) {
        if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
          context.decline("Hudi native append requires insert-only input");
          return null;
        }
        plan = NativeHudiAppendSink.plan(table, type);
      } else {
        plan = NativeHudiMorSink.plan(table, type);
      }
    } catch (IllegalArgumentException | LinkageError failure) {
      context.decline("Hudi sink: " + failure.getMessage());
      return null;
    }
    int[] materializationKeys = null;
    int[] inputUpsertKeys = null;
    RelNode input = sink.getInput();
    if (sink.upsertMaterialize()) {
      var primary = sink.contextResolvedTable().getResolvedSchema().getPrimaryKey().orElseThrow();
      materializationKeys =
          primary.getColumns().stream().mapToInt(type.getFieldNames()::indexOf).toArray();
      var metadata = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster().getMetadataQuery());
      inputUpsertKeys =
          UpsertKeyUtil.getSmallestKey(metadata.getUpsertKeys(context.originalNode(input)));
      input =
          new StreamPhysicalNativeColumnarExchange(
              sink.getCluster(),
              input.getTraitSet(),
              input,
              input.getRowType(),
              materializationKeys,
              false);
    }
    boolean terminalUpserts =
        !OptionsResolver.isAppendMode(table.getConf())
            && table.getConf().get(FlinkOptions.PRE_COMBINE)
            && "COMMIT_TIME_ORDERING"
                .equals(table.getConf().toMap().get("hoodie.write.record.merge.mode"));
    input =
        wholeBatchesBeforeSink(
            input, terminalUpserts, OptionsResolver.isAppendMode(table.getConf()));
    return new StreamPhysicalNativeHudiSink(
        sink.getCluster(),
        sink.getTraitSet(),
        input,
        sink.getRowType(),
        plan,
        materializationKeys,
        inputUpsertKeys,
        NativePlanDetails.from(sink));
  }

  private static RelNode wholeBatchesBeforeSink(
      RelNode input, boolean terminalUpserts, boolean appendOnly) {
    if (input instanceof StreamPhysicalNativeWatermarkAssigner)
      return ((StreamPhysicalNativeWatermarkAssigner) input).wholeBatches();
    if (input instanceof StreamPhysicalNativeCalc) {
      var calc = (StreamPhysicalNativeCalc) input;
      if (calc.hasUserFunctions()) return input;
      if (observesWatermarks(calc)) return input;
    } else if (!canCross(input, terminalUpserts, appendOnly)) {
      return input;
    }
    var children = new ArrayList<RelNode>();
    boolean changed = false;
    for (RelNode child : input.getInputs()) {
      var replacement = wholeBatchesBeforeSink(child, terminalUpserts, appendOnly);
      children.add(replacement);
      changed |= replacement != child;
    }
    return changed ? input.copy(input.getTraitSet(), children) : input;
  }

  private static boolean canCross(RelNode input, boolean terminalUpserts, boolean appendOnly) {
    if (input instanceof StreamPhysicalNativeColumnarExchange
        || input instanceof StreamPhysicalNativeMiniBatchAssigner)
      return terminalUpserts || appendOnly;
    if (input instanceof StreamPhysicalNativeColumnarGroupAggregate
        || input instanceof StreamPhysicalNativeColumnarLocalGroupAggregate
        || input instanceof StreamPhysicalNativeColumnarTopN) return terminalUpserts;
    if (input instanceof StreamPhysicalNativeDeduplicate)
      return terminalUpserts && !((StreamPhysicalNativeDeduplicate) input).requireWatermark();
    if (input instanceof StreamPhysicalNativeColumnarUpdatingJoin) {
      var join = (StreamPhysicalNativeColumnarUpdatingJoin) input;
      return (terminalUpserts || appendOnly && join.isInsertOnlyInnerJoin())
          && !join.hasUserFunctions();
    }
    return false;
  }

  private static boolean observesWatermarks(StreamPhysicalNativeCalc calc) {
    var program = calc.sourceProgram();
    if (program == null) return true;
    var observes = new boolean[1];
    var visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getOperator().getName().equals("CURRENT_WATERMARK")) observes[0] = true;
            return super.visitCall(call);
          }
        };
    program.getExprList().forEach(expression -> expression.accept(visitor));
    return observes[0];
  }
}
