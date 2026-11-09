package tech.streamfusion.compat;

import java.util.function.Supplier;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

public final class FlinkPlanCompat {
  private FlinkPlanCompat() {}

  public static boolean restoresWindowTimerWatermark() {
    return true;
  }

  public static boolean hasDedupFilter(Object function) throws java.io.IOException {
    try {
      var field = function.getClass().getDeclaredField("genFilterCondition");
      field.setAccessible(true);
      return field.get(function) != null;
    } catch (ReflectiveOperationException failure) {
      throw new java.io.IOException("Cannot capture released deduplication filter", failure);
    }
  }

  public static boolean windowEventTime(Object owner) throws java.io.IOException {
    try {
      for (Class<?> type = owner.getClass(); type != null; type = type.getSuperclass()) {
        try {
          var field = type.getDeclaredField("isEventTime");
          field.setAccessible(true);
          return field.getBoolean(owner);
        } catch (NoSuchFieldException inherited) {
          // Continue through the released processor hierarchy.
        }
      }
      throw new NoSuchFieldException("isEventTime");
    } catch (ReflectiveOperationException failure) {
      throw new java.io.IOException("Cannot capture released window clock", failure);
    }
  }

  public static ObjectReader createJsonObjectReader(SerdeContext context) {
    return org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil
        .createJsonObjectReader(context);
  }

  public static ObjectWriter createJsonObjectWriter(SerdeContext context) {
    return org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil
        .createJsonObjectWriter(context);
  }

  public static ObjectWriter createSmileObjectWriter(SerdeContext context) {
    return org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil
        .createSmileObjectWriter(context);
  }

  public static ExecNodeGraphInternalPlan plan(
      Supplier<String> json, Supplier<byte[]> smile, ExecNodeGraph graph) {
    return new ExecNodeGraphInternalPlan(json, smile, graph);
  }

  public static ExecNodeGraphInternalPlan plan(
      Supplier<String> json,
      Supplier<byte[]> smile,
      ExecNodeGraph graph,
      java.util.List<String> sinks) {
    return new ExecNodeGraphInternalPlan(json, smile, graph) {
      @Override
      public java.util.List<String> getSinkIdentifiers() {
        return sinks;
      }
    };
  }

  public static byte[] planBytes(org.apache.flink.table.delegation.InternalPlan plan) {
    return plan.asSmileBytes();
  }

  public static Class<?> legacyWindowOperator() {
    return org.apache.flink.table.runtime.operators.window.groupwindow.operator
        .AggregateWindowOperator.class;
  }

  public static Class<?> legacySessionAssigner() {
    return org.apache.flink.table.runtime.operators.window.groupwindow.assigners
        .SessionWindowAssigner.class;
  }

  public static Class<?> windowOperator() {
    return org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator.class;
  }

  public static Class<?> windowProcessor() {
    return org.apache.flink.table.runtime.operators.aggregate.window.processors
        .AbstractSyncStateWindowAggProcessor.class;
  }

  public static Class<?> windowDedupProcessor() {
    return org.apache.flink.table.runtime.operators.deduplicate.window.processors
        .RowTimeSyncStateWindowDeduplicateProcessor.class;
  }

  public static Class<?> windowRankProcessor() {
    return org.apache.flink.table.runtime.operators.rank.window.processors
        .SyncStateWindowRankProcessor.class;
  }

  public static boolean releasedMiniBatchJoins() {
    return true;
  }

  public static boolean joinKeyContainsUniqueKey(Object side) {
    return ((org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec) side)
        .joinKeyContainsUniqueKey();
  }

  public static tech.streamfusion.state.StockJoinStateContract.SideSpec joinSide(
      org.apache.flink.table.types.logical.RowType type, Object side, boolean outer, long ttl) {
    var spec = (org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec) side;
    boolean unique = spec.hasUniqueKey() && !spec.joinKeyContainsUniqueKey();
    return new tech.streamfusion.state.StockJoinStateContract.SideSpec(
        type,
        spec.joinKeyContainsUniqueKey(),
        unique ? spec.getUniqueKeyType().toRowType() : null,
        unique ? spec.getUniqueKeySelector() : null,
        outer,
        ttl);
  }

  public static org.apache.flink.configuration.Configuration persistedConfig(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> node) {
    return FlinkStateBackendCompat.configuration(
        ((org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase<?>) node)
            .getPersistedConfig());
  }

  public static org.apache.flink.api.common.serialization.SerializerConfig serializerConfig(
      org.apache.flink.api.common.ExecutionConfig config) {
    return config.getSerializerConfig();
  }

  public static long ttlMillis(org.apache.flink.api.common.state.StateTtlConfig ttl) {
    return ttl.getTimeToLive().toMillis();
  }

  public static org.apache.flink.runtime.jobgraph.JobGraph jobGraph(
      org.apache.flink.streaming.api.graph.StreamGraph graph, ClassLoader loader) {
    return graph.getJobGraph(loader);
  }

  public static int maximumParallelism(org.apache.flink.streaming.api.graph.StreamNode node) {
    return node.getMaxParallelism();
  }

  public static org.apache.flink.table.planner.plan.utils.AggregateInfoList windowAggregates(
      org.apache.flink.table.planner.calcite.FlinkTypeFactory factory,
      org.apache.flink.table.types.logical.RowType input,
      scala.collection.Seq<org.apache.calcite.rel.core.AggregateCall> calls,
      org.apache.flink.table.planner.plan.logical.WindowSpec window) {
    return org.apache.flink.table.planner.plan.utils.AggregateUtil
        .deriveStreamWindowAggregateInfoList(factory, input, calls, false, window, true);
  }

  public static tech.streamfusion.state.StockSlicingWindowStateContract.WindowStart windowStart(
      org.apache.flink.table.planner.plan.logical.WindowSpec spec,
      java.time.ZoneId zone,
      long size,
      long slide,
      java.time.Duration offset) {
    var shift = offset == null ? java.time.Duration.ZERO : offset;
    org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner assigner;
    if (spec instanceof org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.cumulative(
                  Integer.MAX_VALUE,
                  zone,
                  java.time.Duration.ofMillis(size),
                  java.time.Duration.ofMillis(slide))
              .withOffset(shift);
    else if (spec instanceof org.apache.flink.table.planner.plan.logical.HoppingWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.hopping(
                  Integer.MAX_VALUE,
                  zone,
                  java.time.Duration.ofMillis(size),
                  java.time.Duration.ofMillis(slide))
              .withOffset(shift);
    else if (spec instanceof org.apache.flink.table.planner.plan.logical.TumblingWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.tumbling(
                  Integer.MAX_VALUE, zone, java.time.Duration.ofMillis(size))
              .withOffset(shift);
    else throw new IllegalArgumentException("Unsupported stock fixed window: " + spec);
    return assigner::getWindowStart;
  }

  public static int unboundedOverVersion() {
    return 2;
  }

  public static Class<?> eventUnboundedOver(boolean rows) {
    return org.apache.flink.table.runtime.operators.over.RowTimeUnboundedPrecedingOverFunctionV2
        .class;
  }

  public static org.apache.flink.table.planner.plan.nodes.physical.stream
          .StreamPhysicalTableSourceScan
      sourceScan(
          org.apache.calcite.plan.RelOptCluster cluster,
          org.apache.flink.table.planner.plan.schema.TableSourceTable table) {
    return new org.apache.flink.table.planner.plan.nodes.physical.stream
        .StreamPhysicalTableSourceScan(
        cluster, cluster.traitSet(), java.util.List.of(), table, false);
  }

  public static Class<?> constraintOperator() {
    return org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer.class;
  }

  public static byte[] constraintDefinition(Object operator) {
    try {
      var field = constraintOperator().getDeclaredField("executor");
      field.setAccessible(true);
      return org.apache.flink.util.InstantiationUtil.serializeObject(field.get(operator));
    } catch (ReflectiveOperationException | java.io.IOException failure) {
      throw new IllegalStateException("Cannot capture released sink constraints", failure);
    }
  }

  public static org.apache.flink.api.common.state.StateTtlConfig stateTtl(Object function)
      throws java.io.IOException {
    try {
      for (Class<?> type = function.getClass(); type != null; type = type.getSuperclass()) {
        try {
          var field = type.getDeclaredField("ttlConfig");
          field.setAccessible(true);
          return (org.apache.flink.api.common.state.StateTtlConfig) field.get(function);
        } catch (NoSuchFieldException absent) {
          continue;
        }
      }
      throw new NoSuchFieldException("ttlConfig");
    } catch (ReflectiveOperationException failure) {
      throw new java.io.IOException("Cannot capture released state TTL", failure);
    }
  }
}
