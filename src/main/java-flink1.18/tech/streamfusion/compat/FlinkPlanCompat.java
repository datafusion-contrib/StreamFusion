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
    return false;
  }

  public static boolean hasDedupFilter(Object function) throws java.io.IOException {
    return false;
  }

  public static boolean windowEventTime(Object owner) throws java.io.IOException {
    try {
      if (owner.getClass() == windowOperator()) {
        var processor = owner.getClass().getDeclaredField("windowProcessor");
        processor.setAccessible(true);
        owner = processor.get(owner);
      }
      if (owner.getClass() == windowRankProcessor() || owner.getClass() == windowDedupProcessor())
        return true;
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
    return org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectReader(
        context);
  }

  public static ObjectWriter createJsonObjectWriter(SerdeContext context) {
    return org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectWriter(
        context);
  }

  public static ObjectWriter createSmileObjectWriter(SerdeContext context) {
    throw new UnsupportedOperationException("Flink 1.18 compiled plans use JSON");
  }

  public static ExecNodeGraphInternalPlan plan(
      Supplier<String> json, Supplier<byte[]> smile, ExecNodeGraph graph) {
    return new ExecNodeGraphInternalPlan(json.get(), graph);
  }

  public static ExecNodeGraphInternalPlan plan(
      Supplier<String> json,
      Supplier<byte[]> smile,
      ExecNodeGraph graph,
      java.util.List<String> sinks) {
    return new ExecNodeGraphInternalPlan(json.get(), graph) {
      @Override
      public java.util.List<String> getSinkIdentifiers() {
        return sinks;
      }
    };
  }

  public static byte[] planBytes(org.apache.flink.table.delegation.InternalPlan plan) {
    throw new UnsupportedOperationException("Flink 1.18 compiled plans use JSON");
  }

  public static Class<?> legacyWindowOperator() {
    return org.apache.flink.table.runtime.operators.window.AggregateWindowOperator.class;
  }

  public static Class<?> legacySessionAssigner() {
    return org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner.class;
  }

  public static Class<?> windowOperator() {
    return org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator.class;
  }

  public static Class<?> windowProcessor() {
    return org.apache.flink.table.runtime.operators.aggregate.window.processors
        .AbstractWindowAggProcessor.class;
  }

  public static Class<?> windowDedupProcessor() {
    return org.apache.flink.table.runtime.operators.deduplicate.window.processors
        .RowTimeWindowDeduplicateProcessor.class;
  }

  public static Class<?> windowRankProcessor() {
    return org.apache.flink.table.runtime.operators.rank.window.processors.WindowRankProcessor
        .class;
  }

  public static boolean releasedMiniBatchJoins() {
    return false;
  }

  public static boolean joinKeyContainsUniqueKey(Object side) {
    return ((org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec) side)
        .joinKeyContainsUniqueKey();
  }

  public static tech.streamfusion.state.StockJoinStateContract.SideSpec joinSide(
      org.apache.flink.table.types.logical.RowType type, Object side, boolean outer, long ttl) {
    var spec = (org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec) side;
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

  public static org.apache.flink.api.common.ExecutionConfig serializerConfig(
      org.apache.flink.api.common.ExecutionConfig config) {
    return config;
  }

  public static long ttlMillis(org.apache.flink.api.common.state.StateTtlConfig ttl) {
    return ttl.getTtl().toMilliseconds();
  }

  public static org.apache.flink.runtime.jobgraph.JobGraph jobGraph(
      org.apache.flink.streaming.api.graph.StreamGraph graph, ClassLoader loader) {
    return graph.getJobGraph(loader, null);
  }

  public static int maximumParallelism(org.apache.flink.streaming.api.graph.StreamNode node) {
    try {
      var method = node.getClass().getDeclaredMethod("getMaxParallelism");
      method.setAccessible(true);
      return (int) method.invoke(node);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot capture released maximum parallelism", failure);
    }
  }

  public static org.apache.flink.table.planner.plan.utils.AggregateInfoList windowAggregates(
      org.apache.flink.table.planner.calcite.FlinkTypeFactory factory,
      org.apache.flink.table.types.logical.RowType input,
      scala.collection.Seq<org.apache.calcite.rel.core.AggregateCall> calls,
      org.apache.flink.table.planner.plan.logical.WindowSpec window) {
    return org.apache.flink.table.planner.plan.utils.AggregateUtil
        .deriveStreamWindowAggregateInfoList(factory, input, calls, window, true);
  }

  public static tech.streamfusion.state.StockSlicingWindowStateContract.WindowStart windowStart(
      org.apache.flink.table.planner.plan.logical.WindowSpec spec,
      java.time.ZoneId zone,
      long size,
      long slide,
      java.time.Duration offset) {
    var shift = offset == null ? java.time.Duration.ZERO : offset;
    org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner assigner;
    if (spec instanceof org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners.cumulative(
                  Integer.MAX_VALUE,
                  zone,
                  java.time.Duration.ofMillis(size),
                  java.time.Duration.ofMillis(slide))
              .withOffset(shift);
    else if (spec instanceof org.apache.flink.table.planner.plan.logical.HoppingWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners.hopping(
                  Integer.MAX_VALUE,
                  zone,
                  java.time.Duration.ofMillis(size),
                  java.time.Duration.ofMillis(slide))
              .withOffset(shift);
    else if (spec instanceof org.apache.flink.table.planner.plan.logical.TumblingWindowSpec)
      assigner =
          org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners.tumbling(
                  Integer.MAX_VALUE, zone, java.time.Duration.ofMillis(size))
              .withOffset(shift);
    else throw new IllegalArgumentException("Unsupported stock fixed window: " + spec);
    return assigner::getWindowStart;
  }

  public static int unboundedOverVersion() {
    return 1;
  }

  public static Class<?> eventUnboundedOver(boolean rows) {
    return rows
        ? org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction.class
        : org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction
            .class;
  }

  public static org.apache.flink.table.planner.plan.nodes.physical.stream
          .StreamPhysicalTableSourceScan
      sourceScan(
          org.apache.calcite.plan.RelOptCluster cluster,
          org.apache.flink.table.planner.plan.schema.TableSourceTable table) {
    return new org.apache.flink.table.planner.plan.nodes.physical.stream
        .StreamPhysicalTableSourceScan(cluster, cluster.traitSet(), java.util.List.of(), table);
  }

  public static Class<?> constraintOperator() {
    return org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer.class;
  }

  public static byte[] constraintDefinition(Object operator) {
    try {
      var definition = new java.util.ArrayList<Object>();
      for (String name :
          java.util.List.of(
              "notNullEnforcer",
              "notNullFieldIndices",
              "allFieldNames",
              "typeLengthEnforcer",
              "charFieldIndices",
              "charFieldLengths",
              "charFieldCouldPad",
              "binaryFieldIndices",
              "binaryFieldLengths",
              "binaryFieldCouldPad")) {
        var field = constraintOperator().getDeclaredField(name);
        field.setAccessible(true);
        definition.add(field.get(operator));
      }
      return org.apache.flink.util.InstantiationUtil.serializeObject(definition);
    } catch (ReflectiveOperationException | java.io.IOException failure) {
      throw new IllegalStateException("Cannot capture released sink constraints", failure);
    }
  }

  public static org.apache.flink.api.common.state.StateTtlConfig stateTtl(Object function)
      throws java.io.IOException {
    try {
      if (function instanceof org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction) {
        var field =
            org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction.class
                .getDeclaredField("stateRetentionTime");
        field.setAccessible(true);
        return org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig(
            (long) field.get(function));
      }
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
