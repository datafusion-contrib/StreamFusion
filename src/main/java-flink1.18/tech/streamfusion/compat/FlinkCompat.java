package tech.streamfusion.compat;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.flink.table.planner.plan.logical.WindowSpec;

/** Planner constructs selected at build time for the target Flink line. */
public final class FlinkCompat {
  private FlinkCompat() {}

  public static org.apache.flink.table.functions.ScalarFunction scalarFunction(
      org.apache.calcite.sql.SqlOperator operator) {
    if (operator
            instanceof
            org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition()
            instanceof org.apache.flink.table.functions.ScalarFunction scalar) {
      return scalar;
    }
    // Subclasses can specialize makeFunction with argument values/types during host codegen.
    if (operator.getClass()
        == org.apache.flink.table.planner.functions.utils.ScalarSqlFunction.class) {
      return ((org.apache.flink.table.planner.functions.utils.ScalarSqlFunction) operator)
          .scalarFunction();
    }
    return null;
  }

  public static final boolean GLOBAL_TTL_EMITS_UNCHANGED = false;

  public static int groupWindowTimeColumn(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate
          node) {
    return node.getInput()
        .getRowType()
        .getFieldNames()
        .indexOf(node.window().timeAttribute().getName());
  }

  public static final boolean EAGER_ROWTIME_KEEP_FIRST = true;

  public static Long singleStateTtl(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate node) {
    return null;
  }

  public static Long singleStateTtl(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate
          node) {
    return null;
  }

  public static Map<Integer, Long> joinStateTtl(List<RelHint> hints) {
    return Map.of();
  }

  public static boolean isSessionWindow(WindowSpec window) {
    return false;
  }

  public static long sessionGapMillis(WindowSpec window) {
    throw new IllegalArgumentException("Flink 1.18 has no session window table function");
  }

  public static boolean isDeltaJoin(RelNode node) {
    return false;
  }

  public static String unescapeJsonPath(String path) {
    return org.apache.flink.table.shaded.com.jayway.jsonpath.internal.Utils.unescape(path);
  }

  public static boolean forceDeltaJoin(RelNode node) {
    return false;
  }

  public static boolean normalizeHasFilter(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize
          node) {
    return false;
  }

  public static boolean normalizeSharesSource(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize
          node) {
    return false;
  }

  public static java.util.Optional<org.apache.calcite.rex.RexNode> watermarkRowtime(
      org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec spec) {
    return java.util.Optional.empty();
  }

  public static org.apache.flink.table.planner.plan.logical.WindowingStrategy windowDedupWindowing(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate
          node) {
    if (node instanceof PreparedWindowDeduplicate prepared) return prepared.windowing;
    var query =
        org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery.reuseOrCreate(
            node.getCluster().getMetadataQuery());
    var properties = query.getRelWindowProperties(node.getInput());
    if (properties == null
        || properties.getWindowStartColumns().isEmpty()
        || properties.getWindowEndColumns().isEmpty()) return null;
    return new org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy(
        properties.getWindowSpec(), properties.getTimeAttributeType(),
        properties.getWindowStartColumns().nth(0), properties.getWindowEndColumns().nth(0));
  }

  public static RelNode prepareWindowDeduplicate(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate
          node,
      boolean keepLast) {
    var windowing = windowDedupWindowing(node);
    return windowing == null
        ? node
        : new PreparedWindowDeduplicate(
            node, node.getTraitSet(), node.getInput(), keepLast, windowing);
  }

  /** Capture released planner metadata before its input is replaced by native relations. */
  private static final class PreparedWindowDeduplicate
      extends org.apache.flink.table.planner.plan.nodes.physical.stream
          .StreamPhysicalWindowDeduplicate {
    private final boolean keepLast;
    private final org.apache.flink.table.planner.plan.logical.WindowingStrategy windowing;

    PreparedWindowDeduplicate(
        org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate
            node,
        org.apache.calcite.plan.RelTraitSet traits,
        RelNode input,
        boolean keepLast,
        org.apache.flink.table.planner.plan.logical.WindowingStrategy windowing) {
      super(
          node.getCluster(),
          traits,
          input,
          node.partitionKeys(),
          node.orderKey(),
          keepLast,
          windowing);
      this.keepLast = keepLast;
      this.windowing = windowing;
    }

    @Override
    public RelNode copy(org.apache.calcite.plan.RelTraitSet traits, List<RelNode> inputs) {
      return new PreparedWindowDeduplicate(this, traits, inputs.get(0), keepLast, windowing);
    }
  }

  public static java.util.List<org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec>
      sinkAbilities(
          org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink sink) {
    return ((org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink)
            sink.translateToExecNode())
        .getTableSinkSpec()
        .getSinkAbilities();
  }

  public static String expressionClassName(
      org.apache.flink.table.planner.codegen.CodeGeneratorContext context) {
    return org.apache.flink.table.planner.codegen.CodeGenUtils.newName("FlinkExpressionEvaluator");
  }

  public static org.apache.flink.table.planner.functions.casting.CastRule.Context castContext(
      ClassLoader classLoader) {
    return org.apache.flink.table.planner.functions.casting.CastRule.Context.create(
        false, false, java.time.ZoneId.of("UTC"), classLoader);
  }
}
