package tech.streamfusion.compat;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.flink.table.planner.plan.logical.WindowSpec;

/** Planner constructs selected at build time for the target Flink line. */
public final class FlinkCompat {
  public static boolean isBuiltinArrayAgg(org.apache.calcite.sql.SqlAggFunction function) {
    return function == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ARRAY_AGG;
  }

  private FlinkCompat() {}

  public static final boolean NARROW_CONSTRUCTORS_COMPILE = true;

  public static final boolean ORDERED_MAP_CONSTRUCTORS = true;

  public static final boolean DECIMAL_ARITHMETIC_CASTS_INTEGERS_TO_RESULT = false;

  public static final boolean TABLE_RUNTIME_ELEMENT_EXCEPTION = true;
  public static final boolean GENERIC_ORDERED_VALUES = true;

  public static long miniBatchSize(org.apache.flink.configuration.ReadableConfig config) {
    return org.apache.flink.table.planner.plan.utils.MinibatchUtil.miniBatchSize(config);
  }

  public static long joinMiniBatchSize(org.apache.flink.configuration.ReadableConfig config) {
    return miniBatchSize(config);
  }

  public static int timestampToTimeTruncationFactor(int precision) {
    return precision >= 3 ? 1 : (int) Math.pow(10, 3 - precision);
  }

  public static org.apache.flink.table.functions.ScalarFunction scalarFunction(
      org.apache.calcite.sql.SqlOperator operator) {
    Object scalarCandidate;
    if (operator instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
        && (scalarCandidate =
                ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) operator)
                    .getDefinition())
            instanceof org.apache.flink.table.functions.ScalarFunction) {
      org.apache.flink.table.functions.ScalarFunction scalar =
          ((org.apache.flink.table.functions.ScalarFunction) scalarCandidate);

      return scalar;
    }
    return null;
  }

  public static final boolean GLOBAL_TTL_EMITS_UNCHANGED = true;

  public static int groupWindowTimeColumn(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate
          node) {
    return node.window().timeAttribute().getFieldIndex();
  }

  public static final boolean EAGER_ROWTIME_KEEP_FIRST = false;

  public static Long singleStateTtl(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate node) {
    return org.apache.flink.table.planner.hint.StateTtlHint.getStateTtlFromHintOnSingleRel(
        node.hints());
  }

  public static Long singleStateTtl(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate
          node) {
    return org.apache.flink.table.planner.hint.StateTtlHint.getStateTtlFromHintOnSingleRel(
        node.hints());
  }

  public static Map<Integer, Long> joinStateTtl(List<RelHint> hints) {
    return org.apache.flink.table.planner.hint.StateTtlHint.getStateTtlFromHintOnBiRel(hints);
  }

  public static boolean isSessionWindow(WindowSpec window) {
    return window instanceof org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
  }

  public static long sessionGapMillis(WindowSpec window) {
    return ((org.apache.flink.table.planner.plan.logical.SessionWindowSpec) window)
        .getGap()
        .toMillis();
  }

  public static boolean isDeltaJoin(RelNode node) {
    return node
        instanceof
        org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
  }

  public static String unescapeJsonPath(String path) {
    return org.apache.flink.shaded.com.jayway.jsonpath.internal.Utils.unescape(path);
  }

  public static boolean forceDeltaJoin(RelNode node) {
    return org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(node)
            .get(
                org.apache.flink.table.api.config.OptimizerConfigOptions
                    .TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY)
        == org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy.FORCE;
  }

  public static boolean normalizeHasFilter(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize
          node) {
    return node.filterCondition() != null;
  }

  public static boolean normalizeSharesSource(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize
          node) {
    return node.sourceReused() || node.commonFilter().length > 0;
  }

  public static java.util.Optional<org.apache.calcite.rex.RexNode> watermarkRowtime(
      org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec spec) {
    return spec.getRowtimeExpr();
  }

  public static org.apache.flink.table.planner.plan.logical.WindowingStrategy windowDedupWindowing(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate
          node) {
    return node.getWindowingStrategy();
  }

  public static java.util.List<org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec>
      sinkAbilities(
          org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink sink) {
    return java.util.Arrays.asList(sink.abilitySpecs());
  }

  public static String expressionClassName(
      org.apache.flink.table.planner.codegen.CodeGeneratorContext context) {
    return org.apache.flink.table.planner.codegen.CodeGenUtils.newName(
        context, "FlinkExpressionEvaluator");
  }

  public static org.apache.flink.table.planner.functions.casting.CastRule.Context castContext(
      ClassLoader classLoader) {
    return castContext(classLoader, false);
  }

  public static org.apache.flink.table.planner.functions.casting.CastRule.Context castContext(
      ClassLoader classLoader, boolean legacyBehaviour) {
    return org.apache.flink.table.planner.functions.casting.CastRule.Context.create(
        false,
        legacyBehaviour,
        java.time.ZoneId.of("UTC"),
        classLoader,
        new org.apache.flink.table.planner.codegen.CodeGeneratorContext(
            new org.apache.flink.configuration.Configuration(), classLoader));
  }
}
