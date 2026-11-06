package tech.streamfusion.planner;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;

/**
 * The services a {@link Substitution} needs from the scan that runs it: rewriting a keyed host
 * exchange into a columnar one, counting a substitution, and recording why one was declined.
 *
 * <p>Passing these through a context rather than leaving them on the scan is what lets each
 * operator's planning live next to its matcher instead of inside the scan itself.
 */
final class PlanContext {

  private final PhysicalPlanScan scan;
  private final Set<String> repeatedSources;
  private final Set<RelNode> javaJsonInputs;
  private final Map<RelNode, RelNode> originals = new IdentityHashMap<>();
  private final Map<RelNode, StreamPhysicalGlobalGroupAggregate> globalGroups =
      new IdentityHashMap<>();

  private final Map<RelNode, Map<Substitution<?>, String>> probeFailures = new IdentityHashMap<>();

  /** Retain the host accumulator layout before child substitution changes its metadata. */
  void rememberGlobalGroup(StreamPhysicalGlobalGroupAggregate group) {
    RelNode local = group.getInput();
    if (local instanceof StreamPhysicalExchange) local = local.getInput(0);
    if (local instanceof StreamPhysicalLocalGroupAggregate) globalGroups.put(local, group);
  }

  StreamPhysicalGlobalGroupAggregate globalGroupForLocal(RelNode local) {
    return globalGroups.get(originalNode(local));
  }

  void probeFailed(Substitution<?> entry, RelNode node, String reason) {
    probeFailures.computeIfAbsent(node, ignored -> new IdentityHashMap<>()).put(entry, reason);
  }

  String probeFailure(Substitution<?> entry, RelNode node) {
    Map<Substitution<?>, String> failures = probeFailures.get(node);
    return failures == null ? null : failures.get(entry);
  }

  PlanContext(PhysicalPlanScan scan, Set<String> repeatedSources) {
    this(scan, repeatedSources, Set.of());
  }

  PlanContext(PhysicalPlanScan scan, Set<String> repeatedSources, Set<RelNode> javaJsonInputs) {
    this.scan = scan;
    this.repeatedSources = repeatedSources;
    this.javaJsonInputs = javaJsonInputs;
  }

  boolean requiresJavaJsonInput(RelNode node) {
    return javaJsonInputs.contains(originalNode(node));
  }

  void rememberOriginal(RelNode rewritten, RelNode original) {
    originals.put(rewritten, original);
  }

  RelNode originalNode(RelNode rewritten) {
    return originals.getOrDefault(rewritten, rewritten);
  }

  String keyedStateUnsupportedReason() {
    return scan.keyedStateUnsupportedReason();
  }

  /** Counts one host node replaced by a native one. */
  void substituted() {
    scan.countSubstitution();
  }

  /** Records why a node the scan recognized stays on the host. */
  void decline(String reason) {
    scan.recordFallback(reason);
  }

  /** Records the final Rust/JVM function assignment for one admitted Calc or filter. */
  void functionExecution(String operator, RexExpression expression) {
    scan.recordFunctionExecution(operator, expression);
  }

  boolean repeatedSource(String sharingKey) {
    return repeatedSources.contains(sharingKey);
  }

  /**
   * Replaces a keyed host exchange with a native columnar one (splitting the batch by the key), so
   * the shuffle is always part of the columnar island. When the exchange's producer is rowwise the
   * transition pass inserts a single transpose below the native exchange (the island perimeter);
   * when it is columnar no transpose is needed. A non-exchange input is returned unchanged.
   */
  RelNode columnarInput(RelNode input, int[] keyColumns) {
    return columnarInput(input, keyColumns, false);
  }

  /** Some SINGLETON consumers keep their state in a fixed one-key-group namespace. */
  RelNode columnarInput(RelNode input, int[] keyColumns, boolean singleKeyGroup) {
    return columnarInput(input, keyColumns, singleKeyGroup, false);
  }

  RelNode columnarInput(
      RelNode input, int[] keyColumns, boolean singleKeyGroup, boolean projectedKeys) {
    if (input instanceof StreamPhysicalExchange) {
      return new StreamPhysicalNativeColumnarExchange(
          input.getCluster(),
          input.getTraitSet(),
          input.getInputs().get(0),
          input.getRowType(),
          keyColumns,
          singleKeyGroup,
          projectedKeys);
    }
    return input;
  }
}
