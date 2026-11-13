package tech.streamfusion.planner.batch;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.NativePlannerExtensions;
import tech.streamfusion.planner.common.NativeRelDigests;

/** Applies optional connector boundaries independently of the batch compute island's coverage. */
final class BatchConnectorPlans {
  private final List<NativePlannerExtension> providers;
  private final ReadableConfig config;
  private final IdentityHashMap<RelNode, RelNode> rewritten = new IdentityHashMap<>();

  BatchConnectorPlans(ReadableConfig config, ClassLoader loader) {
    this.config = config;
    providers = NativePlannerExtensions.load(loader).extensions();
  }

  boolean hasNativeSource(RelNode node) {
    if (!(node instanceof BatchPhysicalTableSourceScan)) return false;
    for (var provider : providers)
      if (provider.planBatchSource((BatchPhysicalTableSourceScan) node, config) != null)
        return true;
    return false;
  }

  List<RelNode> lower(List<RelNode> roots) {
    if (!NativeConfig.nativeEnabled()) return roots;
    var result = new ArrayList<RelNode>();
    for (var root : roots) result.add(lower(root));
    var counts = new IdentityHashMap<RelNode, Integer>();
    var seen = new IdentityHashMap<RelNode, Boolean>();
    for (var root : result) count(root, counts, seen);
    var shared = new IdentityHashMap<RelNode, RelNode>();
    return result.stream()
        .map(root -> share(root, counts, shared))
        .collect(java.util.stream.Collectors.toList());
  }

  private RelNode lower(RelNode node) {
    var known = rewritten.get(node);
    if (known != null) return known;
    if (node instanceof BatchPhysicalTableSourceScan) {
      for (var provider : providers) {
        var source = provider.planBatchSource((BatchPhysicalTableSourceScan) node, config);
        if (source != null) {
          var result = new BatchPhysicalNativeConnectorSource(node, source);
          rewritten.put(node, result);
          return result;
        }
      }
    }
    RelNode target = node;
    if (node instanceof BatchPhysicalSink) {
      for (var provider : providers) {
        var sink = provider.planBatchSink((BatchPhysicalSink) node, config);
        if (sink != null) {
          target = new BatchPhysicalNativeConnectorSink(node, node.getInput(0), sink);
          break;
        }
      }
    }
    var inputs = new ArrayList<RelNode>();
    boolean changed = target != node;
    for (var input : node.getInputs()) {
      var lowered = lower(input);
      if (node instanceof BatchPhysicalRowDataToArrow && lowered instanceof ColumnarOutput) {
        rewritten.put(node, lowered);
        return lowered;
      }
      if (node instanceof BatchPhysicalArrowToRowData && !(lowered instanceof ColumnarOutput)) {
        rewritten.put(node, lowered);
        return lowered;
      }
      if (target instanceof ColumnarInput) {
        if (lowered instanceof BatchPhysicalArrowToRowData) lowered = lowered.getInput(0);
        if (!(lowered instanceof ColumnarOutput))
          lowered = new BatchPhysicalRowDataToArrow(lowered);
      } else if (!(target instanceof BatchPhysicalRowDataToArrow)
          && lowered instanceof ColumnarOutput) {
        lowered = new BatchPhysicalArrowToRowData(lowered);
      }
      inputs.add(lowered);
      changed |= lowered != input;
    }
    var result = changed ? target.copy(target.getTraitSet(), inputs) : target;
    rewritten.put(node, result);
    return result;
  }

  private static void count(
      RelNode node,
      IdentityHashMap<RelNode, Integer> counts,
      IdentityHashMap<RelNode, Boolean> seen) {
    if (seen.put(node, true) != null) return;
    for (var child : node.getInputs()) {
      counts.merge(child, 1, Integer::sum);
      count(child, counts, seen);
    }
  }

  private static RelNode share(
      RelNode node,
      IdentityHashMap<RelNode, Integer> counts,
      IdentityHashMap<RelNode, RelNode> shared) {
    var known = shared.get(node);
    if (known != null) return known;
    var inputs = new ArrayList<RelNode>();
    boolean changed = false;
    for (var child : node.getInputs()) {
      var replacement = share(child, counts, shared);
      inputs.add(replacement);
      changed |= child != replacement;
    }
    RelNode result = changed ? node.copy(node.getTraitSet(), inputs) : node;
    if (node instanceof BatchPhysicalNativeConnectorSource && counts.getOrDefault(node, 0) > 1)
      result = new BatchPhysicalNativeShare(result, counts.get(node), NativeRelDigests.nextId());
    shared.put(node, result);
    return result;
  }
}
