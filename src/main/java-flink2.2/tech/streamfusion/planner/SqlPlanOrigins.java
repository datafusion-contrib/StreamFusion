package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;

/** Keeps original logical roles through physical copies without changing execution nodes. */
final class SqlPlanOrigins {
  private final Map<Integer, ExecNode<?>> stockNodes = new HashMap<>();
  private final Map<ExecNode<?>, ExecNode<?>> runtimeNodes = new IdentityHashMap<>();
  private ExecNodeGraph stockGraph;

  List<RelNode> captureStock(List<RelNode> roots) {
    return capture(roots, (node, exec) -> stockNodes.put(node.getId(), exec));
  }

  void stockGraph(ExecNodeGraph graph) {
    stockGraph = graph;
  }

  List<RelNode> mark(List<RelNode> roots) {
    var visited = new IdentityHashMap<RelNode, RelNode>();
    var marked = new ArrayList<RelNode>();
    for (RelNode root : roots) marked.add(mark(root, visited));
    return marked;
  }

  private RelNode mark(RelNode node, Map<RelNode, RelNode> visited) {
    if (visited.containsKey(node)) return visited.get(node);
    var inputs = new ArrayList<RelNode>();
    for (RelNode input : node.getInputs()) inputs.add(mark(input, visited));
    RelNode result = node.copy(node.getTraitSet().plus(new Origin(node.getId())), inputs);
    visited.put(node, result);
    return result;
  }

  List<RelNode> captureRuntime(List<RelNode> roots) {
    return capture(
        roots,
        (node, exec) -> {
          Origin origin = node.getTraitSet().getTrait(Origin.DEF);
          if (origin != null && stockNodes.containsKey(origin.id))
            runtimeNodes.put(exec, stockNodes.get(origin.id));
        });
  }

  RuntimeGraph runtimeGraph(ExecNodeGraph graph) {
    return new RuntimeGraph(graph, stockGraph, runtimeNodes);
  }

  private static List<RelNode> capture(
      List<RelNode> roots, BiConsumer<RelNode, ExecNode<?>> observer) {
    var visited = new IdentityHashMap<RelNode, RelNode>();
    var result = new ArrayList<RelNode>();
    for (RelNode root : roots) result.add(capture(root, observer, visited));
    return result;
  }

  private static RelNode capture(
      RelNode node, BiConsumer<RelNode, ExecNode<?>> observer, Map<RelNode, RelNode> visited) {
    if (visited.containsKey(node)) return visited.get(node);
    var inputs = new ArrayList<RelNode>();
    for (RelNode input : node.getInputs()) inputs.add(capture(input, observer, visited));
    var result = new Capture(node, inputs, observer);
    visited.put(node, result);
    return result;
  }

  static final class RuntimeGraph extends ExecNodeGraph {
    final ExecNodeGraph stock;
    final Map<ExecNode<?>, ExecNode<?>> originals;

    RuntimeGraph(
        ExecNodeGraph runtime, ExecNodeGraph stock, Map<ExecNode<?>, ExecNode<?>> originals) {
      super(runtime.getFlinkVersion(), runtime.getRootNodes());
      this.stock = stock;
      this.originals = new IdentityHashMap<>(originals);
    }
  }

  /** Used only at the released graph-generator boundary, after relational optimization. */
  private static final class Capture extends AbstractRelNode implements StreamPhysicalRel {
    private final RelNode original;
    private final List<RelNode> inputs;
    private final BiConsumer<RelNode, ExecNode<?>> observer;

    Capture(RelNode original, List<RelNode> inputs, BiConsumer<RelNode, ExecNode<?>> observer) {
      super(original.getCluster(), original.getTraitSet());
      this.original = original;
      this.inputs = List.copyOf(inputs);
      this.observer = observer;
    }

    @Override
    public List<RelNode> getInputs() {
      return inputs;
    }

    @Override
    protected RelDataType deriveRowType() {
      return original.getRowType();
    }

    @Override
    public boolean requireWatermark() {
      return ((StreamPhysicalRel) original).requireWatermark();
    }

    @Override
    public ExecNode<?> translateToExecNode() {
      return translateToExecNode(false);
    }

    @Override
    public ExecNode<?> translateToExecNode(boolean compiled) {
      ExecNode<?> exec = ((FlinkPhysicalRel) original).translateToExecNode(compiled);
      observer.accept(original, exec);
      return exec;
    }

    @Override
    public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
      return new Capture(original, inputs, observer);
    }

    @Override
    public RelWriter explainTerms(RelWriter writer) {
      return ((AbstractRelNode) original).explainTerms(writer);
    }
  }

  // Applied after optimization; registering this metadata would change later logical trait layouts.
  private static final class Origin implements RelTrait {
    static final Origin NONE = new Origin(-1);
    static final RelTraitDef<Origin> DEF =
        new RelTraitDef<>() {
          @Override
          public Class<Origin> getTraitClass() {
            return Origin.class;
          }

          @Override
          public String getSimpleName() {
            return "sqlOrigin";
          }

          @Override
          public Origin getDefault() {
            return NONE;
          }

          @Override
          public RelNode convert(
              RelOptPlanner planner, RelNode node, Origin target, boolean infinite) {
            return null;
          }

          @Override
          public boolean canConvert(RelOptPlanner planner, Origin from, Origin to) {
            return from.equals(to);
          }
        };
    private final int id;

    Origin(int id) {
      this.id = id;
    }

    @Override
    public RelTraitDef<Origin> getTraitDef() {
      return DEF;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Origin && ((Origin) other).id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public boolean satisfies(RelTrait target) {
      return target.equals(NONE) || equals(target);
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public String toString() {
      return "sqlOrigin=" + id;
    }
  }
}
