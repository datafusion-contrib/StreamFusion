package tech.streamfusion.planner;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.logical.*;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.OperatorIdentityProjection;
import tech.streamfusion.state.StockStateContract;

/** Lowers a private runtime graph while retaining a portable stock compiled plan. */
final class StockCompiledPlanLowering {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(StockCompiledPlanLowering.class);
  private final PlannerBase planner;
  private final Map<Integer, Integer> resolvedStockMaximums = new HashMap<>();
  private final boolean nativeKeyedState;
  private final Map<Integer, JsonNode> definitions = new HashMap<>();
  private final Map<Integer, String> stockUids = new HashMap<>();
  private final Map<Integer, ExecNode<?>> stockNodes = new HashMap<>();
  private final Map<Integer, Transformation<?>> stockTransformations = new HashMap<>();
  private final Map<ExecNode<?>, ExecNode<?>> lowered = new IdentityHashMap<>();
  private final Map<ExecNode<?>, StockBinding> bindings = new IdentityHashMap<>();
  private final Set<Integer> stockOwners = new HashSet<>();
  private final ObjectReader reader;
  private final Set<ExecNode<?>> columnar = Collections.newSetFromMap(new IdentityHashMap<>());

  StockCompiledPlanLowering(
      PlannerBase planner,
      StreamGraph resolvedStockGraph,
      org.apache.flink.runtime.jobgraph.JobGraph stockJobGraph) {
    this.planner = planner;
    Map<String, Integer> operatorMaximums = new HashMap<>();
    for (var vertex : stockJobGraph.getVertices())
      for (var identity : vertex.getOperatorIDs()) {
        operatorMaximums.put(
            identity.getGeneratedOperatorID().toString(), vertex.getMaxParallelism());
        identity
            .getUserDefinedOperatorID()
            .ifPresent(id -> operatorMaximums.put(id.toString(), vertex.getMaxParallelism()));
      }
    new StreamGraphHasherV2()
        .traverseStreamGraphAndGenerateHashes(resolvedStockGraph)
        .forEach(
            (id, hash) -> {
              Integer maximum = operatorMaximums.get(new OperatorID(hash).toString());
              if (maximum != null) resolvedStockMaximums.put(id, maximum);
            });
    nativeKeyedState =
        tech.streamfusion.compat.FlinkStateBackendCompat.prepareNativeStateBackend(
                planner.getExecEnv(), planner.getTableConfig().getConfiguration())
            == null;
    reader = CompiledPlanSerdeUtil.createJsonObjectReader(planner.createSerdeContext());
  }

  static boolean containsNativeNodes(ExecNodeGraph graph) {
    var visited = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
    return graph.getRootNodes().stream().anyMatch(node -> containsNative(node, visited));
  }

  private static boolean containsNative(ExecNode<?> node, Set<ExecNode<?>> visited) {
    if (!visited.add(node)) return false;
    return node.getClass().getName().startsWith("tech.streamfusion.")
        || node.getInputEdges().stream()
            .anyMatch(edge -> containsNative(edge.getSource(), visited));
  }

  ExecNodeGraph lower(ExecNodeGraph original) {
    try {
      String json =
          CompiledPlanSerdeUtil.createJsonObjectWriter(planner.createSerdeContext())
              .writeValueAsString(original);
      for (JsonNode node : reader.readTree(json).path("nodes")) {
        definitions.put(node.path("id").intValue(), node);
        if (node.path("type").asText().startsWith("stream-exec-calc")) {
          RexNode[] projections =
              reader.forType(RexNode[].class).readValue(node.path("projection"));
          if (Arrays.stream(projections).anyMatch(JsonStringIdentity::containsSensitiveString))
            return original;
        }
      }
      collectUids(original.getRootNodes());
      ExecNodeGraph copy = reader.forType(ExecNodeGraph.class).readValue(json);
      List<ExecNode<?>> roots = new ArrayList<>();
      for (ExecNode<?> root : copy.getRootNodes()) roots.add(lower(root));
      var runtime = new RuntimeGraph(copy.getFlinkVersion(), roots);
      insertShares(runtime);
      return runtime;
    } catch (IOException failure) {
      throw new TableException(
          "Cannot lower the stock compiled plan for native execution", failure);
    }
  }

  static final class RuntimeGraph extends ExecNodeGraph {
    RuntimeGraph(FlinkVersion version, List<ExecNode<?>> roots) {
      super(version, roots);
    }
  }

  private void insertShares(ExecNodeGraph graph) {
    var visited = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
    Map<ExecNode<?>, Integer> consumers = new IdentityHashMap<>();
    Consumer<ExecNode<?>> visit =
        new Consumer<>() {
          @Override
          public void accept(ExecNode<?> node) {
            if (!visited.add(node)) return;
            for (ExecEdge edge : node.getInputEdges()) {
              consumers.merge(edge.getSource(), 1, Integer::sum);
              accept(edge.getSource());
            }
          }
        };
    graph.getRootNodes().forEach(visit);
    Map<ExecNode<?>, ExecNode<?>> shares = new IdentityHashMap<>();
    consumers.forEach(
        (node, count) -> {
          if (count > 1 && columnar.contains(node)) {
            var share =
                new NativeShareExecNode(
                    planner.getTableConfig().getConfiguration(),
                    InputProperty.DEFAULT,
                    (RowType) node.getOutputType(),
                    "NativeShare(consumers=[" + count + "])",
                    count);
            connect(node, share);
            shares.put(node, share);
          }
        });
    for (ExecNode<?> node : visited) {
      List<ExecEdge> edges = new ArrayList<>();
      for (ExecEdge edge : node.getInputEdges()) {
        edges.add(
            new ExecEdge(
                shares.getOrDefault(edge.getSource(), edge.getSource()),
                node,
                edge.getShuffle(),
                edge.getExchangeMode()));
      }
      node.setInputEdges(edges);
    }
  }

  OperatorIdentityProjection identityProjection(
      StreamGraph stock, StreamGraph runtime, ExecNodeGraph runtimeGraph) {
    Map<Integer, Integer> anchors = new HashMap<>();
    var transposes = new HashSet<Integer>();
    var visited = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
    for (ExecNode<?> root : runtimeGraph.getRootNodes()) {
      collectIdentities(root, stock, runtime, anchors, transposes, visited);
    }
    var identities =
        CanonicalGraphIdentities.bind(
            stock, runtime, anchors, transposes, planner.getFlinkContext().getClassLoader());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(runtime);
    Map<String, OperatorIdentityProjection.EmptyHelper> emptyHelpers = new HashMap<>();
    for (int helper : transposes) {
      var node = runtime.getStreamNode(helper);
      emptyHelpers.put(
          org.apache.flink.util.StringUtils.byteToHexString(hashes.get(helper)),
          new OperatorIdentityProjection.EmptyHelper(
              node.getInEdges().stream()
                  .map(
                      edge ->
                          org.apache.flink.util.StringUtils.byteToHexString(
                              hashes.get(edge.getSourceId())))
                  .collect(java.util.stream.Collectors.toList()),
              node.getParallelism(),
              node.getMaxParallelism(),
              node.getInEdges().stream().allMatch(edge -> edge.getPartitioner().isPointwise())));
    }
    return new OperatorIdentityProjection(identities, emptyHelpers);
  }

  private void collectIdentities(
      ExecNode<?> node,
      StreamGraph stock,
      StreamGraph runtime,
      Map<Integer, Integer> anchors,
      Set<Integer> transposes,
      Set<ExecNode<?>> visited) {
    if (!visited.add(node)) return;
    Transformation<?> transformation = ((ExecNodeBase<?>) node).getTransformation();
    if (node instanceof NativeCanonicalSinkExecNode) {
      var sink = (NativeCanonicalSinkExecNode) node;
      anchors.put(sink.materializerId(), sink.originalMaterializerId());
      transposes.addAll(sink.helperIds());
    }
    if (node instanceof RowDataToArrowExecNode
        || node instanceof ArrowToRowDataExecNode
        || node instanceof NativeShareExecNode) {
      transposes.add(transformation.getId());
    } else if (node instanceof NativeColumnarExchangeExecNode) {
      // The partition is virtual; its producer is the synchronous batch splitter.
      if (transformation instanceof PartitionTransformation) {
        transposes.add(transformation.getInputs().get(0).getId());
      }
    } else {
      int originalId = bindings.containsKey(node) ? bindings.get(node).originalId : node.getId();
      Transformation<?> original = stockTransformations.get(originalId);
      if (original != null
          && stockOwners.contains(originalId)
          && stock.getStreamNode(original.getId()) != null
          && runtime.getStreamNode(transformation.getId()) != null) {
        anchors.put(transformation.getId(), original.getId());
      }
    }
    for (ExecEdge edge : node.getInputEdges()) {
      collectIdentities(edge.getSource(), stock, runtime, anchors, transposes, visited);
    }
  }

  private void collectUids(List<ExecNode<?>> nodes) {
    for (ExecNode<?> node : nodes) {
      if (stockUids.containsKey(node.getId())) continue;
      stockNodes.put(node.getId(), node);
      stockUids.put(node.getId(), ((ExecNodeBase<?>) node).getTransformation().getUid());
      stockTransformations.put(node.getId(), ((ExecNodeBase<?>) node).getTransformation());
      if (node.getInputEdges().stream()
          .noneMatch(
              edge ->
                  ((ExecNodeBase<?>) edge.getSource()).getTransformation()
                      == ((ExecNodeBase<?>) node).getTransformation()))
        stockOwners.add(node.getId());
      for (ExecEdge edge : node.getInputEdges()) collectUids(List.of(edge.getSource()));
    }
  }

  static boolean requiresStockRankTemplate(SqlPlanOrigins.RuntimeGraph graph) {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.getRootNodes(), nodes);
    return nodes.stream()
        .anyMatch(
            node ->
                node instanceof NativeColumnarTopNExecNode
                    && (((NativeColumnarTopNExecNode) node).hasUniqueRowKeyState()
                        || ((NativeColumnarTopNExecNode) node).hasRetractingState()));
  }

  static boolean canBindOrdinary(SqlPlanOrigins.RuntimeGraph graph) {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.getRootNodes(), nodes);
    for (ExecNode<?> node : nodes) {
      if (ordinaryHelper(node)) continue;
      if (!graph.originals.containsKey(node)) return false;
      if (node.getClass().getName().startsWith("tech.streamfusion.")
          && !(node instanceof NativeDeduplicateExecNode
              || node instanceof NativeChangelogNormalizeExecNode
              || node instanceof NativeFirstNExecNode
              || node instanceof NativeColumnarTopNExecNode
              || node instanceof NativeColumnarGroupAggExecNode
              || node instanceof NativeColumnarUpdatingJoinExecNode
              || node instanceof NativeTemporalJoinExecNode
              || node instanceof NativeIntervalJoinExecNode
              || node instanceof NativeWindowJoinExecNode
              || node instanceof NativeCalcExecNode
              || node instanceof NativeFilterExecNode
              || node instanceof NativeMiniBatchAssignerExecNode
              || node instanceof NativeWatermarkAssignerExecNode
              || node instanceof NativeWindowTableFunctionExecNode)) return false;
    }
    return true;
  }

  ExecNodeGraph bindOrdinary(SqlPlanOrigins.RuntimeGraph graph) {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.getRootNodes(), nodes);
    collectUids(graph.stock.getRootNodes());
    Map<ExecNode<?>, ExecNode<?>> replacements = ordinarySharedRoles(graph, nodes);
    if (replacements == null) return null;
    boolean sharedRoles = !replacements.isEmpty();
    Set<ExecNode<?>> assigned = Collections.newSetFromMap(new IdentityHashMap<>());
    for (ExecNode<?> node : nodes) {
      if (ordinaryHelper(node) || replacements.containsKey(node)) continue;
      ExecNode<?> original = graph.originals.get(node);
      if (resolvedStockMaximums.containsKey(stockTransformations.get(original.getId()).getId())
          && !assigned.add(original)) return null;
    }
    Set<ExecNode<?>> originals = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.stock.getRootNodes(), originals);
    for (ExecNode<?> original : originals)
      if (resolvedStockMaximums.containsKey(stockTransformations.get(original.getId()).getId())
          && !assigned.contains(original)) return null;
    Map<NativeColumnarUpdatingJoinExecNode, ExecNode<?>> regularOwners = new IdentityHashMap<>();
    try {
      var writer = CompiledPlanSerdeUtil.createJsonObjectWriter(planner.createSerdeContext());
      for (ExecNode<?> node : nodes) {
        if (ordinaryHelper(node) || replacements.containsKey(node)) continue;
        ExecNode<?> stock = graph.originals.get(node);
        if (node instanceof NativeDeduplicateExecNode
            || node instanceof NativeChangelogNormalizeExecNode
            || node instanceof NativeFirstNExecNode
            || node instanceof NativeColumnarTopNExecNode
            || node instanceof NativeColumnarGroupAggExecNode
            || node instanceof NativeColumnarUpdatingJoinExecNode
            || node instanceof NativeTemporalJoinExecNode
            || node instanceof NativeIntervalJoinExecNode
            || node instanceof NativeWindowJoinExecNode) {
          if (!nativeKeyedState) return null;
          if (!ordinaryKeyLayout(stockTransformations.get(stock.getId()))) return null;
          definitions.put(stock.getId(), reader.readTree(writer.writeValueAsString(stock)));
          ExecNode<?> exit;
          if (node instanceof NativeDeduplicateExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate)
            exit = lowerDedup(stock);
          else if (node instanceof NativeChangelogNormalizeExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream
                      .StreamExecChangelogNormalize) exit = lowerNormalize(stock);
          else if ((node instanceof NativeFirstNExecNode
                  || node instanceof NativeColumnarTopNExecNode)
              && stock
                  instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank)
            exit = lowerRank(stock);
          else if (node instanceof NativeColumnarGroupAggExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate)
            exit = lowerGroupAggregate(stock);
          else if (node instanceof NativeTemporalJoinExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin)
            exit = lowerTemporalJoin(stock);
          else if (node instanceof NativeIntervalJoinExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin)
            exit = lowerIntervalJoin(stock);
          else if (node instanceof NativeWindowJoinExecNode
              && stock
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin)
            exit = lowerWindowJoin(stock);
          else if (node instanceof NativeColumnarUpdatingJoinExecNode
              && stock
                  instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin)
            exit =
                lowerJoin(
                    (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin) stock);
          else return null;
          if (exit == stock) return null;
          ExecNode<?> owner = exit.getInputEdges().get(0).getSource();
          if (owner.getClass() != node.getClass())
            throw new IllegalStateException(
                "Ordinary SQL state binding changed its logical operator");
          for (int side = 0; side < node.getInputEdges().size(); side++) {
            ExecNode<?> source = node.getInputEdges().get(side).getSource();
            if (source instanceof NativeColumnarExchangeExecNode)
              source = source.getInputEdges().get(0).getSource();
            connect(source, owner.getInputEdges().get(side).getSource());
          }
          if (node instanceof NativeColumnarUpdatingJoinExecNode) {
            regularOwners.put((NativeColumnarUpdatingJoinExecNode) node, owner);
            bindings.put(node, bindings.remove(owner));
          } else replacements.put(node, owner);
        } else {
          bindings.put(
              node,
              new StockBinding(
                  stock.getId(),
                  stockOwners.contains(stock.getId()) ? stockUids.get(stock.getId()) : null,
                  null));
        }
      }
      regularOwners.forEach(
          (node, owner) -> {
            node.withStateMaxParallelism(
                stockMaximum(stockTransformations.get(graph.originals.get(node).getId())));
            var inputs = new ArrayList<ExecEdge>();
            for (ExecEdge input : owner.getInputEdges())
              inputs.add(
                  new ExecEdge(
                      input.getSource(), node, input.getShuffle(), input.getExchangeMode()));
            node.setInputEdges(inputs);
          });
      if (sharedRoles)
        for (ExecNode<?> node : nodes)
          if (node instanceof NativeShareExecNode)
            replacements.put(node, node.getInputEdges().get(0).getSource());
      var rewritten = new IdentityHashMap<ExecNode<?>, ExecNode<?>>();
      var roots = new ArrayList<ExecNode<?>>();
      for (ExecNode<?> root : graph.getRootNodes())
        roots.add(rewriteOrdinary(root, replacements, rewritten));
      var result = new RuntimeGraph(graph.getFlinkVersion(), roots);
      if (sharedRoles) {
        Set<ExecNode<?>> reachable = Collections.newSetFromMap(new IdentityHashMap<>());
        collectOrdinaryNodes(roots, reachable);
        for (ExecNode<?> node : reachable)
          if (node.getClass().getName().startsWith("tech.streamfusion.")
              && !(node instanceof ArrowToRowDataExecNode)) columnar.add(node);
        insertShares(result);
      }
      return result;
    } catch (IOException failure) {
      throw new TableException("Cannot capture ordinary SQL stock state contracts", failure);
    }
  }

  private static Map<ExecNode<?>, ExecNode<?>> ordinarySharedRoles(
      SqlPlanOrigins.RuntimeGraph graph, Set<ExecNode<?>> nodes) {
    Map<ExecNode<?>, ExecNode<?>> owners = new IdentityHashMap<>();
    Map<ExecNode<?>, ExecNode<?>> copies = new IdentityHashMap<>();
    for (ExecNode<?> node : nodes) {
      if (!(node instanceof NativeFirstNExecNode
          || node instanceof NativeCalcExecNode
          || node instanceof NativeFilterExecNode
          || node instanceof NativeWatermarkAssignerExecNode)) continue;
      ExecNode<?> original = graph.originals.get(node);
      ExecNode<?> owner = owners.putIfAbsent(original, node);
      if (owner == null) continue;
      if (node.getClass() != owner.getClass()
          || node instanceof NativeFirstNExecNode
              && !sameFirstNDefinition((NativeFirstNExecNode) node, (NativeFirstNExecNode) owner)
          || !node.getDescription().equals(owner.getDescription())
          || !node.getOutputType().equals(owner.getOutputType())
          || !node.getInputEdges()
              .get(0)
              .getSource()
              .getOutputType()
              .equals(owner.getInputEdges().get(0).getSource().getOutputType())
          || ordinaryInputRole(graph, node) == null
          || ordinaryInputRole(graph, node) != ordinaryInputRole(graph, owner)) return null;
      copies.put(node, owner);
    }
    return copies;
  }

  private static boolean sameFirstNDefinition(
      NativeFirstNExecNode node, NativeFirstNExecNode owner) {
    return node.sameStateDefinition(owner)
        && node.getPersistedConfig().toMap().equals(owner.getPersistedConfig().toMap());
  }

  private static ExecNode<?> ordinaryInputRole(
      SqlPlanOrigins.RuntimeGraph graph, ExecNode<?> node) {
    ExecNode<?> source = node.getInputEdges().get(0).getSource();
    while (source instanceof RowDataToArrowExecNode
        || source instanceof NativeShareExecNode
        || source instanceof NativeColumnarExchangeExecNode)
      source = source.getInputEdges().get(0).getSource();
    return graph.originals.get(source);
  }

  private static boolean ordinaryHelper(ExecNode<?> node) {
    return node instanceof RowDataToArrowExecNode
        || node instanceof ArrowToRowDataExecNode
        || node instanceof NativeColumnarExchangeExecNode
        || node instanceof NativeShareExecNode;
  }

  private static boolean ordinaryKeyLayout(Transformation<?> original) {
    var keys =
        original instanceof OneInputTransformation
            ? ((OneInputTransformation<?, ?>) original).getStateKeyType()
            : original
                    instanceof org.apache.flink.streaming.api.transformations.TwoInputTransformation
                ? ((org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
                        original)
                    .getStateKeyType()
                : null;
    if (!(keys instanceof org.apache.flink.table.runtime.typeutils.InternalTypeInfo)) return false;
    for (LogicalType field :
        ((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<?>) keys).toRowFieldTypes())
      if (field.is(LogicalTypeRoot.DECIMAL) && LogicalTypeChecks.getPrecision(field) > 18
          || (field.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || field.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
              && LogicalTypeChecks.getPrecision(field) > 3) return false;
    return true;
  }

  private static void collectOrdinaryNodes(List<ExecNode<?>> roots, Set<ExecNode<?>> visited) {
    for (ExecNode<?> node : roots) {
      if (!visited.add(node)) continue;
      for (ExecEdge edge : node.getInputEdges())
        collectOrdinaryNodes(List.of(edge.getSource()), visited);
    }
  }

  private static ExecNode<?> rewriteOrdinary(
      ExecNode<?> node,
      Map<ExecNode<?>, ExecNode<?>> replacements,
      Map<ExecNode<?>, ExecNode<?>> rewritten) {
    if (rewritten.containsKey(node)) return rewritten.get(node);
    ExecNode<?> result = node;
    while (replacements.containsKey(result)) result = replacements.get(result);
    if (rewritten.containsKey(result)) {
      rewritten.put(node, rewritten.get(result));
      return rewritten.get(result);
    }
    var edges = new ArrayList<ExecEdge>();
    for (ExecEdge edge : result.getInputEdges())
      edges.add(
          new ExecEdge(
              rewriteOrdinary(edge.getSource(), replacements, rewritten),
              result,
              edge.getShuffle(),
              edge.getExchangeMode()));
    result.setInputEdges(edges);
    rewritten.put(node, result);
    return result;
  }

  private ExecNode<?> lower(ExecNode<?> node) throws IOException {
    if (lowered.containsKey(node)) return lowered.get(node);
    List<ExecEdge> edges = new ArrayList<>();
    for (ExecEdge edge : node.getInputEdges())
      edges.add(
          new ExecEdge(lower(edge.getSource()), node, edge.getShuffle(), edge.getExchangeMode()));
    node.setInputEdges(edges);
    ExecNode<?> result = node;
    if (node instanceof StreamExecTableSourceScan)
      result = lowerSource((StreamExecTableSourceScan) node);
    else if (node instanceof StreamExecCalc && NativeConfig.operatorEnabled("calc"))
      result = lowerCalc((StreamExecCalc) node);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin
        && nativeKeyedState
        && NativeConfig.operatorEnabled("temporalJoin")) result = lowerTemporalJoin(node);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin
        && nativeKeyedState
        && NativeConfig.operatorEnabled("intervalJoin")) result = lowerIntervalJoin(node);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin
        && nativeKeyedState
        && NativeConfig.operatorEnabled("windowJoin")) result = lowerWindowJoin(node);
    else if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin
        && nativeKeyedState
        && NativeConfig.operatorEnabled("updatingJoin"))
      result =
          lowerJoin((org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin) node);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("deduplicate")) result = lowerDedup(node);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize
        && nativeKeyedState
        && NativeConfig.operatorEnabled("changelogNormalize")) result = lowerNormalize(node);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("groupAggregate")) result = lowerGroupAggregate(node);
    else if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank
        && nativeKeyedState
        && NativeConfig.operatorEnabled("topN")) result = lowerRank(node);
    lowered.put(node, result);
    return result;
  }

  private ExecNode<?> lowerGroupAggregate(ExecNode<?> stock) throws IOException {
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (!(operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator))
      return stock;
    var function =
        ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
            .getUserFunction();
    if (function.getClass()
        != org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction.class) return stock;
    JsonNode definition = definitions.get(stock.getId());
    int[] keys = reader.forType(int[].class).readValue(definition.path("grouping"));
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    RowType outputType = (RowType) stock.getOutputType();
    if (keys.length > 1) return stock;
    boolean global = keys.length == 0;
    var keyRoot = global ? null : inputType.getTypeAt(keys[0]).getTypeRoot();
    if (!global && !(keyRoot == LogicalTypeRoot.BIGINT || keyRoot == LogicalTypeRoot.INTEGER)
        || java.util.stream.IntStream.range(0, inputType.getFieldCount())
            .anyMatch(
                i -> (global || i != keys[0]) && !inputType.getTypeAt(i).is(LogicalTypeRoot.BIGINT))
        || !global && outputType.getTypeAt(0).getTypeRoot() != keyRoot
        || outputType.getChildren().stream()
            .skip(keys.length)
            .anyMatch(type -> !type.is(LogicalTypeRoot.BIGINT))
        || !(original.getStateKeySelector()
            instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    boolean retract = definition.path("needRetraction").asBoolean();
    var calls =
        reader
            .forType(org.apache.calcite.rel.core.AggregateCall[].class)
            .<org.apache.calcite.rel.core.AggregateCall[]>readValue(definition.path("aggCalls"));
    boolean[] needsRetract =
        reader.forType(boolean[].class).readValue(definition.path("aggCallNeedRetractions"));
    var infos =
        org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToStreamAggregateInfoList(
            planner.getTypeFactory(),
            inputType,
            org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(
                Arrays.asList(calls)),
            needsRetract,
            retract,
            true,
            true);
    if (infos.distinctInfos().length != 0) return stock;
    LogicalType[] accumulatorTypes = (LogicalType[]) releasedField(function, "accTypes");
    LogicalType[] generatedTypes =
        Arrays.stream(infos.getAccTypes())
            .map(org.apache.flink.table.types.DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    int recordCount = infos.getIndexOfCountStar();
    var counter = releasedField(function, "recordCounter");
    if (!Arrays.equals(accumulatorTypes, generatedTypes)
        || retract && recordCount < 0
        || counter.getClass()
            != org.apache.flink.table.runtime.operators.aggregate.RecordCounter.of(recordCount)
                .getClass()
        || recordCount >= 0 && recordCount != (Integer) releasedField(counter, "indexOfCountStar")
        || Arrays.stream(accumulatorTypes).anyMatch(type -> !type.is(LogicalTypeRoot.BIGINT)))
      return stock;
    int size = infos.getActualAggregateInfos().length;
    if (size == 0) return stock;
    int[] kinds = new int[size],
        columns = new int[size],
        scalars = new int[size],
        counts = new int[size];
    Arrays.fill(counts, -1);
    int slot = 0, actual = 0;
    for (var info : infos.aggInfos()) {
      int width = info.externalAccTypes().length;
      if (infos.countStarInserted()
          && info.aggIndex() == ((Number) infos.indexOfCountStar().get()).intValue()) {
        slot += width;
        continue;
      }
      var call = info.agg();
      if (call.isDistinct()
          || call.isApproximate()
          || call.filterArg >= 0
          || !call.getCollation().getFieldCollations().isEmpty()
          || info.viewSpecs().length != 0
          || info.argIndexes().length > 1) return stock;
      Class<?> type = info.function().getClass();
      int kind, fields;
      boolean countEveryRow =
          type == org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction.class;
      if (countEveryRow
          || type == org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction.class) {
        kind = 3;
        fields = 1;
      } else if (type
          == org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction
              .LongSum0AggFunction.class) {
        kind = 22;
        fields = 1;
      } else if (!retract
          && type
              == org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction
                  .LongSumAggFunction.class) {
        kind = 0;
        fields = 1;
      } else if (retract
          && type
              == org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction
                  .LongSumWithRetractAggFunction.class) {
        kind = 0;
        fields = 2;
      } else if (type
          == org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction.LongAvgAggFunction
              .class) {
        kind = 4;
        fields = 2;
      } else return stock;
      if (width != fields
          || (kind != 3
              && (info.argIndexes().length != 1
                  || !inputType.getTypeAt(info.argIndexes()[0]).is(LogicalTypeRoot.BIGINT))))
        return stock;
      kinds[actual] = kind;
      columns[actual] = countEveryRow || info.argIndexes().length == 0 ? -1 : info.argIndexes()[0];
      scalars[actual] = slot;
      if (fields == 2) counts[actual] = slot + 1;
      actual++;
      slot += width;
    }
    if (actual != size || slot != accumulatorTypes.length) return stock;
    var ttl =
        (org.apache.flink.api.common.state.StateTtlConfig) releasedField(function, "ttlConfig");
    long retention = ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0;
    config.setString("table.exec.state.ttl", retention + " ms");
    config.setString("table.exec.mini-batch.enabled", "false");
    var selector =
        (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
            original.getStateKeySelector();
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var contract =
        new tech.streamfusion.state.StockGroupArithmeticStateContract(
            serializer,
            (RowType) selector.getProducedType().toLogicalType(),
            RowType.of(accumulatorTypes),
            kinds,
            scalars,
            counts,
            recordCount,
            ttl.isEnabled());
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var group =
        new NativeColumnarGroupAggExecNode(
                config,
                InputProperty.DEFAULT,
                outputType,
                "NativeColumnarGroupAggregate",
                kinds,
                new int[size],
                columns,
                keys,
                new int[0],
                new int[0],
                new int[0],
                -1,
                (Boolean) releasedField(function, "generateUpdateBefore"),
                retention,
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
            .withStateMaxParallelism(maximum);
    connect(exchange, group);
    bindings.put(group, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(group);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(group, exit);
    return exit;
  }

  private ExecNode<?> lowerDedup(ExecNode<?> stock) throws IOException {
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    Object function;
    boolean miniBatch;
    if (operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator) {
      function =
          ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
              .getUserFunction();
      miniBatch = false;
    } else if (operator
        instanceof org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator) {
      function = releasedField(operator, "function");
      miniBatch = true;
    } else return stock;
    String name = function.getClass().getSimpleName();
    boolean rowtime;
    boolean keepLast;
    boolean compact = false;
    boolean buffered = false;
    switch (name) {
      case "ProcTimeDeduplicateKeepFirstRowFunction":
      case "ProcTimeMiniBatchDeduplicateKeepFirstRowFunction":
        rowtime = false;
        keepLast = false;
        break;
      case "ProcTimeDeduplicateKeepLastRowFunction":
      case "ProcTimeMiniBatchDeduplicateKeepLastRowFunction":
        if (!(Boolean) releasedField(function, miniBatch ? "inputInsertOnly" : "inputIsInsertOnly")
            || releasedField(function, "genFilterCondition") != null) return stock;
        rowtime = false;
        keepLast = true;
        break;
      case "RowTimeDeduplicateKeepFirstRowFunction":
        rowtime = true;
        keepLast = false;
        buffered = true;
        break;
      case "RowTimeMiniBatchLatestChangeDeduplicateFunction":
        compact = true;
      // The compact and full-chain bundles retain the same winning row in keyed state.
      case "RowTimeMiniBatchDeduplicateFunction":
      case "RowTimeDeduplicateFunction":
        rowtime = true;
        keepLast = (Boolean) releasedField(function, "keepLastRow");
        break;
      default:
        return stock;
    }
    boolean presenceOnly = !rowtime && !keepLast;
    boolean generateInsert =
        buffered || presenceOnly || (Boolean) releasedField(function, "generateInsert");
    boolean generateUpdateBefore =
        !buffered && !presenceOnly && (Boolean) releasedField(function, "generateUpdateBefore");
    boolean stateless = !rowtime && keepLast && !generateInsert && !generateUpdateBefore;
    long retention =
        (Long) releasedField(function, miniBatch ? "minRetentionTime" : "stateRetentionTime");
    int rowtimeColumn = rowtime ? (Integer) releasedField(function, "rowtimeIndex") : -1;
    var definition = definitions.get(stock.getId());
    int[] keys = reader.forType(int[].class).readValue(definition.path("uniqueKeys"));
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    if (!RowDataArrowConverter.supports(inputType)
        || !ArrowRowTypeSupport.supportsFields(inputType, keys)
        || Arrays.stream(keys).anyMatch(key -> containsRow(inputType.getTypeAt(key)))) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    config.setString(
        "table.exec.deduplicate.mini-batch.compact-changes-enabled", Boolean.toString(compact));
    config.setString(
        "table.exec.deduplicate.insert-update-after-sensitive-enabled",
        Boolean.toString(generateInsert));
    config.setString("table.exec.state.ttl", retention + " ms");
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType =
        RowType.of(Arrays.stream(keys).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new));
    StockStateContract contract =
        buffered
            ? new tech.streamfusion.state.StockBufferedDedupStateContract(
                serializer,
                (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
                    original.getStateKeySelector(),
                keyType,
                inputType,
                keys,
                rowtimeColumn,
                retention)
            : stateless
                ? new tech.streamfusion.state.StockEmptyDedupStateContract(
                    serializer, keyType, inputType, retention)
                : new tech.streamfusion.state.StockDedupStateContract(
                    serializer, keyType, inputType, presenceOnly, rowtimeColumn, retention);
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var dedup =
        new NativeDeduplicateExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeDeduplicate",
                keys,
                rowtimeColumn,
                keepLast,
                generateUpdateBefore,
                !rowtime,
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
            .withStateMaxParallelism(maximum);
    if (buffered) dedup.withStockBufferedState();
    else dedup.withStockEagerState();
    connect(exchange, dedup);
    bindings.put(dedup, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(dedup);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(dedup, exit);
    return exit;
  }

  private ExecNode<?> lowerNormalize(ExecNode<?> stock) throws IOException {
    var definition = definitions.get(stock.getId());
    if (!definition.path("filterCondition").isMissingNode()
        && !definition.path("filterCondition").isNull()) {
      recordFallback("changelog normalize: a pushed filter condition is not supported");
      return stock;
    }
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    Object function;
    boolean miniBatch;
    if (operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator) {
      function =
          ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
              .getUserFunction();
      if (!(function
          instanceof
          org.apache.flink.table.runtime.operators.deduplicate
              .ProcTimeDeduplicateKeepLastRowFunction)) return stock;
      miniBatch = false;
    } else if (operator
        instanceof org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator) {
      function = releasedField(operator, "function");
      if (!(function
          instanceof
          org.apache.flink.table.runtime.operators.deduplicate
              .ProcTimeMiniBatchDeduplicateKeepLastRowFunction)) return stock;
      miniBatch = true;
    } else return stock;
    if (!(Boolean) releasedField(function, "generateInsert")
        || (Boolean) releasedField(function, miniBatch ? "inputInsertOnly" : "inputIsInsertOnly")
        || releasedField(function, "genFilterCondition") != null) return stock;
    long retention =
        (Long) releasedField(function, miniBatch ? "minRetentionTime" : "stateRetentionTime");
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    int[] keys = reader.forType(int[].class).readValue(definition.path("uniqueKeys"));
    if (!RowDataArrowConverter.supports(inputType)
        || !ArrowRowTypeSupport.supportsFields(inputType, keys)
        || Arrays.stream(keys).anyMatch(key -> containsRow(inputType.getTypeAt(key)))) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    config.setString("table.exec.state.ttl", retention + " ms");
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType =
        RowType.of(Arrays.stream(keys).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockChangelogNormalizeStateContract(
            serializer, keyType, inputType, retention > 0);
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var normalize =
        new NativeChangelogNormalizeExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeChangelogNormalize",
                keys,
                (Boolean) releasedField(function, "generateUpdateBefore"),
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
            .withStateMaxParallelism(maximum);
    connect(exchange, normalize);
    bindings.put(
        normalize, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(normalize);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(normalize, exit);
    return exit;
  }

  private static boolean containsRow(LogicalType type) {
    return type.is(LogicalTypeRoot.ROW)
        || type.getChildren().stream().anyMatch(StockCompiledPlanLowering::containsRow);
  }

  private static Object releasedField(Object object, String name) throws IOException {
    for (Class<?> type = object.getClass(); type != null; type = type.getSuperclass()) {
      try {
        var field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException absent) {
        continue;
      } catch (IllegalAccessException failure) {
        throw new IOException("Cannot capture released state field " + name, failure);
      }
    }
    throw new IOException("Missing released state field " + name);
  }

  private ExecNode<?> lowerRank(ExecNode<?> stock) throws IOException {
    Transformation<?> transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    Object operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (!(operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator))
      return stock;
    Object function =
        ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
            .getUserFunction();
    if (!(function
        instanceof org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction))
      return lowerValueRank(stock, original, function);
    JsonNode definition = definitions.get(stock.getId());
    var range =
        reader
            .forType(org.apache.flink.table.runtime.operators.rank.RankRange.class)
            .<org.apache.flink.table.runtime.operators.rank.RankRange>readValue(
                definition.path("rankRange"));
    if (!(range instanceof org.apache.flink.table.runtime.operators.rank.ConstantRankRange))
      return stock;
    var constant = (org.apache.flink.table.runtime.operators.rank.ConstantRankRange) range;
    if (constant.getRankStart() != 1
        || constant.getRankEnd() < 1
        || constant.getRankEnd() > Integer.MAX_VALUE) return stock;
    var partition =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec>readValue(
                definition.path("partition"));
    int[] keys = partition.getFieldIndices();
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    if (!RowDataArrowConverter.supports(inputType)
        || !RowDataArrowConverter.supports((RowType) stock.getOutputType())
        || !ArrowRowTypeSupport.supportsFields(inputType, keys)) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    org.apache.flink.api.common.state.StateTtlConfig ttl;
    try {
      var field =
          org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction.class.getDeclaredField(
              "ttlConfig");
      field.setAccessible(true);
      ttl = (org.apache.flink.api.common.state.StateTtlConfig) field.get(function);
    } catch (ReflectiveOperationException failure) {
      throw new IOException("Cannot capture released first-N TTL", failure);
    }
    config.setString(
        "table.exec.state.ttl", (ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0) + " ms");
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType =
        RowType.of(Arrays.stream(keys).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockFirstNStateContract(serializer, keyType, ttl.isEnabled());
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var first =
        new NativeFirstNExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeColumnarFirstN",
                keys,
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys),
                (int) constant.getRankEnd(),
                definition.path("outputRowNumber").asBoolean())
            .withStateMaxParallelism(maximum);
    connect(exchange, first);
    bindings.put(first, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(first);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(first, exit);
    return exit;
  }

  private ExecNode<?> lowerValueRank(
      ExecNode<?> stock, OneInputTransformation<?, ?> original, Object function)
      throws IOException {
    boolean top1 =
        function instanceof org.apache.flink.table.runtime.operators.rank.FastTop1Function;
    boolean updateFunction =
        function instanceof org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
    boolean retractFunction =
        function instanceof org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
    if (!top1
        && !updateFunction
        && !retractFunction
        && !(function
            instanceof org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction))
      return stock;
    JsonNode definition = definitions.get(stock.getId());
    var strategy =
        reader
            .forType(org.apache.flink.table.planner.plan.utils.RankProcessStrategy.class)
            .<org.apache.flink.table.planner.plan.utils.RankProcessStrategy>readValue(
                definition.path("rankStrategy"));
    boolean updating =
        strategy
            instanceof
            org.apache.flink.table.planner.plan.utils.RankProcessStrategy.UpdateFastStrategy;
    boolean retracting =
        strategy
            instanceof
            org.apache.flink.table.planner.plan.utils.RankProcessStrategy.RetractStrategy;
    if (retracting
        ? !retractFunction
        : updating
            ? !top1 && !updateFunction
            : updateFunction
                || retractFunction
                || !(strategy
                    instanceof
                    org.apache.flink.table.planner.plan.utils.RankProcessStrategy
                        .AppendFastStrategy)) return stock;
    int[] rowKeys =
        updating
            ? ((org.apache.flink.table.planner.plan.utils.RankProcessStrategy.UpdateFastStrategy)
                    strategy)
                .getPrimaryKeys()
                .clone()
            : null;
    var range =
        reader
            .forType(org.apache.flink.table.runtime.operators.rank.RankRange.class)
            .<org.apache.flink.table.runtime.operators.rank.RankRange>readValue(
                definition.path("rankRange"));
    int bound = -1;
    long offset = 0;
    long limit;
    if (range instanceof org.apache.flink.table.runtime.operators.rank.ConstantRankRange) {
      var constant = (org.apache.flink.table.runtime.operators.rank.ConstantRankRange) range;
      if (constant.getRankStart() < 1
          || constant.getRankEnd() < constant.getRankStart()
          || (!retracting
              && (constant.getRankStart() != 1 || constant.getRankEnd() > Integer.MAX_VALUE)))
        return stock;
      offset = constant.getRankStart() - 1;
      limit = constant.getRankEnd();
    } else if (!updating
        && range instanceof org.apache.flink.table.runtime.operators.rank.VariableRankRange) {
      bound =
          ((org.apache.flink.table.runtime.operators.rank.VariableRankRange) range)
              .getRankEndIndex();
      limit = 100;
    } else return stock;
    var partition =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec>readValue(
                definition.path("partition"));
    var sort =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec>readValue(
                definition.path("orderBy"));
    int[] keys = partition.getFieldIndices();
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    if (((updating || retracting)
            ? sort.getFieldSize() < 1 || sort.getFieldSize() > 2
            : sort.getFieldSize() != 1)
        || inputType.getChildren().stream()
            .anyMatch(
                type ->
                    !java.util.Set.of(
                            LogicalTypeRoot.BIGINT,
                            LogicalTypeRoot.INTEGER,
                            LogicalTypeRoot.VARCHAR)
                        .contains(type.getTypeRoot()))) return stock;
    if (retracting
        && bound >= 0
        && (bound >= inputType.getFieldCount()
            || inputType.getTypeAt(bound).isNullable()
            || !(inputType.getTypeAt(bound).is(LogicalTypeRoot.INTEGER)
                || inputType.getTypeAt(bound).is(LogicalTypeRoot.BIGINT)))) return stock;
    if (!RowDataArrowConverter.supports(inputType)
        || !RowDataArrowConverter.supports((RowType) stock.getOutputType())
        || !ArrowRowTypeSupport.supportsFields(inputType, keys)) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    var ttl =
        (org.apache.flink.api.common.state.StateTtlConfig) releasedField(function, "ttlConfig");
    if (updating && ttl.isEnabled()) return stock;
    config.setString(
        "table.exec.state.ttl", (ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0) + " ms");
    if (!(original.getStateKeySelector()
        instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    var selector =
        (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
            original.getStateKeySelector();
    var sortSelector =
        (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
            releasedField(function, "sortKeySelector");
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType = (RowType) selector.getProducedType().toLogicalType();
    tech.streamfusion.state.StockStateContract contract;
    if (retracting) {
      contract =
          new tech.streamfusion.state.StockRetractingTopNStateContract(
              serializer,
              selector,
              sortSelector,
              keyType,
              inputType,
              (org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator)
                  releasedField(function, "serializableComparator"),
              bound >= 0,
              ttl.isEnabled());
    } else if (updating) {
      if (!ArrowRowTypeSupport.supportsFields(inputType, rowKeys)) return stock;
      var rowKeySelector =
          top1
              ? org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
                  planner.getClass().getClassLoader(),
                  rowKeys,
                  org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(inputType))
              : (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
                  releasedField(function, "rowKeySelector");
      contract =
          new tech.streamfusion.state.StockUpdatingTopNStateContract(
              serializer, selector, rowKeySelector, sortSelector, keyType, inputType, top1);
    } else {
      contract =
          new tech.streamfusion.state.StockAppendTopNStateContract(
              serializer,
              selector,
              sortSelector,
              keyType,
              inputType,
              sort.getFieldIndices(),
              top1,
              bound >= 0,
              ttl.isEnabled());
    }
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var rank =
        new NativeColumnarTopNExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeColumnarTopN",
                keys,
                sort.getFieldIndices(),
                Arrays.stream(sort.getFieldSpecs())
                    .mapToInt(field -> field.getIsAscendingOrder() ? 1 : 0)
                    .toArray(),
                Arrays.stream(sort.getFieldSpecs())
                    .mapToInt(field -> field.getNullIsLast() ? 0 : 1)
                    .toArray(),
                offset,
                limit,
                bound,
                (Boolean) releasedField(function, "outputRankNumber"),
                retracting,
                rowKeys,
                (Boolean) releasedField(function, "generateUpdateBefore"),
                rowKeys == null ? null : FlinkKeyGroupUtils.stockKeyDescriptor(inputType, rowKeys),
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys),
                bound >= 0)
            .withStateMaxParallelism(maximum);
    connect(exchange, rank);
    bindings.put(rank, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(rank);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(rank, exit);
    return exit;
  }

  private ExecNode<?> lowerJoin(
      org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin stock)
      throws IOException {
    JsonNode definition = definitions.get(stock.getId());
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec>readValue(
                definition.path("joinSpec"));
    int kind = RegularJoinMatcher.joinTypeCode(spec.getJoinType());
    int[] leftKeys = spec.getLeftKeys(), rightKeys = spec.getRightKeys();
    if (kind < 0 || leftKeys.length != rightKeys.length) return stock;
    var left = stock.getInputEdges().get(0).getSource();
    var right = stock.getInputEdges().get(1).getSource();
    RowType leftType = (RowType) left.getOutputType(), rightType = (RowType) right.getOutputType();
    RowType outputType = (RowType) stock.getOutputType();
    if (leftKeys.length == 0
        && (kind != 0 && kind != 5
            || leftType.getFieldCount() == 0
            || rightType.getFieldCount() == 0
            || !stockProducesOnlyInserts(
                stockNodes.get(stock.getId()).getInputEdges().get(0).getSource())
            || !stockProducesOnlyInserts(
                stockNodes.get(stock.getId()).getInputEdges().get(1).getSource()))) return stock;

    if (!RowDataArrowConverter.supports(leftType)
        || !RowDataArrowConverter.supports(rightType)
        || !RowDataArrowConverter.supports(outputType)
        || !ArrowRowTypeSupport.supportsFields(leftType, leftKeys)
        || !ArrowRowTypeSupport.supportsFields(rightType, rightKeys)
        || Stream.concat(leftType.getChildren().stream(), rightType.getChildren().stream())
            .anyMatch(type -> type.is(LogicalTypeFamily.CONSTRUCTED))) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    stock.getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    RexExpression predicate = null;
    if (spec.getNonEquiCondition().isPresent()) {
      RexNode condition =
          org.apache.calcite.rex.RexUtil.expandSearch(
              new RexBuilder(planner.getTypeFactory()), null, spec.getNonEquiCondition().get());
      predicate = RexExpression.encode(condition);
      if (predicate == null || predicate.requiresCollectionRepresentation()) return stock;
    }
    var original =
        (org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
            stockTransformations.get(stock.getId());
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    var leftSpec =
        (org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec)
            stockJoinField(operator, "leftInputSideSpec");
    var rightSpec =
        (org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec)
            stockJoinField(operator, "rightInputSideSpec");
    long leftTtl = (Long) stockJoinField(operator, "leftStateRetentionTime");
    long rightTtl = (Long) stockJoinField(operator, "rightStateRetentionTime");
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType =
        RowType.of(
            Arrays.stream(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockJoinStateContract(
            serializer,
            keyType,
            stockJoinSide(leftType, leftSpec, kind == 1 || kind == 3 || kind >= 4, leftTtl),
            stockJoinSide(rightType, rightSpec, kind == 2 || kind == 3, rightTtl));
    int maximum = stockMaximum(original);
    var leftExchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(left, config, leftType, leftKeys, true);
    var rightExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(right, config, rightType, rightKeys, true);
    leftExchange.withStateMaxParallelism(maximum);
    rightExchange.withStateMaxParallelism(maximum);
    var join =
        new NativeColumnarUpdatingJoinExecNode(
                config,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                outputType,
                "NativeColumnarUpdatingJoin",
                leftKeys,
                rightKeys,
                java.util.stream.IntStream.range(0, spec.getFilterNulls().length)
                    .map(i -> spec.getFilterNulls()[i] ? 1 : 0)
                    .toArray(),
                kind,
                leftType,
                rightType,
                predicate,
                FlinkKeyGroupUtils.stockKeyDescriptor(leftType, leftKeys),
                leftSpec.joinKeyContainsUniqueKey(),
                rightSpec.joinKeyContainsUniqueKey(),
                leftKeys.length == 0,
                leftKeys.length == 0,
                leftTtl,
                rightTtl)
            .withStateMaxParallelism(maximum);
    join.setInputEdges(
        List.of(
            ExecEdge.builder().source(leftExchange).target(join).build(),
            ExecEdge.builder().source(rightExchange).target(join).build()));
    bindings.put(join, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(join);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(join, exit);
    return exit;
  }

  private boolean stockProducesOnlyInserts(ExecNode<?> node) {
    if (node instanceof StreamExecTableSourceScan)
      return ((StreamExecTableSourceScan) node)
          .getTableSourceSpec()
          .getScanTableSource(planner.getFlinkContext(), planner.getTypeFactory())
          .getChangelogMode()
          .containsOnly(org.apache.flink.types.RowKind.INSERT);
    if (!(node instanceof StreamExecCalc
        || node instanceof StreamExecExchange
        || node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner
        || node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner
        || node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction))
      return false;
    return node.getInputEdges().size() == 1
        && stockProducesOnlyInserts(node.getInputEdges().get(0).getSource());
  }

  private int stockMaximum(Transformation<?> original) {
    int maximum =
        resolvedStockMaximums.getOrDefault(original.getId(), original.getMaxParallelism());
    if (maximum < 1) {
      int parallelism = original.getParallelism();
      if (parallelism < 1) parallelism = planner.getExecEnv().getParallelism();
      if (parallelism < 1)
        parallelism =
            planner
                .getTableConfig()
                .getConfiguration()
                .get(org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM);
      maximum = FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), Math.max(1, parallelism));
    }
    return maximum;
  }

  private static Object stockJoinField(Object operator, String name) throws IOException {
    try {
      var field =
          org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator.class
              .getDeclaredField(name);
      field.setAccessible(true);
      return field.get(operator);
    } catch (ReflectiveOperationException failure) {
      throw new IOException("Cannot bind released join state " + name, failure);
    }
  }

  private static tech.streamfusion.state.StockJoinStateContract.SideSpec stockJoinSide(
      RowType rowType,
      org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec spec,
      boolean outer,
      long ttl) {
    boolean otherUnique = spec.hasUniqueKey() && !spec.joinKeyContainsUniqueKey();
    return new tech.streamfusion.state.StockJoinStateContract.SideSpec(
        rowType,
        spec.joinKeyContainsUniqueKey(),
        otherUnique ? spec.getUniqueKeyType().toRowType() : null,
        otherUnique ? spec.getUniqueKeySelector() : null,
        outer,
        ttl);
  }

  private ExecNode<?> lowerSource(StreamExecTableSourceScan stock) throws IOException {
    var spec =
        reader
            .forType(DynamicTableSourceSpec.class)
            .<DynamicTableSourceSpec>readValue(
                definitions.get(stock.getId()).path("scanTableSource"));
    RowType outputType = (RowType) stock.getOutputType();
    var builder = new RexBuilder(planner.getTypeFactory());
    var cluster =
        FlinkRelOptClusterFactory.create(
            new VolcanoPlanner(RelOptCostImpl.FACTORY, planner.getFlinkContext()), builder);
    var abilities =
        spec.getSourceAbilities() == null
            ? new SourceAbilitySpec[0]
            : spec.getSourceAbilities().toArray(new SourceAbilitySpec[0]);
    var table =
        new TableSourceTable(
            null,
            planner.getTypeFactory().createFieldTypeFromLogicalType(outputType),
            FlinkStatistic.UNKNOWN(),
            spec.getScanTableSource(planner.getFlinkContext(), planner.getTypeFactory()),
            true,
            spec.getContextResolvedTable(),
            planner.getFlinkContext(),
            planner.getTypeFactory(),
            abilities);
    var scan =
        new StreamPhysicalTableSourceScan(cluster, cluster.traitSet(), List.of(), table, false);
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    stock.getPersistedConfig().toMap().forEach(config::setString);
    for (var extension :
        ServiceLoader.load(
            NativePlannerExtension.class, planner.getFlinkContext().getClassLoader())) {
      var source = extension.lowerCompiledSource(scan, config);
      if (source == null) continue;
      source.setInputEdges(List.of());
      bindings.put(source, new StockBinding(stock.getId(), stockUids.get(stock.getId()), null));
      columnar.add(source);
      var exit =
          new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
      connect(source, exit);
      return exit;
    }
    return stock;
  }

  private ExecNode<?> lowerCalc(StreamExecCalc stock) throws IOException {
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    RowType outputType = (RowType) stock.getOutputType();
    // Collection representation and time-indicator materialization need their physical-plan
    // context.
    if (Stream.concat(inputType.getChildren().stream(), outputType.getChildren().stream())
        .anyMatch(
            type ->
                type.is(LogicalTypeFamily.CONSTRUCTED) || LogicalTypeChecks.isTimeAttribute(type)))
      return stock;
    if (!RowDataArrowConverter.supports(inputType) || !RowDataArrowConverter.supports(outputType))
      return stock;
    JsonNode definition = definitions.get(stock.getId());
    var projections =
        reader.forType(RexNode[].class).<RexNode[]>readValue(definition.path("projection"));
    var condition =
        definition.path("condition").isMissingNode() || definition.path("condition").isNull()
            ? null
            : reader.forType(RexNode.class).<RexNode>readValue(definition.path("condition"));
    var builder = new RexBuilder(planner.getTypeFactory());
    var cluster =
        FlinkRelOptClusterFactory.create(
            new VolcanoPlanner(RelOptCostImpl.FACTORY, planner.getFlinkContext()), builder);
    var inputRelType = planner.getTypeFactory().createFieldTypeFromLogicalType(inputType);
    var outputRelType = planner.getTypeFactory().createFieldTypeFromLogicalType(outputType);
    var program =
        RexProgram.create(
            inputRelType, Arrays.asList(projections), condition, outputRelType, builder);
    var calc = LogicalCalc.create(LogicalValues.createEmpty(cluster, inputRelType), program);
    var encoded = RexExpression.encodeCalc(calc);
    if (encoded == null
        || encoded.requiresCollectionRepresentation()
        || CalcOutputTypeCheck.mismatch(encoded, inputRelType, outputType) != null) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    stock.getPersistedConfig().toMap().forEach(config::setString);
    var entry = arrowInput(input, config, inputType, true);
    var nativeCalc =
        new NativeCalcExecNode(
            config,
            InputProperty.DEFAULT,
            outputType,
            "NativeCalc",
            encoded.kinds(),
            encoded.payload(),
            encoded.childCounts(),
            encoded.longs(),
            encoded.doubles(),
            encoded.strings(),
            encoded.projectionRoots(),
            encoded.conditionRoot(),
            encoded.outputNames(),
            encoded.udfBinding());
    connect(entry, nativeCalc);
    bindings.put(nativeCalc, new StockBinding(stock.getId(), stockUids.get(stock.getId()), null));
    columnar.add(nativeCalc);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(nativeCalc, exit);
    return exit;
  }

  private ExecNode<?> lowerTemporalJoin(ExecNode<?> stock) throws IOException {
    if (planner.getExecEnv().getCheckpointConfig().isUnalignedCheckpointsEnabled()) {
      recordFallback("temporal join: canonical exchange reassembly is not yet bound");
      return stock;
    }
    var definition = definitions.get(stock.getId());
    int leftTime = definition.path("leftTimeAttributeIndex").asInt(-1);
    int rightTime = definition.path("rightTimeAttributeIndex").asInt(-1);
    if (leftTime < 0 || rightTime < 0) return stock;
    var joinSpec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec>readValue(
                definition.path("joinSpec"));
    int kind = TemporalJoinMatcher.joinTypeCode(joinSpec.getJoinType());
    int[] leftKeys = joinSpec.getLeftKeys(), rightKeys = joinSpec.getRightKeys();
    if (kind < 0 || leftKeys.length == 0 || leftKeys.length != rightKeys.length) return stock;
    for (boolean filter : joinSpec.getFilterNulls()) if (!filter) return stock;
    var left = stock.getInputEdges().get(0).getSource();
    var right = stock.getInputEdges().get(1).getSource();
    var leftType = (RowType) left.getOutputType();
    var rightType = (RowType) right.getOutputType();
    var outputType = (RowType) stock.getOutputType();
    if (leftTime >= leftType.getFieldCount()
        || rightTime >= rightType.getFieldCount()
        || !RowDataArrowConverter.supports(leftType)
        || !RowDataArrowConverter.supports(rightType)
        || !RowDataArrowConverter.supports(outputType)
        || !ArrowRowTypeSupport.supportsFields(leftType, leftKeys)
        || !ArrowRowTypeSupport.supportsFields(rightType, rightKeys)
        || Stream.concat(leftType.getChildren().stream(), rightType.getChildren().stream())
            .anyMatch(type -> type.is(LogicalTypeFamily.CONSTRUCTED))) return stock;
    for (LogicalType time : List.of(leftType.getTypeAt(leftTime), rightType.getTypeAt(rightTime)))
      if (!(time.is(LogicalTypeRoot.BIGINT)
          || (time.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || time.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
              && LogicalTypeChecks.getPrecision(time) <= 3)) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    RexExpression predicate = null;
    if (joinSpec.getNonEquiCondition().isPresent()) {
      predicate =
          RexExpression.encode(
              org.apache.calcite.rex.RexUtil.expandSearch(
                  new RexBuilder(planner.getTypeFactory()),
                  null,
                  joinSpec.getNonEquiCondition().get()));
      if (predicate == null || predicate.requiresCollectionRepresentation()) return stock;
    }
    var originalTransform = stockTransformations.get(stock.getId());
    if (!(originalTransform
        instanceof org.apache.flink.streaming.api.transformations.TwoInputTransformation))
      return stock;
    var original =
        (org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
            originalTransform;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (!(operator
        instanceof
        org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator))
      return stock;
    int maximum =
        resolvedStockMaximums.getOrDefault(original.getId(), original.getMaxParallelism());
    if (maximum < 1)
      maximum =
          FlinkKeyGroupUtils.maxParallelism(
              planner.getExecEnv(), original.getParallelism() > 0 ? original.getParallelism() : 1);
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> keys =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var keyType =
        RowType.of(
            Arrays.stream(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockTemporalJoinStateContract(
            keys,
            keyType,
            leftType,
            rightType,
            leftKeys,
            rightKeys,
            leftTime,
            rightTime,
            config
                .get(org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION)
                .toMillis());
    var leftExchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(left, config, leftType, leftKeys, true);
    var rightExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(right, config, rightType, rightKeys, true);
    leftExchange.withStateMaxParallelism(maximum);
    rightExchange.withStateMaxParallelism(maximum);
    var join =
        new NativeTemporalJoinExecNode(
                config,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                outputType,
                "NativeTemporalJoin",
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
                kind,
                leftType,
                rightType,
                predicate,
                FlinkKeyGroupUtils.stockKeyDescriptor(leftType, leftKeys))
            .withStateMaxParallelism(maximum);
    join.setInputEdges(
        List.of(
            ExecEdge.builder().source(leftExchange).target(join).build(),
            ExecEdge.builder().source(rightExchange).target(join).build()));
    bindings.put(join, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(join);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(join, exit);
    return exit;
  }

  private ExecNode<?> lowerIntervalJoin(ExecNode<?> stock) throws IOException {
    if (planner.getExecEnv().getCheckpointConfig().isUnalignedCheckpointsEnabled()) {
      recordFallback("interval join: canonical exchange reassembly is not yet bound");
      return stock;
    }
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec>readValue(
                definitions.get(stock.getId()).path("intervalJoinSpec"));
    var bounds = spec.getWindowBounds();
    if (bounds.getLeftUpperBound() - bounds.getLeftLowerBound() < 0) {
      LOG.debug("Stock interval bounds already eliminate the join");
      return stock;
    }
    var joinSpec = spec.getJoinSpec();
    int kind = IntervalJoinMatcher.joinTypeCode(joinSpec.getJoinType());
    int[] leftKeys = joinSpec.getLeftKeys(), rightKeys = joinSpec.getRightKeys();
    if (kind < 0 || leftKeys.length == 0 || leftKeys.length != rightKeys.length) return stock;
    for (boolean filter : joinSpec.getFilterNulls()) if (!filter) return stock;
    var left = stock.getInputEdges().get(0).getSource();
    var right = stock.getInputEdges().get(1).getSource();
    var leftType = (RowType) left.getOutputType();
    var rightType = (RowType) right.getOutputType();
    var outputType = (RowType) stock.getOutputType();
    if (!RowDataArrowConverter.supports(leftType)
        || !RowDataArrowConverter.supports(rightType)
        || !RowDataArrowConverter.supports(outputType)
        || !ArrowRowTypeSupport.supportsFields(leftType, leftKeys)
        || !ArrowRowTypeSupport.supportsFields(rightType, rightKeys)
        || Stream.concat(leftType.getChildren().stream(), rightType.getChildren().stream())
            .anyMatch(type -> type.is(LogicalTypeFamily.CONSTRUCTED))) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (config
                .get(
                    org.apache.flink.table.api.config.ExecutionConfigOptions
                        .TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL)
                .toMillis()
            > 0
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    RexExpression predicate = null;
    if (joinSpec.getNonEquiCondition().isPresent()) {
      predicate =
          RexExpression.encode(
              org.apache.calcite.rex.RexUtil.expandSearch(
                  new RexBuilder(planner.getTypeFactory()),
                  null,
                  joinSpec.getNonEquiCondition().get()));
      if (predicate == null || predicate.requiresCollectionRepresentation()) return stock;
    }
    var originalTransform = stockTransformations.get(stock.getId());
    if (!(originalTransform
        instanceof org.apache.flink.streaming.api.transformations.TwoInputTransformation))
      return stock;
    var original =
        (org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
            originalTransform;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (!(operator instanceof org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator))
      return stock;
    var function =
        ((org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator<?, ?, ?, ?>) operator)
            .getUserFunction();
    if (bounds.isEventTime()
        ? !(function
            instanceof org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin)
        : !(function
            instanceof org.apache.flink.table.runtime.operators.join.interval.ProcTimeIntervalJoin))
      return stock;
    int maximum =
        resolvedStockMaximums.getOrDefault(original.getId(), original.getMaxParallelism());
    if (maximum < 1)
      maximum =
          FlinkKeyGroupUtils.maxParallelism(
              planner.getExecEnv(), original.getParallelism() > 0 ? original.getParallelism() : 1);
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> keys =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var keyType =
        RowType.of(
            Arrays.stream(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockIntervalJoinStateContract(
            keys,
            keyType,
            leftType,
            rightType,
            leftKeys,
            rightKeys,
            bounds.getLeftTimeIdx(),
            bounds.getRightTimeIdx(),
            !bounds.isEventTime());
    var leftExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(left, config, leftType, leftKeys, false);
    var rightExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(right, config, rightType, rightKeys, false);
    leftExchange.withStateMaxParallelism(maximum);
    rightExchange.withStateMaxParallelism(maximum);
    var join =
        new NativeIntervalJoinExecNode(
                config,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                outputType,
                "NativeIntervalJoin",
                leftKeys,
                rightKeys,
                bounds.getLeftTimeIdx(),
                bounds.getRightTimeIdx(),
                bounds.getLeftLowerBound(),
                bounds.getLeftUpperBound(),
                kind,
                leftType,
                rightType,
                predicate,
                !bounds.isEventTime(),
                FlinkKeyGroupUtils.stockKeyDescriptor(leftType, leftKeys))
            .withStateMaxParallelism(maximum);
    join.setInputEdges(
        List.of(
            ExecEdge.builder().source(leftExchange).target(join).build(),
            ExecEdge.builder().source(rightExchange).target(join).build()));
    bindings.put(join, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(join);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(join, exit);
    return exit;
  }

  private ExecNode<?> lowerWindowJoin(ExecNode<?> stock) throws IOException {
    if (planner.getExecEnv().getCheckpointConfig().isUnalignedCheckpointsEnabled()) {
      recordFallback("window join: canonical exchange reassembly is not yet bound");
      return stock;
    }
    var definition = definitions.get(stock.getId());
    var joinSpec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec>readValue(
                definition.path("joinSpec"));
    var leftWindow =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(definition.path("leftWindowing"));
    var rightWindow =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(definition.path("rightWindowing"));
    if (!(leftWindow
            instanceof org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy)
        || !(rightWindow
            instanceof org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy)
        || !leftWindow.isRowtime()
        || !rightWindow.isRowtime()
        || !leftWindow.getWindow().equals(rightWindow.getWindow())
        || WindowAggregateMatcher.isLtz(leftWindow) != WindowAggregateMatcher.isLtz(rightWindow))
      return stock;
    var leftAttached =
        (org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy) leftWindow;
    var rightAttached =
        (org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy) rightWindow;
    var left = stock.getInputEdges().get(0).getSource();
    var right = stock.getInputEdges().get(1).getSource();
    var leftType = (RowType) left.getOutputType();
    var rightType = (RowType) right.getOutputType();
    var outputType = (RowType) stock.getOutputType();
    int kind = IntervalJoinMatcher.joinTypeCode(joinSpec.getJoinType());
    int[] leftKeys = joinSpec.getLeftKeys(), rightKeys = joinSpec.getRightKeys();
    if (kind < 0) {
      recordFallback("window join: semi/anti retain the released stock state owner");
      return stock;
    }
    if (leftKeys.length != rightKeys.length
        || !RowDataArrowConverter.supports(leftType)
        || !RowDataArrowConverter.supports(rightType)
        || !RowDataArrowConverter.supports(outputType)
        || Stream.concat(leftType.getChildren().stream(), rightType.getChildren().stream())
            .anyMatch(type -> type.is(LogicalTypeFamily.CONSTRUCTED))
        || !ArrowRowTypeSupport.supportsFields(leftType, leftKeys)
        || !ArrowRowTypeSupport.supportsFields(rightType, rightKeys)) return stock;
    for (boolean filter : joinSpec.getFilterNulls()) if (!filter) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    RexExpression predicate = null;
    if (joinSpec.getNonEquiCondition().isPresent()) {
      predicate =
          RexExpression.encode(
              org.apache.calcite.rex.RexUtil.expandSearch(
                  new RexBuilder(planner.getTypeFactory()),
                  null,
                  joinSpec.getNonEquiCondition().get()));
      if (predicate == null || predicate.requiresCollectionRepresentation()) return stock;
    }
    var original =
        (org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
            stockTransformations.get(stock.getId());
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)
        || !(((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator()
            instanceof org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator))
      return stock;
    int maximum =
        resolvedStockMaximums.getOrDefault(original.getId(), original.getMaxParallelism());
    if (maximum < 1)
      maximum =
          FlinkKeyGroupUtils.maxParallelism(
              planner.getExecEnv(), original.getParallelism() > 0 ? original.getParallelism() : 1);
    var spec = leftWindow.getWindow();
    long size = WindowAggregateMatcher.windowSize(leftWindow),
        slide = WindowAggregateMatcher.windowSlide(leftWindow);
    Duration offset;
    boolean cumulative = spec instanceof CumulativeWindowSpec;
    if (spec instanceof TumblingWindowSpec) offset = ((TumblingWindowSpec) spec).getOffset();
    else if (spec instanceof HoppingWindowSpec) offset = ((HoppingWindowSpec) spec).getOffset();
    else if (cumulative) offset = ((CumulativeWindowSpec) spec).getOffset();
    else return stock;
    long offsetMillis = offset == null ? 0 : offset.toMillis();
    String zone =
        WindowAggregateMatcher.isLtz(leftWindow)
            ? org.apache.flink.table.planner.utils.TableConfigUtils.getLocalTimeZone(config).getId()
            : "UTC";
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> keys =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var keyType =
        RowType.of(
            Arrays.stream(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new));
    var contract =
        new tech.streamfusion.state.StockWindowJoinStateContract(
            keys,
            keyType,
            leftType,
            rightType,
            leftKeys,
            rightKeys,
            leftAttached.getWindowStart(),
            leftAttached.getWindowEnd(),
            rightAttached.getWindowStart(),
            rightAttached.getWindowEnd(),
            zone,
            cumulative
                ? tech.streamfusion.state.StockWindowJoinStateContract.WindowSpec.cumulative(
                    size, offsetMillis)
                : tech.streamfusion.state.StockWindowJoinStateContract.WindowSpec.fixed(size));
    var leftExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(left, config, leftType, leftKeys, false);
    var rightExchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(right, config, rightType, rightKeys, false);
    leftExchange.withStateMaxParallelism(maximum);
    rightExchange.withStateMaxParallelism(maximum);
    var join =
        new NativeWindowJoinExecNode(
                config,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                outputType,
                "NativeWindowJoin",
                leftKeys,
                rightKeys,
                leftAttached.getWindowStart(),
                leftAttached.getWindowEnd(),
                rightAttached.getWindowStart(),
                rightAttached.getWindowEnd(),
                kind,
                leftType,
                rightType,
                predicate,
                false,
                size,
                slide,
                cumulative,
                java.time.ZoneId.of(zone).getRules().isFixedOffset()
                    ? java.time.ZoneId.of(zone)
                            .getRules()
                            .getOffset(java.time.Instant.EPOCH)
                            .getTotalSeconds()
                        * 1000L
                    : 0,
                FlinkKeyGroupUtils.stockKeyDescriptor(leftType, leftKeys))
            .withCalendarZone(zone)
            .withStateMaxParallelism(maximum);
    join.setInputEdges(
        List.of(
            ExecEdge.builder().source(leftExchange).target(join).build(),
            ExecEdge.builder().source(rightExchange).target(join).build()));
    bindings.put(join, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(join);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(join, exit);
    return exit;
  }

  private ExecNode<?> canonicalJoinInput(
      ExecNode<?> input, Configuration config, RowType type, int[] keys, boolean changelog) {
    var source =
        input instanceof StreamExecExchange ? input.getInputEdges().get(0).getSource() : input;
    var exchange =
        new NativeColumnarExchangeExecNode(
            config,
            InputProperty.DEFAULT,
            type,
            "NativeColumnarExchange",
            keys,
            FlinkKeyGroupUtils.stockKeyDescriptor(type, keys),
            keys.length == 0);
    connect(arrowInput(source, config, type, changelog), exchange);
    return exchange;
  }

  private void recordFallback(String reason) {
    LOG.debug("Stock compiled plan fallback: {}", reason);
  }

  private static void connect(ExecNode<?> source, ExecNode<?> target) {
    target.setInputEdges(List.of(ExecEdge.builder().source(source).target(target).build()));
  }

  private static ExecNode<?> arrowInput(
      ExecNode<?> rows, Configuration config, RowType inputType, boolean changelog) {
    if (rows instanceof ArrowToRowDataExecNode) return rows.getInputEdges().get(0).getSource();
    var entry =
        new RowDataToArrowExecNode(
            config, InputProperty.DEFAULT, inputType, "RowDataToArrow", changelog, null);
    connect(rows, entry);
    return entry;
  }

  void configureOperators() {
    bindings.forEach(
        (node, binding) -> {
          Transformation<?> transformation = ((ExecNodeBase<?>) node).getTransformation();
          if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink)
            transformation.setName(stockTransformations.get(binding.originalId).getName());
          if (binding.uid != null) transformation.setUid(binding.uid);
          if (binding.contract != null)
            nativeOperator(node).setStockStateContract(binding.contract);
        });
  }

  void configureStateBackend(StreamGraph runtime) {
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(runtime);
    bindings.forEach(
        (node, binding) -> {
          if (binding.contract == null) return;
          Transformation<?> transformation = ((ExecNodeBase<?>) node).getTransformation();
          byte[] hash = hashes.get(transformation.getId());
          if (hash == null)
            throw new IllegalStateException("Native state owner has no physical primary identity");
          CanonicalStateBackend.install(
              planner.getExecEnv(),
              planner.getTableConfig().getConfiguration(),
              new OperatorID(hash).toString(),
              nativeOperator(node).getClass().getSimpleName(),
              binding.contract,
              planner.getFlinkContext().getClassLoader());
        });
  }

  private static AbstractNativeStatefulOperator<?> nativeOperator(ExecNode<?> node) {
    Transformation<?> transformation = ((ExecNodeBase<?>) node).getTransformation();
    var factory =
        transformation instanceof OneInputTransformation
            ? ((OneInputTransformation<?, ?>) transformation).getOperatorFactory()
            : ((org.apache.flink.streaming.api.transformations.TwoInputTransformation<?, ?, ?>)
                    transformation)
                .getOperatorFactory();
    return (AbstractNativeStatefulOperator<?>) ((SimpleOperatorFactory<?>) factory).getOperator();
  }

  private static final class StockBinding {
    final int originalId;
    final String uid;
    final StockStateContract contract;

    StockBinding(int originalId, String uid, StockStateContract contract) {
      this.originalId = originalId;
      this.uid = uid;
      this.contract = contract;
    }
  }
}
