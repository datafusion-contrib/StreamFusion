package tech.streamfusion.planner;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.OperatorIdentityProjection;
import tech.streamfusion.state.StockSlicingWindowStateContract;
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
  private final Map<Integer, Integer> consumerCounts = new HashMap<>();
  private final Map<Integer, Transformation<?>> stockTransformations = new HashMap<>();
  private final Map<ExecNode<?>, ExecNode<?>> lowered = new IdentityHashMap<>();
  private final Map<ExecNode<?>, StockBinding> bindings = new IdentityHashMap<>();
  private final Set<Integer> stockOwners = new HashSet<>();
  private final Set<Integer> stringComparisonInputs = new HashSet<>();
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
      retainStringComparisonInputs(copy.getRootNodes());
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

  private void retainStringComparisonInputs(List<ExecNode<?>> roots) throws IOException {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(roots, nodes);
    for (ExecNode<?> node : nodes) {
      boolean global =
          node
              instanceof
              org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
      if (!(node
              instanceof
              org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate)
          && !global) continue;
      JsonNode definition = definitions.get(node.getId());
      if (definition.path("needRetraction").asBoolean()) continue;
      var input = node.getInputEdges().get(0).getSource();
      if (global) {
        if (!(input instanceof StreamExecExchange)) continue;
        var local = input.getInputEdges().get(0).getSource();
        if (!(local
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate))
          continue;
        input = local.getInputEdges().get(0).getSource();
      }
      RowType type = (RowType) input.getOutputType();
      AggregateCall[] calls =
          reader.forType(AggregateCall[].class).readValue(definition.path("aggCalls"));
      for (AggregateCall call : calls) {
        SqlKind kind = call.getAggregation().getKind();
        if ((kind == SqlKind.MIN || kind == SqlKind.MAX) && call.getArgList().size() == 1) {
          LogicalTypeRoot root = type.getTypeAt(call.getArgList().get(0)).getTypeRoot();
          if (root == LogicalTypeRoot.CHAR || root == LogicalTypeRoot.VARCHAR)
            retainStringComparisonInput(input);
        }
      }
    }
  }

  private void retainStringComparisonInput(ExecNode<?> node) {
    if (!stringComparisonInputs.add(node.getId())) return;
    // Flink compares Java strings differently from binary strings. Keep the captured producer
    // until native expression pipelines preserve that representation, then transport its cache.
    for (ExecEdge edge : node.getInputEdges()) retainStringComparisonInput(edge.getSource());
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
    for (var node : runtime.getStreamNodes()) {
      if (node.getOperatorFactory() != null
          && CanonicalGraphIdentities.isExtensionStatelessOperator(
              node.getOperatorFactory()
                  .getStreamOperatorClass(planner.getFlinkContext().getClassLoader()),
              planner.getFlinkContext().getClassLoader())) transposes.add(node.getId());
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
      for (ExecEdge edge : node.getInputEdges())
        consumerCounts.merge(edge.getSource().getId(), 1, Integer::sum);
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

  static boolean requiresStockTemplate(SqlPlanOrigins.RuntimeGraph graph) {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.getRootNodes(), nodes);
    return nodes.stream()
        .anyMatch(
            node -> {
              if (node instanceof NativeTemporalSortExecNode
                  || node instanceof NativeJvmGroupExecNode
                  || node instanceof NativeColumnarWindowAggExecNode
                  || node instanceof NativeColumnarSessionWindowAggExecNode
                  || node instanceof NativeColumnarGlobalWindowAggExecNode) return true;
              if (needsStringTemplate(node, graph) || needsGlobalGroupTemplate(node, graph))
                return true;
              if (node instanceof NativeColumnarGroupAggExecNode
                  && ((NativeColumnarGroupAggExecNode) node).hasStockDistinctState()) return true;
              if (node
                  instanceof
                  org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate) {
                try {
                  var calls =
                      (org.apache.calcite.rel.core.AggregateCall[]) releasedField(node, "aggCalls");
                  for (var call : calls)
                    if (JvmGroupAggregateMatcher.userFunction(call.getAggregation()) != null)
                      return true;
                } catch (IOException failure) {
                  throw new TableException("Cannot inspect captured user aggregate calls", failure);
                }
              }
              if (node instanceof NativeWindowRankExecNode)
                return !((NativeWindowRankExecNode) node).isProcessingTime();
              if (node instanceof NativeColumnarTopNExecNode)
                return ((NativeColumnarTopNExecNode) node).hasUniqueRowKeyState()
                    || ((NativeColumnarTopNExecNode) node).hasRetractingState();
              return node instanceof NativeOverAggExecNode
                  && ((NativeOverAggExecNode) node).hasStockArithmeticState();
            });
  }

  private static boolean needsStringTemplate(ExecNode<?> node, SqlPlanOrigins.RuntimeGraph graph) {
    if (!(node instanceof NativeColumnarGroupAggExecNode)
        || !((NativeColumnarGroupAggExecNode) node).hasStringExtrema()) return false;
    ExecNode<?> original = graph.originals.get(node);
    if (!(original
        instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate))
      return false;
    try {
      return !(Boolean) releasedField(original, "needRetraction");
    } catch (IOException failure) {
      throw new TableException("Cannot inspect the captured string aggregate contract", failure);
    }
  }

  private static boolean needsGlobalGroupTemplate(
      ExecNode<?> node, SqlPlanOrigins.RuntimeGraph graph) {
    if (!(node instanceof NativeColumnarGroupAggExecNode)
        || !((NativeColumnarGroupAggExecNode) node).hasStockGroupPartials()
        || !(graph.originals.get(node)
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate))
      return false;
    try {
      return !(Boolean) releasedField(graph.originals.get(node), "needRetraction");
    } catch (IOException failure) {
      throw new TableException("Cannot inspect global aggregate origins", failure);
    }
  }

  static boolean canBindOrdinary(SqlPlanOrigins.RuntimeGraph graph, ClassLoader loader) {
    Set<ExecNode<?>> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collectOrdinaryNodes(graph.getRootNodes(), nodes);
    for (ExecNode<?> node : nodes) {
      if (needsStringTemplate(node, graph) || needsGlobalGroupTemplate(node, graph)) return false;
      if (node instanceof NativeTemporalSortExecNode || node instanceof NativeWindowRankExecNode)
        return false;
      if (node instanceof NativeOverAggExecNode
          && ((NativeOverAggExecNode) node).hasStockArithmeticState()) return false;
      if (ordinaryHelper(node)) continue;
      if (!graph.originals.containsKey(node)) return false;
      if (graph.originals.get(node) instanceof StreamExecTableSourceScan
          && node.getInputEdges().isEmpty()
          && ServiceLoader.load(NativePlannerExtension.class, loader).stream()
              .map(ServiceLoader.Provider::get)
              .anyMatch(extension -> extension.preservesStockSourceState(node))) continue;
      if (graph.originals.get(node)
              instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink
          && node.getInputEdges().size() == 1
          && ServiceLoader.load(NativePlannerExtension.class, loader).stream()
              .map(ServiceLoader.Provider::get)
              .anyMatch(extension -> extension.preservesStockSinkState(node))) continue;
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
    if (stringComparisonInputs.contains(node.getId())) {
      lowered.put(node, node);
      return node;
    }
    List<ExecEdge> edges = new ArrayList<>();
    for (ExecEdge edge : node.getInputEdges())
      edges.add(
          new ExecEdge(lower(edge.getSource()), node, edge.getShuffle(), edge.getExchangeMode()));
    node.setInputEdges(edges);
    ExecNode<?> result = node;
    if (node instanceof StreamExecTableSourceScan)
      result = lowerSource((StreamExecTableSourceScan) node);
    else if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink)
      result = lowerSink(node);
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
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank
        && nativeKeyedState
        && NativeConfig.operatorEnabled("windowRank")) result = lowerWindowRank(node, false);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("windowRank")) result = lowerWindowRank(node, true);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort
        && nativeKeyedState
        && NativeConfig.operatorEnabled("temporalSort")) result = lowerTemporalSort(node);
    else if (node instanceof StreamExecWindowAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("windowAggregate"))
      result = lowerWindow((StreamExecWindowAggregate) node);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("windowAggregate")) result = lowerGlobalWindow(node);
    else if (node
            instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("over")) result = lowerOver(node);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("groupAggregate")) result = lowerGroupAggregate(node);
    else if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate
        && nativeKeyedState
        && NativeConfig.operatorEnabled("groupAggregate")) result = lowerGlobalGroupAggregate(node);
    else if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank
        && nativeKeyedState
        && NativeConfig.operatorEnabled("topN")) result = lowerRank(node);
    lowered.put(node, result);
    return result;
  }

  private ExecNode<?> lowerGlobalWindow(ExecNode<?> stock) throws IOException {
    var exchange = stock.getInputEdges().get(0).getSource();
    if (!(exchange instanceof StreamExecExchange)
        || consumerCounts.getOrDefault(exchange.getId(), 0) != 1)
      return windowFallback(stock, "global window requires a dedicated local exchange");
    var local = exchange.getInputEdges().get(0).getSource();
    if (!(local
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate)
        || consumerCounts.getOrDefault(local.getId(), 0) != 1)
      return windowFallback(stock, "global window requires a dedicated local aggregate identity");
    var globalStrategy =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(definitions.get(stock.getId()).path("windowing"));
    var localStrategy =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(definitions.get(local.getId()).path("windowing"));
    boolean sliced =
        globalStrategy
                instanceof
                org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy
            && localStrategy instanceof TimeAttributeWindowingStrategy;
    boolean finalWindows =
        globalStrategy
                instanceof
                org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy
            && localStrategy
                instanceof
                org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
    if ((!sliced && !finalWindows) || !globalStrategy.getWindow().equals(localStrategy.getWindow()))
      return windowFallback(stock, "global/local attached window contract is not bound");
    return lowerWindow(stock, local);
  }

  private ExecNode<?> lowerWindow(StreamExecWindowAggregate stock) throws IOException {
    return lowerWindow(stock, null);
  }

  private ExecNode<?> lowerWindow(ExecNode<?> stock, ExecNode<?> local) throws IOException {
    JsonNode definition = definitions.get(stock.getId());
    JsonNode assignment = local == null ? definition : definitions.get(local.getId());
    var windowing =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(assignment.path("windowing"));
    boolean finalWindows =
        local != null
            && windowing
                instanceof
                org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
    if (!(windowing instanceof TimeAttributeWindowingStrategy) && !finalWindows)
      return windowFallback(stock, "attached window state contract is not bound");
    int windowStartColumn = -1, windowEndColumn = -1;
    if (finalWindows) {
      var attached =
          (org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy) windowing;
      windowStartColumn = attached.getWindowStart();
      windowEndColumn = attached.getWindowEnd();
      if (windowStartColumn < 0 || windowEndColumn < 0)
        return windowFallback(stock, "attached local bounds are not available");
    }
    boolean processingTime = windowing.isProctime();
    if (processingTime && local != null)
      return windowFallback(stock, "processing-time partial boundaries are not bound");
    if (definition.path("needRetraction").asBoolean())
      return windowFallback(stock, "retracting state contract is not bound");
    var spec = windowing.getWindow();
    boolean session = FlinkCompat.isSessionWindow(spec);
    if (session && local != null)
      return windowFallback(stock, "SESSION partial boundaries are not bound");
    Duration offset;
    if (spec instanceof TumblingWindowSpec) {
      offset = ((TumblingWindowSpec) spec).getOffset();
    } else if (spec instanceof HoppingWindowSpec) {
      offset = ((HoppingWindowSpec) spec).getOffset();
    } else if (spec instanceof CumulativeWindowSpec) {
      offset = ((CumulativeWindowSpec) spec).getOffset();
    } else if (session) {
      offset = Duration.ZERO;
    } else {
      return windowFallback(stock, "window namespace family is not bound");
    }
    if (processingTime && offset != null && !offset.isZero())
      return windowFallback(stock, "processing-time offset assignment is not bound");
    long size =
        session
            ? WindowAggregateMatcher.gapMillis(windowing)
            : WindowAggregateMatcher.windowSize(windowing);
    long slide = session ? size : WindowAggregateMatcher.windowSlide(windowing);
    if (slide <= 0 || size <= 0 || size % slide != 0)
      return windowFallback(stock, "window size is not a positive multiple of its slice");
    boolean shared = !session && !finalWindows && !(spec instanceof TumblingWindowSpec);
    var raw = stockTransformations.get(stock.getId());
    if (!(raw instanceof OneInputTransformation))
      return windowFallback(stock, "state owner is not unary");
    var transformation = (OneInputTransformation<?, ?>) raw;
    if (!(transformation.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var stockOperator =
        ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
    int maximumParallelism = stockMaximum(transformation);
    if (stockOperator.getClass() != WindowAggOperator.class)
      return windowFallback(stock, "stock state owner is not synchronous window aggregation");
    var processor = releasedField(stockOperator, "windowProcessor");
    if (!(processor
        instanceof
        org.apache.flink.table.runtime.operators.aggregate.window.processors
            .AbstractSyncStateWindowAggProcessor)) return stock;
    if ((boolean) releasedField(processor, "isEventTime") == processingTime) return stock;
    var properties =
        reader
            .forType(NamedWindowProperty[].class)
            .<NamedWindowProperty[]>readValue(definition.path("namedWindowProperties"));
    if (properties.length != 0 && properties.length != 2 && properties.length != 3)
      return windowFallback(stock, "window properties have an unsupported arity");
    if (properties.length >= 2
        && (!(properties[0].getProperty() instanceof WindowStart)
            || !(properties[1].getProperty() instanceof WindowEnd)))
      return windowFallback(stock, "window properties have an unsupported order");
    if (properties.length == 3 && !(properties[2].getProperty() instanceof RowtimeAttribute))
      return windowFallback(stock, "window-time property is not rowtime");
    int[] keys = reader.forType(int[].class).readValue(assignment.path("grouping"));
    AggregateCall[] calls =
        reader.forType(AggregateCall[].class).readValue(definition.path("aggCalls"));
    if (session && Arrays.stream(calls).anyMatch(call -> call.filterArg >= 0))
      return windowFallback(stock, "SESSION filtered state contract is not bound");
    if (Arrays.stream(calls)
        .anyMatch(
            call ->
                call.isApproximate()
                    || call.isDistinct() && call.getAggregation().getKind() != SqlKind.COUNT))
      return windowFallback(
          stock, "aggregate requires an unbound approximate or data-view contract");
    var input = (local == null ? stock : local).getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    RowType outputType = (RowType) stock.getOutputType();
    var sequence = JavaScalaConversionUtil.toScala(Arrays.asList(calls));
    var relationalType = planner.getTypeFactory().buildRelNodeRowType(inputType);
    if (!RowDataArrowConverter.supports(inputType)
        || !RowDataArrowConverter.supports(outputType)
        || !WindowAggregateMatcher.supportedTimeAttribute(windowing)
        || !WindowAggregateMatcher.supportedAggregates(keys, sequence, relationalType))
      return windowFallback(
          stock, "input, output, grouping, time attribute or aggregate type is not admitted");
    var information =
        AggregateUtil.deriveStreamWindowAggregateInfoList(
            planner.getTypeFactory(), inputType, sequence, false, windowing.getWindow(), true);
    boolean hiddenCount = information.countStarInserted();
    if (information.aggInfos().length != calls.length + (hiddenCount ? 1 : 0))
      return windowFallback(stock, "stock accumulator count differs from its aggregate calls");
    int countStar = -1;
    List<Integer> averages = new ArrayList<>();
    int width = 0;
    for (int i = 0; i < information.aggInfos().length; i++) {
      var info = information.aggInfos()[i];
      int expectedWidth = i < calls.length ? WindowAggregateMatcher.partialWidth(calls[i]) : 1;
      if (info.viewSpecs().length != 0 || info.externalAccTypes().length != expectedWidth)
        return windowFallback(
            stock,
            "stock accumulator layout differs at aggregate "
                + i
                + ": "
                + info.externalAccTypes().length
                + " fields, "
                + info.viewSpecs().length
                + " views");
      if (width == information.getIndexOfCountStar()) countStar = i;
      if (i < calls.length && calls[i].getAggregation().getKind() == SqlKind.AVG)
        averages.add(width);
      width += info.externalAccTypes().length;
    }
    int[] kinds = WindowAggregateMatcher.kinds(sequence);
    int[] values = WindowAggregateMatcher.valueColumns(sequence);
    int[] types = WindowAggregateMatcher.valueTypeCodes(sequence, relationalType);
    int[] filters = Arrays.stream(calls).mapToInt(call -> call.filterArg).toArray();
    if (hiddenCount) {
      kinds = Arrays.copyOf(kinds, kinds.length + 1);
      kinds[kinds.length - 1] =
          tech.streamfusion.operator.NativeWindowOperatorCore.KIND_HIDDEN_LIVE_COUNT;
      values = Arrays.copyOf(values, values.length + 1);
      values[values.length - 1] = -1;
      types = Arrays.copyOf(types, types.length + 1);
      filters = Arrays.copyOf(filters, filters.length + 1);
      filters[filters.length - 1] = -1;
    }
    RowType accumulatorType =
        RowType.of(
            Arrays.stream(information.getAccTypes())
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new));
    if (!new org.apache.flink.table.runtime.typeutils.RowDataSerializer(accumulatorType)
        .equals(releasedField(processor, "accSerializer")))
      return windowFallback(
          stock, "generated accumulator serializer differs from the original owner");
    var distinctViews = new ArrayList<tech.streamfusion.state.StockWindowDistinctView>();
    for (var distinct : information.distinctInfos()) {
      if (distinct.excludeAcc() || distinct.dataViewSpec().isEmpty())
        return windowFallback(stock, "distinct accumulator is excluded or has no state view");
      var view = distinct.dataViewSpec().get();
      int[] slots = new int[distinct.aggIndexes().size()];
      for (int i = 0; i < slots.length; i++) {
        slots[i] = (Integer) distinct.aggIndexes().apply(i);
        if (slots[i] >= calls.length || !calls[slots[i]].isDistinct() || kinds[slots[i]] != 7)
          return windowFallback(
              stock,
              "distinct slot "
                  + slots[i]
                  + " is not bound to a distinct call: "
                  + Arrays.toString(kinds));
        if (session && types[slots[i]] == 100)
          return windowFallback(stock, "SESSION DISTINCT value schema is not bound");
        if (distinct.consumeRetraction())
          kinds[slots[i]] =
              tech.streamfusion.operator.NativeWindowOperatorCore.KIND_COUNT_DISTINCT_WITH_COUNTS;
      }
      distinctViews.add(
          new tech.streamfusion.state.StockWindowDistinctView(
              view.getStateId(),
              distinct.keyType().getLogicalType(),
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(view.getKeyDataType()),
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                  view.getValueDataType()),
              slots,
              distinct.consumeRetraction()));
    }
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            transformation
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    RowType keyType =
        RowType.of(Arrays.stream(keys).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new));
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false"))
        || Boolean.parseBoolean(
            config.getString("execution.checkpointing.unaligned.enabled", "false"))) return stock;
    String sessionTimeZoneId =
        org.apache.flink.table.planner.utils.TableConfigUtils.getLocalTimeZone(config).getId();
    String zone = ((java.time.ZoneId) releasedField(processor, "shiftTimeZone")).getId();
    if (!(WindowAggregateMatcher.isLtz(windowing)
            ? java.time.ZoneId.of(sessionTimeZoneId)
            : java.time.ZoneId.of("UTC"))
        .equals(java.time.ZoneId.of(zone))) return stock;
    StockSlicingWindowStateContract.WindowStart finalWindowStart = null;
    if (finalWindows) {
      var zoneId = java.time.ZoneId.of(zone);
      Duration windowOffset = offset == null ? Duration.ZERO : offset;
      org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner assigner;
      if (spec instanceof CumulativeWindowSpec)
        assigner =
            org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.cumulative(
                    Integer.MAX_VALUE, zoneId, Duration.ofMillis(size), Duration.ofMillis(slide))
                .withOffset(windowOffset);
      else if (spec instanceof HoppingWindowSpec)
        assigner =
            org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.hopping(
                    Integer.MAX_VALUE, zoneId, Duration.ofMillis(size), Duration.ofMillis(slide))
                .withOffset(windowOffset);
      else
        assigner =
            org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.tumbling(
                    Integer.MAX_VALUE, zoneId, Duration.ofMillis(size))
                .withOffset(windowOffset);
      finalWindowStart = assigner::getWindowStart;
    }
    StockStateContract contract =
        session
            ? new tech.streamfusion.state.StockSessionWindowStateContract(
                serializer,
                keyType,
                accumulatorType,
                size,
                zone,
                averages.stream().mapToInt(Integer::intValue).toArray(),
                distinctViews,
                width,
                processingTime)
            : new StockSlicingWindowStateContract(
                serializer,
                keyType,
                accumulatorType,
                size,
                slide,
                shared,
                countStar,
                zone,
                averages.stream().mapToInt(Integer::intValue).toArray(),
                distinctViews,
                width,
                finalWindowStart,
                processingTime);
    ExecNode<?> rows =
        input instanceof StreamExecExchange ? input.getInputEdges().get(0).getSource() : input;
    ExecNode<?> entry = arrowInput(rows, config, inputType, false);
    RowType exchangeType = inputType;
    int[] exchangeKeys = keys;
    if (local != null) {
      var fields = new ArrayList<RowType.RowField>();
      for (int i = 0; i < keys.length; i++)
        fields.add(new RowType.RowField("key" + i, inputType.getTypeAt(keys[i])));
      var partials = new ArrayList<LogicalType>();
      for (int i = 0; i < kinds.length; i++) {
        if (i < calls.length && calls[i].isDistinct()) {
          partials.add(
              new org.apache.flink.table.types.logical.ArrayType(inputType.getTypeAt(values[i])));
          if (kinds[i]
              == tech.streamfusion.operator.NativeWindowOperatorCore
                  .KIND_COUNT_DISTINCT_WITH_COUNTS) {
            partials.add(
                new org.apache.flink.table.types.logical.ArrayType(
                    new org.apache.flink.table.types.logical.BigIntType(false)));
            partials.add(new org.apache.flink.table.types.logical.BigIntType(false));
          }
        } else {
          for (var type : information.aggInfos()[i].externalAccTypes())
            partials.add(type.getLogicalType());
        }
      }
      for (int i = 0; i < partials.size(); i++)
        fields.add(new RowType.RowField("partial" + i, partials.get(i)));
      fields.add(
          new RowType.RowField(
              "slice_end", new org.apache.flink.table.types.logical.BigIntType(false)));
      exchangeType = new RowType(fields);
      exchangeKeys = java.util.stream.IntStream.range(0, keys.length).toArray();
      var partial =
          new NativeColumnarLocalWindowAggExecNode(
              config,
              InputProperty.DEFAULT,
              exchangeType,
              "NativeColumnarLocalWindowAggregate",
              slide,
              finalWindows ? -1 : WindowAggregateMatcher.timeColumn(windowing),
              windowStartColumn,
              windowEndColumn,
              values,
              filters,
              keys,
              types,
              kinds,
              WindowAggregateMatcher.isLtz(windowing),
              FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys));
      partial.withStockLocalState(
          stockMaximum(stockTransformations.get(local.getId())),
          sessionTimeZoneId,
          offset == null ? 0 : offset.toMillis());
      connect(entry, partial);
      bindings.put(partial, new StockBinding(local.getId(), stockUids.get(local.getId()), null));
      columnar.add(partial);
      lowered.put(local, partial);
      entry = partial;
    }
    var exchange =
        new NativeColumnarExchangeExecNode(
            config,
            InputProperty.DEFAULT,
            exchangeType,
            "NativeColumnarExchange",
            exchangeKeys,
            FlinkKeyGroupUtils.stockKeyDescriptor(exchangeType, exchangeKeys),
            keys.length == 0);
    exchange.withStateMaxParallelism(maximumParallelism);
    connect(entry, exchange);
    ExecNode<?> aggregate;
    if (session) {
      aggregate =
          new NativeColumnarSessionWindowAggExecNode(
                  config,
                  InputProperty.DEFAULT,
                  outputType,
                  "NativeColumnarSessionWindowAggregate",
                  size,
                  WindowAggregateMatcher.timeColumn(windowing),
                  values,
                  keys,
                  types,
                  kinds,
                  processingTime,
                  WindowAggregateMatcher.isLtz(windowing),
                  FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
              .withStockState(maximumParallelism, sessionTimeZoneId);
    } else if (local != null) {
      var global =
          new NativeColumnarGlobalWindowAggExecNode(
                  config,
                  InputProperty.DEFAULT,
                  outputType,
                  finalWindows
                      ? "NativeColumnarGlobalWindowAggregateFinal"
                      : "NativeColumnarGlobalWindowAggregateSlice",
                  size,
                  slide,
                  WindowAggregateMatcher.isCumulative(windowing),
                  exchangeKeys,
                  types,
                  kinds,
                  WindowAggregateMatcher.isLtz(windowing),
                  FlinkKeyGroupUtils.stockKeyDescriptor(exchangeType, exchangeKeys))
              .withStockState(
                  maximumParallelism, sessionTimeZoneId, offset == null ? 0 : offset.toMillis());
      if (finalWindows) global.withFinalWindows();
      aggregate = global;
    } else {
      var single =
          new NativeColumnarWindowAggExecNode(
              config,
              InputProperty.DEFAULT,
              outputType,
              "NativeColumnarWindowAggregate",
              WindowAggregateMatcher.isCumulative(windowing),
              size,
              slide,
              WindowAggregateMatcher.timeColumn(windowing),
              values,
              filters,
              keys,
              types,
              kinds,
              processingTime,
              WindowAggregateMatcher.isLtz(windowing),
              FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys));
      single.withStateMaxParallelism(maximumParallelism);
      single.withStateTimeZone(sessionTimeZoneId);
      single.withWindowOffset(offset == null ? 0 : offset.toMillis());
      aggregate = single;
    }
    connect(exchange, aggregate);
    bindings.put(
        aggregate, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(aggregate);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(aggregate, exit);
    return exit;
  }

  private ExecNode<?> windowFallback(ExecNode<?> stock, String reason) {
    recordFallback("window aggregate: " + reason);
    return stock;
  }

  private ExecNode<?> lowerWindowRank(ExecNode<?> stock, boolean deduplicate) throws IOException {
    var definition = definitions.get(stock.getId());
    if (!deduplicate && !definition.path("rankType").asText().equals("ROW_NUMBER")) return stock;
    long limit = 1;
    if (!deduplicate) {
      var range =
          reader
              .forType(org.apache.flink.table.runtime.operators.rank.RankRange.class)
              .<org.apache.flink.table.runtime.operators.rank.RankRange>readValue(
                  definition.path("rankRange"));
      if (!(range instanceof org.apache.flink.table.runtime.operators.rank.ConstantRankRange))
        return stock;
      var constant = (org.apache.flink.table.runtime.operators.rank.ConstantRankRange) range;
      if (constant.getRankStart() != 1 || constant.getRankEnd() <= 0) return stock;
      limit = constant.getRankEnd();
    }
    var windowing =
        reader
            .forType(WindowingStrategy.class)
            .<WindowingStrategy>readValue(definition.path("windowing"));
    if (!(windowing
            instanceof org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy)
        || !windowing.isRowtime()) return stock;
    var attached =
        (org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy) windowing;
    if (attached.getWindowStart() < 0 || attached.getWindowEnd() < 0) return stock;
    var input = stock.getInputEdges().get(0).getSource();
    var inputType = (RowType) stock.getInputEdges().get(0).getOutputType();
    if (!RowDataArrowConverter.supports(inputType)) return stock;
    int[] partitions, sortColumns, ascending, nullsFirst;
    if (deduplicate) {
      partitions = reader.forType(int[].class).readValue(definition.path("partitionKeys"));
      sortColumns = new int[] {definition.path("orderKey").asInt()};
      ascending = new int[] {definition.path("keepLastRow").asBoolean() ? 0 : 1};
      nullsFirst = new int[] {0};
    } else {
      partitions =
          reader
              .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec.class)
              .<org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec>readValue(
                  definition.path("partitionSpec"))
              .getFieldIndices();
      var sort =
          reader
              .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec.class)
              .<org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec>readValue(
                  definition.path("sortSpec"));
      sortColumns = sort.getFieldIndices();
      ascending = new int[sortColumns.length];
      nullsFirst = new int[sortColumns.length];
      for (int i = 0; i < sortColumns.length; i++) {
        ascending[i] = sort.getFieldSpec(i).getIsAscendingOrder() ? 1 : 0;
        nullsFirst[i] = sort.getFieldSpec(i).getNullIsLast() ? 0 : 1;
      }
    }
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)
        || !(original.getStateKeySelector()
            instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (operator.getClass()
            != org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator.class
        || !(boolean) releasedField(operator, "isEventTime")) return stock;
    var processor = releasedField(operator, "windowProcessor");
    if (processor.getClass()
        != (deduplicate
            ? org.apache.flink.table.runtime.operators.deduplicate.window.processors
                .RowTimeSyncStateWindowDeduplicateProcessor.class
            : org.apache.flink.table.runtime.operators.rank.window.processors
                .SyncStateWindowRankProcessor.class)) return stock;
    var rowSerializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(inputType);
    if (!rowSerializer.equals(releasedField(processor, "inputSerializer"))
        || (int) releasedField(processor, "windowEndIndex") != attached.getWindowEnd())
      return stock;
    if (deduplicate) {
      var buffer = releasedField(processor, "bufferFactory");
      if (buffer.getClass()
          != org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer
              .Factory.class) return stock;
      var combiner = releasedField(buffer, "factory");
      if (combiner.getClass()
              != org.apache.flink.table.runtime.operators.deduplicate.window.combines
                  .RowTimeDeduplicateRecordsCombiner.Factory.class
          || (int) releasedField(combiner, "rowtimeIndex") != sortColumns[0]
          || (boolean) releasedField(combiner, "keepLastRow")
              != definition.path("keepLastRow").asBoolean()
          || !rowSerializer.equals(releasedField(combiner, "recordSerializer"))) return stock;
    } else if ((long) releasedField(processor, "rankStart") != 1
        || (long) releasedField(processor, "rankEnd") != limit
        || (boolean) releasedField(processor, "outputRankNumber")
            != definition.path("outputRowNumber").asBoolean()) return stock;
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> keySerializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var selectors =
        org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            partitions,
            org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(inputType));
    var sortSelector =
        org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            sortColumns,
            org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(inputType));
    if (!keySerializer.equals(selectors.getProducedType().toSerializer())) return stock;
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> sortSerializer =
        deduplicate
            ? null
            : (TypeSerializer<RowData>) releasedField(processor, "sortKeySerializer");
    if (!deduplicate && !sortSerializer.equals(sortSelector.getProducedType().toSerializer()))
      return stock;
    int maximum = stockMaximum(original);
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    String zone = ((java.time.ZoneId) releasedField(processor, "shiftTimeZone")).getId();
    var zoneId = java.time.ZoneId.of(zone);
    var spec = windowing.getWindow();
    if (!(spec instanceof TumblingWindowSpec)
        && !(spec instanceof HoppingWindowSpec)
        && !(spec instanceof CumulativeWindowSpec)) return stock;
    long size = WindowAggregateMatcher.windowSize(windowing),
        slide = WindowAggregateMatcher.windowSlide(windowing);
    Duration offset;
    org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner assigner;
    if (spec instanceof TumblingWindowSpec) {
      offset = ((TumblingWindowSpec) spec).getOffset();
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.tumbling(
                  Integer.MAX_VALUE, zoneId, Duration.ofMillis(size))
              .withOffset(offset == null ? Duration.ZERO : offset);
    } else if (spec instanceof HoppingWindowSpec) {
      offset = ((HoppingWindowSpec) spec).getOffset();
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.hopping(
                  Integer.MAX_VALUE, zoneId, Duration.ofMillis(size), Duration.ofMillis(slide))
              .withOffset(offset == null ? Duration.ZERO : offset);
    } else if (spec instanceof CumulativeWindowSpec) {
      offset = ((CumulativeWindowSpec) spec).getOffset();
      assigner =
          org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.cumulative(
                  Integer.MAX_VALUE, zoneId, Duration.ofMillis(size), Duration.ofMillis(slide))
              .withOffset(offset == null ? Duration.ZERO : offset);
    } else return stock;
    var contract =
        deduplicate
            ? tech.streamfusion.state.StockWindowRankStateContract.deduplicate(
                keySerializer,
                selectors.getProducedType().toRowType(),
                inputType,
                partitions,
                sortColumns[0],
                attached.getWindowStart(),
                attached.getWindowEnd(),
                zone,
                assigner::getWindowStart)
            : new tech.streamfusion.state.StockWindowRankStateContract(
                keySerializer,
                selectors.getProducedType().toRowType(),
                sortSerializer,
                sortSelector.getProducedType().toRowType(),
                inputType,
                partitions,
                sortColumns,
                attached.getWindowStart(),
                attached.getWindowEnd(),
                zone,
                assigner::getWindowStart);
    var exchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(input, config, inputType, partitions, false);
    exchange.withStateMaxParallelism(maximum);
    var rank =
        new NativeWindowRankExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeColumnarWindowRank",
                attached.getWindowStart(),
                attached.getWindowEnd(),
                partitions,
                sortColumns,
                ascending,
                nullsFirst,
                limit,
                !deduplicate && definition.path("outputRowNumber").asBoolean(),
                false,
                size,
                slide,
                spec instanceof CumulativeWindowSpec,
                zoneId.getRules().isFixedOffset()
                    ? zoneId.getRules().getOffset(java.time.Instant.EPOCH).getTotalSeconds() * 1000L
                    : 0,
                deduplicate && definition.path("keepLastRow").asBoolean(),
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, partitions))
            .withCalendarZone(zone)
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

  private ExecNode<?> lowerTemporalSort(ExecNode<?> stock) throws IOException {
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)
        || !(original.getStateKeySelector()
            instanceof org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector)
        || original.getParallelism() != 1) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (operator.getClass()
        != org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator.class) return stock;
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec>readValue(
                definitions.get(stock.getId()).path("orderBy"));
    if (spec.getFieldSize() != 1 || !spec.getFieldSpec(0).getIsAscendingOrder()) return stock;
    var input = stock.getInputEdges().get(0).getSource();
    var inputType = (RowType) input.getOutputType();
    int time = spec.getFieldSpec(0).getFieldIndex();
    if (time < 0
        || time >= inputType.getFieldCount()
        || !LogicalTypeChecks.isRowtimeAttribute(inputType.getTypeAt(time))
        || LogicalTypeChecks.getPrecision(inputType.getTypeAt(time)) > 3
        || !RowDataArrowConverter.supports(inputType)
        || !inputType.equals(stock.getOutputType())
        || (int) releasedField(operator, "rowTimeIdx") != time
        || releasedField(operator, "gComparator") != null) return stock;
    var retained =
        (org.apache.flink.table.runtime.typeutils.InternalTypeInfo<?>)
            releasedField(operator, "inputRowType");
    if (!inputType.equals(retained.toRowType())) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var contract =
        new tech.streamfusion.state.StockTemporalSortStateContract(serializer, inputType, time);
    int maximum = stockMaximum(original);
    var sort =
        new NativeTemporalSortExecNode(
            config,
            InputProperty.DEFAULT,
            (RowType) stock.getOutputType(),
            "NativeTemporalSort",
            time,
            maximum);
    connect(arrowInput(input, config, inputType, false), sort);
    bindings.put(sort, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(sort);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(sort, exit);
    return exit;
  }

  private ExecNode<?> lowerOver(ExecNode<?> stock) throws IOException {
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)
        || !(original.getStateKeySelector()
            instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    if (!(operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator))
      return stock;
    var function =
        ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
            .getUserFunction();
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec>readValue(
                definitions.get(stock.getId()).path("overSpec"));
    if (spec.getGroups().size() != 1) return stock;
    var group = spec.getGroups().get(0);
    var input = stock.getInputEdges().get(0).getSource();
    var inputType = (RowType) input.getOutputType();
    int[] keys = spec.getPartition().getFieldIndices(), order = group.getSort().getFieldIndices();
    boolean bounded = !group.getLowerBound().isUnbounded();
    if (keys.length > 1
        || keys.length == 1 && !inputType.getTypeAt(keys[0]).is(LogicalTypeRoot.BIGINT)
        || order.length != 1
        || !group.getSort().getAscendingOrders()[0]
        || !(LogicalTypeChecks.isProctimeAttribute(inputType.getTypeAt(order[0]))
            || LogicalTypeChecks.isRowtimeAttribute(inputType.getTypeAt(order[0])))
        || bounded && !(group.getLowerBound().isPreceding() || group.getLowerBound().isCurrentRow())
        || !group.getUpperBound().isCurrentRow()) return stock;
    if (inputType.getChildren().stream()
        .anyMatch(
            type ->
                !NativeOverAggExecNode.stockArithmeticValueType(type)
                    && !((type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                            || type.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
                        && LogicalTypeChecks.getPrecision(type) <= 3))) return stock;
    boolean processing = LogicalTypeChecks.isProctimeAttribute(inputType.getTypeAt(order[0]));
    if (bounded && processing && !group.isRows() || !bounded && !processing) return stock;
    long offset = 0;
    if (bounded) {
      var bound =
          org.apache.flink.table.planner.plan.utils.OverAggregateUtil.getBoundary(
              spec, group.getLowerBound());
      if (!(bound instanceof Long) || (long) bound > 0 || (long) bound == Long.MIN_VALUE)
        return stock;
      offset = -(long) bound;
      if (group.isRows() && offset == Long.MAX_VALUE) return stock;
    }
    Class<?> expected =
        !bounded
            ? org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction.class
            : processing
                ? org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction
                    .class
                : group.isRows()
                    ? org.apache.flink.table.runtime.operators.over
                        .RowTimeRowsBoundedPrecedingFunction.class
                    : org.apache.flink.table.runtime.operators.over
                        .RowTimeRangeBoundedPrecedingFunction.class;
    if (function.getClass() != expected) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    long retention =
        config
            .get(org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION)
            .toMillis();
    if (!bounded) {
      var ttl =
          (org.apache.flink.api.common.state.StateTtlConfig) releasedField(function, "ttlConfig");
      retention = ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0;
      config.setString("table.exec.state.ttl", retention + " ms");
    }
    if (bounded && !processing && group.isRows() && retention > 1) return stock;
    var calls = group.getAggCalls();
    if (calls.isEmpty()) return stock;
    boolean countStar =
        calls.stream()
            .anyMatch(
                call ->
                    call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.COUNT
                        && call.getArgList().isEmpty());
    var referenced =
        calls.stream()
            .flatMap(call -> call.getArgList().stream())
            .filter(index -> index >= inputType.getFieldCount())
            .map(index -> index - inputType.getFieldCount())
            .collect(java.util.stream.Collectors.toSet());
    var constants = OverConstantArguments.create(spec.getConstants(), countStar, referenced);
    if (constants.isEmpty()) return stock;
    var argumentType = constants.get().appendTo(inputType);
    int[] kinds = new int[calls.size()],
        columns = new int[calls.size()],
        types = new int[calls.size()];
    for (int i = 0; i < calls.size(); i++) {
      var call = calls.get(i);
      if (call.isDistinct()
          || call.isApproximate()
          || call.filterArg >= 0
          || !call.getCollation().getFieldCollations().isEmpty()
          || call.getArgList().size() > 1) return stock;
      switch (call.getAggregation().getKind()) {
        case SUM:
          kinds[i] = 0;
          break;
        case COUNT:
          kinds[i] = 3;
          break;
        case AVG:
          kinds[i] = 4;
          break;
        case SUM0:
          kinds[i] = 22;
          break;
        default:
          return stock;
      }
      if (call.getArgList().isEmpty() && kinds[i] != 3) return stock;
      columns[i] =
          call.getArgList().isEmpty()
              ? inputType.getFieldCount() + spec.getConstants().size()
              : call.getArgList().get(0);
      if (columns[i] < 0 || columns[i] >= argumentType.getFieldCount()) return stock;
      var type = argumentType.getTypeAt(columns[i]);
      if (!NativeOverAggExecNode.stockArithmeticValueType(type)
          || !bounded && type.is(LogicalTypeRoot.DECIMAL)) return stock;
      types[i] =
          WindowAggregateMatcher.windowValueTypeCode(
              planner.getTypeFactory().createFieldTypeFromLogicalType(type));
    }
    boolean[] retract = new boolean[calls.size()];
    Arrays.fill(retract, bounded);
    var infos =
        org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToStreamAggregateInfoList(
            planner.getTypeFactory(),
            argumentType,
            org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(calls),
            retract,
            bounded,
            true,
            true);
    if (infos.distinctInfos().length != 0
        || infos.getActualAggregateInfos().length != calls.size()
        || Arrays.stream(infos.aggInfos()).anyMatch(info -> info.viewSpecs().length != 0))
      return stock;
    var generated =
        Arrays.stream(infos.getAccTypes())
            .map(org.apache.flink.table.types.DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    if (!Arrays.equals(generated, (LogicalType[]) releasedField(function, "accTypes")))
      return stock;
    int[] widths =
        Arrays.stream(infos.getActualAggregateInfos())
            .mapToInt(info -> info.externalAccTypes().length)
            .toArray();
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var selector =
        (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
            original.getStateKeySelector();
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    StockStateContract contract;
    try {
      int hiddenCount = infos.countStarInserted() ? infos.getIndexOfCountStar() : -1;
      contract =
          !bounded
              ? new tech.streamfusion.state.StockOverFoldStateContract(
                  serializer, keyType, RowType.of(generated), kinds, widths, retention > 0)
              : processing
                  ? new tech.streamfusion.state.StockOverProcessingRowsStateContract(
                      serializer,
                      keyType,
                      inputType,
                      RowType.of(generated),
                      keys,
                      order[0],
                      kinds,
                      widths,
                      hiddenCount,
                      constants.get().type(),
                      constants.get().encodedValues(),
                      offset + 1,
                      retention > 1)
                  : group.isRows()
                      ? new tech.streamfusion.state.StockOverEventRowsStateContract(
                          serializer,
                          keyType,
                          inputType,
                          RowType.of(generated),
                          keys,
                          order[0],
                          kinds,
                          widths,
                          hiddenCount,
                          constants.get().type(),
                          constants.get().encodedValues(),
                          offset + 1)
                      : new tech.streamfusion.state.StockOverRangeStateContract(
                          serializer,
                          keyType,
                          inputType,
                          RowType.of(generated),
                          keys,
                          order[0],
                          kinds,
                          widths,
                          hiddenCount,
                          constants.get().type(),
                          constants.get().encodedValues());
    } catch (IllegalArgumentException unsupported) {
      return stock;
    }
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode) canonicalJoinInput(input, config, inputType, keys, true);
    exchange.withStateMaxParallelism(maximum);
    var over =
        new NativeOverAggExecNode(
                config,
                InputProperty.DEFAULT,
                (RowType) stock.getOutputType(),
                "NativeOverAggregate",
                order[0],
                columns,
                keys,
                types,
                kinds,
                bounded ? group.isRows() ? 1 : 2 : group.isRows() ? 3 : 0,
                offset,
                processing,
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
            .withStateMaxParallelism(maximum);
    if (constants.get().type().getFieldCount() > 0)
      over.withConstantArguments(constants.get().type(), constants.get().encodedValues());
    connect(exchange, over);
    bindings.put(over, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(over);
    var exit =
        new ArrowToRowDataExecNode(
            config, InputProperty.DEFAULT, (RowType) stock.getOutputType(), "ArrowToRowData");
    connect(over, exit);
    return exit;
  }

  private ExecNode<?> lowerGroupAggregate(ExecNode<?> stock) throws IOException {
    var arithmetic = lowerArithmeticGroupAggregate(stock);
    if (arithmetic != stock) return arithmetic;
    var typed = lowerTypedGroupAggregate(stock);
    return typed == stock ? lowerJvmGroupAggregate(stock) : typed;
  }

  private ExecNode<?> lowerJvmGroupAggregate(ExecNode<?> stock) throws IOException {
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.jvmGroupAggregates", "true"))) return stock;
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation) || !ordinaryKeyLayout(transformation))
      return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)
        || !(original.getStateKeySelector()
            instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    boolean miniBatch =
        operator.getClass()
            == org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator.class;
    Object function;
    if (miniBatch) function = releasedField(operator, "function");
    else if (operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator)
      function =
          ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
              .getUserFunction();
    else return stock;
    if (function.getClass()
        != (miniBatch
            ? org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction.class
            : org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction.class))
      return stock;
    JsonNode definition = definitions.get(stock.getId());
    var calls =
        reader
            .forType(org.apache.calcite.rel.core.AggregateCall[].class)
            .<org.apache.calcite.rel.core.AggregateCall[]>readValue(definition.path("aggCalls"));
    boolean userAggregate = false;
    for (var call : calls) {
      var user = JvmGroupAggregateMatcher.userFunction(call.getAggregation());
      if (user instanceof org.apache.flink.table.functions.python.PythonFunction
          || user instanceof org.apache.flink.table.functions.SpecializedFunction) return stock;
      userAggregate |= user != null;
    }
    if (!userAggregate) return stock;
    var input = stock.getInputEdges().get(0).getSource();
    var inputType = (RowType) input.getOutputType();
    var outputType = (RowType) stock.getOutputType();
    if (!typedGroupArrowType(inputType) || !typedGroupArrowType(outputType)) return stock;
    var config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false"))
        || Boolean.parseBoolean(
            config.getString("execution.checkpointing.unaligned.enabled", "false"))) return stock;
    int[] grouping = reader.forType(int[].class).readValue(definition.path("grouping"));
    int maximum = stockMaximum(original);
    var exchange =
        (NativeColumnarExchangeExecNode)
            canonicalJoinInput(input, config, inputType, grouping, true);
    exchange.withStateMaxParallelism(maximum);
    long retention;
    if (miniBatch) retention = ((Number) releasedField(function, "stateRetentionTime")).longValue();
    else {
      var ttl =
          (org.apache.flink.api.common.state.StateTtlConfig) releasedField(function, "ttlConfig");
      retention = ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0;
    }
    var owner =
        new CapturedJvmGroupExecNode(
            config,
            inputType,
            outputType,
            miniBatch
                ? null
                : (org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction) function,
            miniBatch
                ? (org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction)
                    function
                : null,
            miniBatch
                ? (org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger<
                        org.apache.flink.table.data.RowData>)
                    releasedField(operator, "bundleTrigger")
                : null,
            (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
                original.getStateKeySelector(),
            grouping,
            maximum,
            retention);
    connect(exchange, owner);
    bindings.put(owner, new StockBinding(stock.getId(), stockUids.get(stock.getId()), null));
    columnar.add(owner);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(owner, exit);
    return exit;
  }

  private static boolean typedGroupArrowType(LogicalType type) {
    return type.getTypeRoot() != LogicalTypeRoot.RAW
        && ArrowRowTypeSupport.supports(type)
        && type.getChildren().stream().allMatch(StockCompiledPlanLowering::typedGroupArrowType);
  }

  private static int typedGroupValueCode(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      case CHAR:
      case VARCHAR:
        return 3;
      case SMALLINT:
        return 4;
      case TINYINT:
        return 5;
      case FLOAT:
        return 6;
      case DATE:
        return 8;
      case TIME_WITHOUT_TIME_ZONE:
        return 9;
      case BOOLEAN:
        return 10;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 1000
            + org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type);
      case DECIMAL:
        return 2000
            + 100 * org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type)
            + org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale(type);
      default:
        return -1;
    }
  }

  private ExecNode<?> lowerTypedGroupAggregate(ExecNode<?> stock) throws IOException {
    return lowerTypedGroupAggregate(stock, null);
  }

  private ExecNode<?> lowerGlobalGroupAggregate(ExecNode<?> stock) throws IOException {
    var exchange = stock.getInputEdges().get(0).getSource();
    if (!(exchange instanceof StreamExecExchange)
        || consumerCounts.getOrDefault(exchange.getId(), 0) != 1) return stock;
    var local = exchange.getInputEdges().get(0).getSource();
    if (!(local
            instanceof
            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate)
        || consumerCounts.getOrDefault(local.getId(), 0) != 1) return stock;
    return lowerTypedGroupAggregate(stock, local);
  }

  private ExecNode<?> lowerTypedGroupAggregate(ExecNode<?> stock, ExecNode<?> local)
      throws IOException {
    boolean global = local != null;
    var transformation = stockTransformations.get(stock.getId());
    if (!(transformation instanceof OneInputTransformation)) return stock;
    var original = (OneInputTransformation<?, ?>) transformation;
    if (!(original.getOperatorFactory() instanceof SimpleOperatorFactory)) return stock;
    var operator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    boolean miniBatch =
        operator instanceof org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
    Object function;
    if (miniBatch) function = releasedField(operator, "function");
    else if (operator instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator)
      function =
          ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) operator)
              .getUserFunction();
    else return stock;
    Class<?> expectedFunctionClass =
        global
            ? org.apache.flink.table.runtime.operators.aggregate.MiniBatchGlobalGroupAggFunction
                .class
            : miniBatch
                ? org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction.class
                : org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction.class;
    if (function.getClass() != expectedFunctionClass || global && !miniBatch) return stock;
    if (!(original.getStateKeySelector()
        instanceof org.apache.flink.table.runtime.keyselector.RowDataKeySelector)) return stock;
    JsonNode definition = definitions.get(stock.getId());
    int[] keys = reader.forType(int[].class).readValue(definition.path("grouping"));
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    RowType sourceType =
        global ? (RowType) local.getInputEdges().get(0).getSource().getOutputType() : inputType;
    int[] localKeys =
        global
            ? reader.forType(int[].class).readValue(definitions.get(local.getId()).path("grouping"))
            : keys;
    if (global
        && !Arrays.equals(keys, java.util.stream.IntStream.range(0, localKeys.length).toArray()))
      return stock;
    RowType outputType = (RowType) stock.getOutputType();
    if (!global && !typedGroupArrowType(inputType)
        || !typedGroupArrowType(outputType)
        || !typedGroupArrowType(sourceType)) return stock;
    for (int i = 0; i < keys.length; i++)
      if (!inputType.getTypeAt(keys[i]).equals(outputType.getTypeAt(i))) return stock;
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    if (Boolean.parseBoolean(config.getString("execution.checkpointing.unaligned.enabled", "false"))
        || Boolean.parseBoolean(config.getString("table.exec.async-state.enabled", "false")))
      return stock;
    boolean retract = definition.path("needRetraction").asBoolean();
    if (miniBatch && retract) return stock;
    var calls =
        reader
            .forType(org.apache.calcite.rel.core.AggregateCall[].class)
            .<org.apache.calcite.rel.core.AggregateCall[]>readValue(definition.path("aggCalls"));
    boolean[] retractions =
        reader.forType(boolean[].class).readValue(definition.path("aggCallNeedRetractions"));
    var infos =
        org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToStreamAggregateInfoList(
            planner.getTypeFactory(),
            sourceType,
            org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(
                Arrays.asList(calls)),
            retractions,
            retract,
            true,
            true);
    var localKinds = new ArrayList<Integer>();
    var localTypes = new ArrayList<Integer>();
    var localColumns = new ArrayList<Integer>();
    var localFilters = new ArrayList<Integer>();
    var localViewKeys = new ArrayList<String>();
    var localViewSources = new ArrayList<Integer>();
    var localViewTypes = new ArrayList<LogicalType>();
    LogicalType[] actualTypes = (LogicalType[]) releasedField(function, "accTypes");
    LogicalType[] derivedTypes =
        Arrays.stream(infos.getAccTypes())
            .map(org.apache.flink.table.types.DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    int recordCount = infos.getIndexOfCountStar();
    var counter = releasedField(function, "recordCounter");
    if (!Arrays.equals(actualTypes, derivedTypes)
        || retract && recordCount < 0
        || counter.getClass()
            != org.apache.flink.table.runtime.operators.aggregate.RecordCounter.of(recordCount)
                .getClass()
        || recordCount >= 0 && recordCount != (Integer) releasedField(counter, "indexOfCountStar"))
      return stock;
    int size = infos.getActualAggregateInfos().length;
    if (size == 0) return stock;
    int[] kinds = new int[size],
        types = new int[size],
        columns = new int[size],
        scalars = new int[size],
        counts = new int[size],
        viewColumns = new int[size],
        filters = new int[size];
    Arrays.fill(counts, -1);
    Arrays.fill(viewColumns, -1);
    var nativeIndices = new java.util.HashMap<Integer, Integer>();
    int position = 0, actual = 0;
    for (var info : infos.aggInfos()) {
      int width = info.externalAccTypes().length;
      if (infos.countStarInserted()
          && info.aggIndex() == ((Number) infos.indexOfCountStar().get()).intValue()) {
        position += width;
        continue;
      }
      var call = info.aggIndex() < calls.length ? calls[info.aggIndex()] : info.agg();
      if (call.isApproximate()
          || !call.getCollation().getFieldCollations().isEmpty()
          || info.argIndexes().length > 1
          || info.viewSpecs().length != 0) return stock;
      Class<?> type = info.function().getClass();
      boolean countAll =
          type == org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction.class;
      boolean count =
          countAll
              || type
                  == org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction.class
              || type
                  == org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction
                      .LongSum0AggFunction.class;
      int kind, expected;
      if (call.isDistinct()) {
        if (info.argIndexes().length != 1) return stock;
        if (count && call.getAggregation().getKind() == SqlKind.COUNT) kind = 7;
        else if (!retract
            && call.getAggregation().getKind() == SqlKind.SUM
            && type.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction.class)
          kind = 9;
        else return stock;
        expected = 1;
      } else if (count) {
        kind =
            type
                    == org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction
                        .LongSum0AggFunction.class
                ? 22
                : 3;
        expected = 1;
      } else if (!retract
          && type.getEnclosingClass()
              == org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction.class) {
        kind = 0;
        expected = 1;
      } else if (retract
          && type.getEnclosingClass()
              == org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction
                  .class) {
        kind = 0;
        expected = 2;
      } else if (type.getEnclosingClass()
          == org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction.class) {
        kind = 4;
        expected = 2;
      } else if (!retract
          && type.getEnclosingClass()
              == org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction.class) {
        kind = 10;
        expected = 1;
      } else if (!retract
          && type.getEnclosingClass()
              == org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction.class) {
        kind = 11;
        expected = 1;
      } else return stock;
      if (width != expected) return stock;
      int value =
          kind == 7
              ? call.getArgList().get(0)
              : countAll || info.argIndexes().length == 0 ? -1 : info.argIndexes()[0];
      int valueType = value < 0 ? 0 : typedGroupValueCode(sourceType.getTypeAt(value));
      if (valueType < 0) return stock;
      if (call.filterArg >= 0 && !sourceType.getTypeAt(call.filterArg).is(LogicalTypeRoot.BOOLEAN))
        return stock;
      kinds[actual] = kind;
      types[actual] = valueType;
      columns[actual] = value;
      scalars[actual] = position;
      if (width == 2) counts[actual] = position + 1;
      filters[actual] = call.filterArg;
      if (global) {
        int localPosition = localKinds.size();
        if (call.isDistinct()) {
          String viewKey = call.getArgList() + "#" + call.filterArg;
          int view = localViewKeys.indexOf(viewKey);
          if (view < 0) {
            view = localViewKeys.size();
            localViewKeys.add(viewKey);
            localViewSources.add(localPosition);
            localViewTypes.add(sourceType.getTypeAt(value));
          }
          viewColumns[actual] = view;
        }
        localKinds.add(kind == 4 ? 8 : kind);
        localTypes.add(kind == 4 ? typedGroupValueCode(actualTypes[position]) : valueType);
        localColumns.add(value);
        localFilters.add(call.filterArg);
        if (kind == 4) {
          localKinds.add(3);
          localTypes.add(valueType);
          localColumns.add(value);
          localFilters.add(call.filterArg);
        }
        columns[actual] = localKeys.length + position;
        kinds[actual] = kind == 3 ? 22 : kind;
        if (kind != 4 && kind != 7 && kind != 9)
          types[actual] = typedGroupValueCode(inputType.getTypeAt(columns[actual]));
        filters[actual] = -1;
      }
      nativeIndices.put(info.aggIndex(), actual);
      actual++;
      position += width;
    }
    if (actual != size || position + infos.distinctInfos().length != actualTypes.length)
      return stock;
    var views = new ArrayList<tech.streamfusion.state.StockGroupDistinctView>();
    for (var distinct : infos.distinctInfos()) {
      if (distinct.excludeAcc() || distinct.dataViewSpec().isEmpty()) return stock;
      var view = distinct.dataViewSpec().get();
      int[] slots = new int[distinct.aggIndexes().size()];
      for (int i = 0; i < slots.length; i++) {
        Integer index = nativeIndices.get((Integer) distinct.aggIndexes().apply(i));
        if (index == null
            || !tech.streamfusion.state.StockGroupDistinctView.supportsAggregateIndex(index)
            || kinds[index] != 7 && kinds[index] != 9) return stock;
        slots[i] = index;
      }
      views.add(
          new tech.streamfusion.state.StockGroupDistinctView(
              view.getStateId(),
              distinct.keyType().getLogicalType(),
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(view.getKeyDataType()),
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                  view.getValueDataType()),
              slots,
              distinct.consumeRetraction()));
    }
    var ttl =
        miniBatch
            ? org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig(
                (Long) releasedField(function, "stateRetentionTime"))
            : (org.apache.flink.api.common.state.StateTtlConfig)
                releasedField(function, "ttlConfig");
    long retention = ttl.isEnabled() ? ttl.getTimeToLive().toMillis() : 0;
    config.setString("table.exec.state.ttl", retention + " ms");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    @SuppressWarnings("unchecked")
    TypeSerializer<RowData> serializer =
        (TypeSerializer<RowData>)
            original
                .getStateKeyType()
                .createSerializer(planner.getExecEnv().getConfig().getSerializerConfig());
    var selector =
        (org.apache.flink.table.runtime.keyselector.RowDataKeySelector)
            original.getStateKeySelector();
    tech.streamfusion.state.StockStateContract contract;
    try {
      contract =
          new tech.streamfusion.state.StockGroupTypedStateContract(
              serializer,
              (RowType) selector.getProducedType().toLogicalType(),
              RowType.of(actualTypes),
              kinds,
              scalars,
              counts,
              recordCount,
              ttl.isEnabled(),
              views,
              global);
    } catch (IllegalArgumentException unsupported) {
      return stock;
    }
    int maximum = stockMaximum(original);
    boolean stringCaches =
        java.util.stream.IntStream.range(0, kinds.length)
            .anyMatch(i -> (kinds[i] == 10 || kinds[i] == 11) && types[i] == 3);
    NativeColumnarExchangeExecNode exchange;
    if (global) {
      RowType stockPartialType = (RowType) local.getOutputType();
      var localInfos =
          org.apache.flink.table.planner.plan.utils.AggregateUtil
              .transformToStreamAggregateInfoList(
                  planner.getTypeFactory(),
                  sourceType,
                  org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(
                      Arrays.asList(calls)),
                  retractions,
                  retract,
                  false,
                  true);
      LogicalType[] localAccumulatorTypes =
          Arrays.stream(localInfos.getAccTypes())
              .map(org.apache.flink.table.types.DataType::getLogicalType)
              .toArray(LogicalType[]::new);
      if (stockPartialType.getFieldCount() != localKeys.length + localAccumulatorTypes.length)
        return stock;
      for (int i = 0; i < localAccumulatorTypes.length; i++)
        if (!stockPartialType
            .getTypeAt(localKeys.length + i)
            .copy(true)
            .equals(localAccumulatorTypes[i].copy(true))) return stock;
      for (int i = 0; i < localKeys.length; i++)
        if (!stockPartialType.getTypeAt(i).equals(sourceType.getTypeAt(localKeys[i]))) return stock;
      if (localKinds.size() + infos.distinctInfos().length != localAccumulatorTypes.length)
        return stock;
      var partialFields = new ArrayList<LogicalType>();
      for (int i = 0; i < localKeys.length + localKinds.size(); i++)
        partialFields.add(stockPartialType.getTypeAt(i));
      for (var type : localViewTypes)
        partialFields.add(
            new org.apache.flink.table.types.logical.ArrayType(
                false,
                RowType.of(
                    new LogicalType[] {type.copy(true), new BigIntType(false)},
                    new String[] {"value", "count"})));
      RowType partialType = RowType.of(partialFields.toArray(LogicalType[]::new));
      for (int i = 0; i < viewColumns.length; i++)
        if (viewColumns[i] >= 0) viewColumns[i] += localKeys.length + localKinds.size();
      inputType = partialType;
      var localConfig = new Configuration(config);
      ((ExecNodeBase<?>) local).getPersistedConfig().toMap().forEach(localConfig::setString);
      localConfig.setString("table.exec.mini-batch.enabled", "true");
      int[] localKindArray = localKinds.stream().mapToInt(Integer::intValue).toArray();
      var localOwner =
          new NativeColumnarLocalGroupAggExecNode(
              localConfig,
              InputProperty.DEFAULT,
              partialType,
              "NativeColumnarLocalGroupAggregate",
              localKindArray,
              localTypes.stream().mapToInt(Integer::intValue).toArray(),
              localColumns.stream().mapToInt(Integer::intValue).toArray(),
              localFilters.stream().mapToInt(Integer::intValue).toArray(),
              localKeys,
              localViewSources.stream().mapToInt(Integer::intValue).toArray(),
              FlinkKeyGroupUtils.stockKeyDescriptor(sourceType, localKeys));
      if (!localViewSources.isEmpty()) localOwner.withStockAppendDistinct();
      var producer = local.getInputEdges().get(0).getSource();
      ExecNode<?> entry;
      if (stringCaches) {
        entry =
            new RowDataToArrowExecNode(
                config,
                InputProperty.DEFAULT,
                sourceType,
                "RowDataToArrow",
                true,
                null,
                false,
                1024,
                false,
                true);
        connect(producer, entry);
      } else entry = arrowInput(producer, config, sourceType, true);
      connect(entry, localOwner);
      bindings.put(localOwner, new StockBinding(local.getId(), stockUids.get(local.getId()), null));
      columnar.add(localOwner);
      exchange =
          new NativeColumnarExchangeExecNode(
              config,
              InputProperty.DEFAULT,
              partialType,
              "NativeColumnarExchange",
              keys,
              FlinkKeyGroupUtils.stockKeyDescriptor(partialType, keys),
              keys.length == 0);
      connect(localOwner, exchange);
    } else
      exchange =
          (NativeColumnarExchangeExecNode)
              canonicalJoinInput(input, config, inputType, keys, true, stringCaches);
    exchange.withStateMaxParallelism(maximum);
    var owner =
        new NativeColumnarGroupAggExecNode(
                config,
                InputProperty.DEFAULT,
                outputType,
                "NativeColumnarGroupAggregate",
                kinds,
                types,
                columns,
                keys,
                filters,
                global
                    ? java.util.stream.IntStream.range(0, kinds.length)
                        .map(i -> kinds[i] == 4 ? columns[i] + 1 : -1)
                        .toArray()
                    : new int[0],
                global ? viewColumns : new int[0],
                -1,
                (Boolean) releasedField(function, "generateUpdateBefore"),
                retention,
                FlinkKeyGroupUtils.stockKeyDescriptor(inputType, keys))
            .withStateMaxParallelism(maximum);
    connect(exchange, owner);
    bindings.put(owner, new StockBinding(stock.getId(), stockUids.get(stock.getId()), contract));
    columnar.add(owner);
    var exit =
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, outputType, "ArrowToRowData");
    connect(owner, exit);
    return exit;
  }

  private ExecNode<?> lowerArithmeticGroupAggregate(ExecNode<?> stock) throws IOException {
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
                i ->
                    (global || i != keys[0])
                        && !tech.streamfusion.state.StockGroupExtremaStateContract
                            .supportsValueType(inputType.getTypeAt(i)))
        || !global && outputType.getTypeAt(0).getTypeRoot() != keyRoot
        || outputType.getChildren().stream()
            .skip(keys.length)
            .anyMatch(
                type ->
                    !tech.streamfusion.state.StockGroupExtremaStateContract.supportsValueType(type))
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
        || recordCount >= 0 && recordCount != (Integer) releasedField(counter, "indexOfCountStar"))
      return stock;
    int size = infos.getActualAggregateInfos().length;
    if (size == 0) return stock;
    int[] kinds = new int[size],
        valueTypes = new int[size],
        columns = new int[size],
        scalars = new int[size],
        counts = new int[size];
    String[] mapNames = new String[size];
    TypeSerializer<?>[] mapKeys = new TypeSerializer<?>[size],
        mapValues = new TypeSerializer<?>[size];
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
      } else if (!retract
          && type
              == org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction
                  .LongMinAggFunction.class) {
        kind = 10;
        fields = 1;
      } else if (!retract
          && type
              == org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction
                  .LongMaxAggFunction.class) {
        kind = 11;
        fields = 1;
      } else if (retract
          && type
              == org.apache.flink.table.runtime.functions.aggregate.MinWithRetractAggFunction
                  .class) {
        kind = 38;
        fields = 1;
      } else if (retract
          && type
              == org.apache.flink.table.runtime.functions.aggregate.MaxWithRetractAggFunction
                  .class) {
        kind = 39;
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
                  || (kind == 38 || kind == 39
                      ? !tech.streamfusion.state.StockGroupExtremaStateContract.supportsValueType(
                          inputType.getTypeAt(info.argIndexes()[0]))
                      : !inputType.getTypeAt(info.argIndexes()[0]).is(LogicalTypeRoot.BIGINT)))))
        return stock;
      if (kind == 38 || kind == 39) {
        if (info.viewSpecs().length != 1
            || !(info.viewSpecs()[0]
                instanceof org.apache.flink.table.runtime.dataview.MapViewSpec)) return stock;
        var view = (org.apache.flink.table.runtime.dataview.MapViewSpec) info.viewSpecs()[0];
        if (view.getFieldIndex() != 2
            || view.containsNullKey()
            || !view.getKeyDataType()
                .getLogicalType()
                .copy(true)
                .equals(inputType.getTypeAt(info.argIndexes()[0]).copy(true))
            || !view.getValueDataType().getLogicalType().is(LogicalTypeRoot.BIGINT)) return stock;
        mapNames[actual] = view.getStateId();
        mapKeys[actual] =
            view.getKeySerializer()
                .orElseGet(
                    () ->
                        org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                            view.getKeyDataType()));
        mapValues[actual] =
            view.getValueSerializer()
                .orElseGet(
                    () ->
                        org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                            view.getValueDataType()));
      } else if (info.viewSpecs().length != 0) return stock;
      kinds[actual] = kind;
      if (kind == 38 || kind == 39)
        valueTypes[actual] = typedGroupValueCode(inputType.getTypeAt(info.argIndexes()[0]));
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
    tech.streamfusion.state.StockStateContract contract;
    try {
      contract =
          Arrays.stream(kinds).anyMatch(kind -> kind == 38 || kind == 39)
              ? new tech.streamfusion.state.StockGroupExtremaStateContract(
                  serializer,
                  (RowType) selector.getProducedType().toLogicalType(),
                  RowType.of(accumulatorTypes),
                  kinds,
                  scalars,
                  counts,
                  mapNames,
                  mapKeys,
                  mapValues,
                  recordCount,
                  ttl.isEnabled())
              : new tech.streamfusion.state.StockGroupArithmeticStateContract(
                  serializer,
                  (RowType) selector.getProducedType().toLogicalType(),
                  RowType.of(accumulatorTypes),
                  kinds,
                  scalars,
                  counts,
                  recordCount,
                  ttl.isEnabled());
    } catch (IllegalArgumentException unsupported) {
      return stock;
    }
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
                valueTypes,
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

  private ExecNode<?> lowerSink(ExecNode<?> stock) throws IOException {
    JsonNode definition = definitions.get(stock.getId());
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec>readValue(
                definition.path("dynamicTableSink"));
    var mode =
        reader
            .forType(org.apache.flink.table.connector.ChangelogMode.class)
            .<org.apache.flink.table.connector.ChangelogMode>readValue(
                definition.path("inputChangelogMode"));
    ExecNode<?> input = stock.getInputEdges().get(0).getSource();
    RowType inputType = (RowType) input.getOutputType();
    var cluster =
        FlinkRelOptClusterFactory.create(
            new VolcanoPlanner(RelOptCostImpl.FACTORY, planner.getFlinkContext()),
            new RexBuilder(planner.getTypeFactory()));
    // This schema-only input is used for admission; execution retains the actual lowered edge.
    var physical =
        new org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink(
            cluster,
            cluster.traitSet(),
            LogicalValues.createEmpty(
                cluster, planner.getTypeFactory().createFieldTypeFromLogicalType(inputType)),
            List.of(),
            spec.getContextResolvedTable(),
            spec.getTableSink(planner.getFlinkContext()),
            spec.getTargetColumns(),
            spec.getSinkAbilities() == null
                ? new org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec[0]
                : spec.getSinkAbilities()
                    .toArray(
                        new org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec[0]),
            definition.path("requireUpsertMaterialize").asBoolean());
    Configuration config = new Configuration(planner.getTableConfig().getConfiguration());
    ((ExecNodeBase<?>) stock).getPersistedConfig().toMap().forEach(config::setString);
    for (var extension :
        ServiceLoader.load(
            NativePlannerExtension.class, planner.getFlinkContext().getClassLoader())) {
      var sink = extension.lowerCompiledSink(physical, config, mode);
      if (sink == null) continue;
      connect(
          arrowInput(
              input, config, inputType, !mode.containsOnly(org.apache.flink.types.RowKind.INSERT)),
          sink);
      bindings.put(sink, new StockBinding(stock.getId(), stockUids.get(stock.getId()), null));
      return sink;
    }
    return stock;
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
    return canonicalJoinInput(input, config, type, keys, changelog, false);
  }

  private ExecNode<?> canonicalJoinInput(
      ExecNode<?> input,
      Configuration config,
      RowType type,
      int[] keys,
      boolean changelog,
      boolean preserveStringCaches) {
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
    ExecNode<?> columnarInput;
    if (preserveStringCaches) {
      columnarInput =
          new RowDataToArrowExecNode(
              config,
              InputProperty.DEFAULT,
              type,
              "RowDataToArrow",
              changelog,
              null,
              false,
              1024,
              false,
              true);
      connect(source, columnarInput);
    } else columnarInput = arrowInput(source, config, type, changelog);
    connect(columnarInput, exchange);
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
          if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink
              || ServiceLoader.load(
                      NativePlannerExtension.class, planner.getFlinkContext().getClassLoader())
                  .stream()
                  .map(ServiceLoader.Provider::get)
                  .anyMatch(extension -> extension.preservesStockSinkState(node)))
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
