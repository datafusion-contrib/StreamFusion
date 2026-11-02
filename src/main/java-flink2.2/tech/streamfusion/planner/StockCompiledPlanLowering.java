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
    lowered.put(node, result);
    return result;
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
    if (stockUids.get(stock.getId()) == null) return stock;
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
    if (stockUids.get(stock.getId()) == null) return stock;
    var spec =
        reader
            .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec.class)
            .<org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec>readValue(
                definitions.get(stock.getId()).path("intervalJoinSpec"));
    var bounds = spec.getWindowBounds();
    if (bounds.getLeftUpperBound() - bounds.getLeftLowerBound() < 0) {
      recordFallback("interval join: negative bounds retain stock stateless filters");
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
    if (stockUids.get(stock.getId()) == null) return stock;
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
          if (binding.uid != null) transformation.setUid(binding.uid);
          if (binding.contract == null) return;
          var factory =
              transformation instanceof OneInputTransformation
                  ? ((OneInputTransformation<?, ?>) transformation).getOperatorFactory()
                  : ((org.apache.flink.streaming.api.transformations.TwoInputTransformation<
                              ?, ?, ?>)
                          transformation)
                      .getOperatorFactory();
          var operator =
              (AbstractNativeStatefulOperator<?>)
                  ((SimpleOperatorFactory<?>) factory).getOperator();
          operator.setStockStateContract(binding.contract);
          String operatorId =
              new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(binding.uid)).toString();
          CanonicalStateBackend.install(
              planner.getExecEnv(),
              planner.getTableConfig().getConfiguration(),
              operatorId,
              operator.getClass().getSimpleName(),
              binding.contract,
              planner.getFlinkContext().getClassLoader());
        });
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
