package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.ArrowToRowDataOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;

class CanonicalGraphIdentitiesTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void generatedStockSourceIdsDependOnChainabilityDuringRescale(boolean chaining) {
    String before = generatedSourceHash(1, chaining);
    String rescaled = generatedSourceHash(3, chaining);
    if (chaining) assertNotEquals(before, rescaled);
    else assertEquals(before, rescaled);
  }

  private String generatedSourceHash(int parallelism, boolean chaining) {
    var environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    if (!chaining) environment.disableOperatorChaining();
    var source = environment.fromData(org.apache.flink.api.common.typeinfo.Types.INT, 1);
    source
        .map((MapFunction<Integer, Integer>) value -> value)
        .returns(org.apache.flink.api.common.typeinfo.Types.INT)
        .setParallelism(parallelism)
        .sinkTo(
            FileSink.forRowFormat(
                    new Path(directory.resolve("generated-identity").toUri()),
                    new SimpleStringEncoder<Integer>())
                .build())
        .setParallelism(1);
    var graph = environment.getStreamGraph(false);
    assertNull(graph.getStreamNode(source.getId()).getTransformationUID());
    return hex(
        new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph).get(source.getId()));
  }

  @Test
  void unionInputsAndRepeatedEdgesSurviveHelperElision() {
    for (boolean repeated : List.of(false, true)) {
      var stock = graph(false, 1, true, repeated);
      var runtime = graph(true, 1, true, repeated);
      assertEquals(
          stock.graph.getStreamNodes().size(),
          bind(stock, runtime, Map.of(runtime.groups.get(0), stock.groups.get(0))).size());
    }
  }

  @Test
  void derivesRealPrimaryIdsIncludingExpandedSinkWriterAndCommitter() {
    var stock = graph(false, 1);
    var runtime = graph(true, 1);
    var identities = bind(stock, runtime, Map.of(runtime.groups.get(0), stock.groups.get(0)));
    var stockHashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(stock.graph);
    var runtimeHashes =
        new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(runtime.graph);
    assertEquals(stock.graph.getStreamNodes().size(), identities.size());
    assertTrue(
        stock.graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("Writer")));
    assertTrue(
        stock.graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("Committer")));
    assertEquals(
        hex(stockHashes.get(stock.groups.get(0))),
        identities.get(hex(runtimeHashes.get(runtime.groups.get(0)))));
    assertNotEquals(
        hex(stockHashes.get(stock.groups.get(0))), hex(runtimeHashes.get(runtime.groups.get(0))));
    for (int transpose : runtime.transposes)
      assertFalse(identities.containsKey(hex(runtimeHashes.get(transpose))));
  }

  @Test
  void repeatedConnectorNamesAreDisambiguatedByTheirLogicalOperators() {
    var stock = graph(false, 2);
    var runtime = graph(true, 2);
    var identities =
        bind(
            stock,
            runtime,
            Map.of(
                runtime.groups.get(0),
                stock.groups.get(0),
                runtime.groups.get(1),
                stock.groups.get(1)));
    assertEquals(stock.graph.getStreamNodes().size(), identities.size());
    assertEquals(
        identities,
        bind(
            stock,
            runtime,
            Map.of(
                runtime.groups.get(0),
                stock.groups.get(0),
                runtime.groups.get(1),
                stock.groups.get(1))));
    assertThrows(IllegalArgumentException.class, () -> bind(stock, runtime, Map.of()));
  }

  @Test
  void refusesToOmitAnOperatorThatIsNotATranspose() {
    var stock = graph(false, 1);
    var runtime = graph(true, 1);
    Set<Integer> invalid = new HashSet<>(runtime.transposes);
    invalid.add(runtime.groups.get(0));
    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CanonicalGraphIdentities.bind(
                    stock.graph, runtime.graph, Map.of(), invalid, getClass().getClassLoader()));
    assertTrue(failure.getMessage().contains("stateless representation and exchange"));
  }

  @Test
  void duplicateAndNonphysicalBindingsAreRejected() {
    var stock = graph(false, 2);
    var runtime = graph(true, 2);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            bind(
                stock,
                runtime,
                Map.of(
                    runtime.groups.get(0),
                    stock.groups.get(0),
                    runtime.groups.get(1),
                    stock.groups.get(0))));
    assertThrows(
        IllegalArgumentException.class,
        () -> bind(stock, runtime, Map.of(-1, stock.groups.get(0))));
  }

  @Test
  void sameNameAndFactoryDoNotAuthorizeAnUnboundImplementationReplacement() {
    var stock = graph(false, 1);
    var runtime = graph(false, 1);
    runtime
        .graph
        .getStreamNode(runtime.groups.get(0))
        .setOperatorFactory(
            org.apache.flink.streaming.api.operators.SimpleOperatorFactory.of(
                new org.apache.flink.streaming.api.operators.StreamFilter<RowData>(row -> true)));
    assertThrows(IllegalArgumentException.class, () -> bind(stock, runtime, Map.of()));
    assertEquals(
        stock.graph.getStreamNodes().size(),
        bind(stock, runtime, Map.of(runtime.groups.get(0), stock.groups.get(0))).size());
  }

  @Test
  void equivalentGeneratedStockStagesMayHaveDifferentCompiledClassNames() {
    var stock = graph(false, 1);
    var runtime = graph(false, 1);
    var first = generatedMap("StockIdentityMapA");
    var second = generatedMap("StockIdentityMapB");
    assertNotEquals(
        first.getStreamOperatorClass(getClass().getClassLoader()),
        second.getStreamOperatorClass(getClass().getClassLoader()));
    stock.graph.getStreamNode(stock.groups.get(0)).setOperatorFactory(first);
    runtime.graph.getStreamNode(runtime.groups.get(0)).setOperatorFactory(second);
    assertEquals(stock.graph.getStreamNodes().size(), bind(stock, runtime, Map.of()).size());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"equal", "strategy", "fieldIndex", "fieldName", "sameNameDifferentStrategy"})
  void sinkConstraintDefinitionsDetermineRolesInsteadOfDisplayIds(String variant) {
    var stockType =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new BigIntType(false), new BigIntType()
            },
            new String[] {"a", "b"});
    var runtimeType =
        variant.equals("fieldIndex")
            ? RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {
                  new BigIntType(), new BigIntType(false)
                },
                new String[] {"a", "b"})
            : variant.equals("fieldName")
                ? RowType.of(
                    new org.apache.flink.table.types.logical.LogicalType[] {
                      new BigIntType(false), new BigIntType()
                    },
                    new String[] {"other", "b"})
                : stockType;
    var stock =
        constraintGraph(
            "ConstraintEnforcer[8]",
            stockType,
            org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer.ERROR);
    var runtime =
        constraintGraph(
            variant.equals("sameNameDifferentStrategy")
                ? "ConstraintEnforcer[8]"
                : "ConstraintEnforcer[18]",
            runtimeType,
            variant.contains("trategy")
                ? org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer.DROP
                : org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer.ERROR);
    if (variant.equals("equal"))
      assertEquals(stock.graph.getStreamNodes().size(), bind(stock, runtime, Map.of()).size());
    else assertThrows(IllegalArgumentException.class, () -> bind(stock, runtime, Map.of()));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"equal", "index", "precision", "sameNameDifferentIndex"})
  void sinkTimestampDefinitionsDetermineRolesInsteadOfDisplayIds(String variant) {
    var stock = timestampGraph("StreamRecordTimestampInserter[8]", 0, 3);
    var runtime =
        timestampGraph(
            variant.equals("sameNameDifferentIndex")
                ? "StreamRecordTimestampInserter[8]"
                : "StreamRecordTimestampInserter[18]",
            variant.equals("index") || variant.equals("sameNameDifferentIndex") ? 1 : 0,
            variant.equals("precision") ? 9 : 3);
    if (variant.equals("equal"))
      assertEquals(stock.graph.getStreamNodes().size(), bind(stock, runtime, Map.of()).size());
    else assertThrows(IllegalArgumentException.class, () -> bind(stock, runtime, Map.of()));
  }

  private Graph timestampGraph(String name, int field, int precision) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var type =
        RowType.of(
            new org.apache.flink.table.types.logical.TimestampType(3),
            new org.apache.flink.table.types.logical.TimestampType(3));
    var info = InternalTypeInfo.<RowData>of(type);
    var rows =
        env.fromData(
                info,
                GenericRowData.of(
                    org.apache.flink.table.data.TimestampData.fromEpochMillis(1),
                    org.apache.flink.table.data.TimestampData.fromEpochMillis(2)))
            .name("source")
            .transform(
                name,
                info,
                new org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter(
                    field, precision));
    rows.sinkTo(
            FileSink.forRowFormat(
                    new Path(directory.resolve("timestamp-sink").toUri()),
                    new SimpleStringEncoder<RowData>())
                .build())
        .name("sink");
    return new Graph(env.getStreamGraph(false), List.of(rows.getId()), Set.of());
  }

  private Graph constraintGraph(
      String name,
      RowType type,
      org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer strategy) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var information = InternalTypeInfo.<RowData>of(type);
    var executor =
        org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcerExecutor.create(
                type,
                strategy,
                org.apache.flink.table.api.config.ExecutionConfigOptions.TypeLengthEnforcer.IGNORE,
                org.apache.flink.table.api.config.ExecutionConfigOptions.NestedEnforcer.IGNORE)
            .orElseThrow();
    var rows =
        env.fromData(information, GenericRowData.of(1L, 2L))
            .name("source")
            .transform(
                name,
                information,
                new org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer(
                    executor, "constraints"));
    rows.sinkTo(
            FileSink.forRowFormat(
                    new Path(directory.resolve("constraint-sink").toUri()),
                    new SimpleStringEncoder<RowData>())
                .build())
        .name("sink");
    return new Graph(env.getStreamGraph(false), List.of(rows.getId()), Set.of());
  }

  private static org.apache.flink.table.runtime.operators.CodeGenOperatorFactory<RowData>
      generatedMap(String name) {
    String code =
        "public class "
            + name
            + " extends"
            + " org.apache.flink.streaming.api.operators.AbstractStreamOperator<org.apache.flink.table.data.RowData>"
            + " implements"
            + " org.apache.flink.streaming.api.operators.OneInputStreamOperator<org.apache.flink.table.data.RowData,org.apache.flink.table.data.RowData>"
            + " { public "
            + name
            + "(Object[] refs) {} public void"
            + " processElement(org.apache.flink.streaming.runtime.streamrecord.StreamRecord<org.apache.flink.table.data.RowData>"
            + " row) {output.collect(row);} }";
    return new org.apache.flink.table.runtime.operators.CodeGenOperatorFactory<>(
        new org.apache.flink.table.runtime.generated.GeneratedOperator<>(
            name, code, new Object[0], new org.apache.flink.configuration.Configuration()));
  }

  private Map<String, String> bind(Graph stock, Graph runtime, Map<Integer, Integer> anchors) {
    return CanonicalGraphIdentities.bind(
        stock.graph, runtime.graph, anchors, runtime.transposes, getClass().getClassLoader());
  }

  private static String hex(byte[] bytes) {
    return StringUtils.byteToHexString(bytes);
  }

  private Graph graph(boolean nativeShape, int branches) {
    return graph(nativeShape, branches, false, false);
  }

  private Graph graph(boolean nativeShape, int branches, boolean union, boolean repeated) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    RowType rowType = RowType.of(new BigIntType());
    InternalTypeInfo<RowData> type = InternalTypeInfo.of(rowType);
    List<Integer> groups = new ArrayList<>();
    Set<Integer> transposes = new HashSet<>();
    for (int branch = 0; branch < branches; branch++) {
      org.apache.flink.streaming.api.datastream.DataStream<RowData> source =
          env.fromData(type, GenericRowData.of(1L)).name("user-source");
      if (union)
        source =
            source.union(
                repeated
                    ? source
                    : env.fromData(type, GenericRowData.of(2L)).name("other-user-source"));
      org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<RowData> rows;
      if (nativeShape) {
        var entry =
            source.transform(
                "RowDataToArrow",
                ArrowBatchTypeInformation.INSTANCE,
                new RowDataToArrowOperator(rowType, 3, true, null));
        var group =
            entry.transform(
                "native logical group",
                ArrowBatchTypeInformation.INSTANCE,
                new StreamMap<ArrowBatch, ArrowBatch>(value -> value));
        var exit = group.transform("ArrowToRowData", type, new ArrowToRowDataOperator(rowType));
        transposes.add(entry.getId());
        transposes.add(exit.getId());
        groups.add(group.getId());
        rows = exit;
      } else {
        rows =
            source
                .map((MapFunction<RowData, RowData>) value -> value)
                .returns(type)
                .name("stock logical group");
        groups.add(rows.getId());
      }
      rows.sinkTo(
              FileSink.forRowFormat(
                      new Path(directory.resolve("sink").toUri()),
                      new SimpleStringEncoder<RowData>())
                  .build())
          .name("user-sink");
    }
    return new Graph(env.getStreamGraph(false), groups, transposes);
  }

  private static final class Graph {
    final StreamGraph graph;
    final List<Integer> groups;
    final Set<Integer> transposes;

    Graph(StreamGraph graph, List<Integer> groups, Set<Integer> transposes) {
      this.graph = graph;
      this.groups = groups;
      this.transposes = transposes;
    }
  }
}
