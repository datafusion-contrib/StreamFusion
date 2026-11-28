package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class PaimonAppendRoutingPlanTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "3,0,false", "3,0,true", "3,3,false", "3,3,true",
    "3,2,false", "3,2,true", "3,5,false", "3,5,true",
    "786,0,true", "786,786,false"
  })
  void appendJobGraphMatchesStock(
      int upstreamParallelism, int configuredWriters, boolean partitioned) {
    StreamGraph stock = plan(false, upstreamParallelism, configuredWriters, partitioned);
    StreamGraph nativeGraph = plan(true, upstreamParallelism, configuredWriters, partitioned);
    int writers = configuredWriters == 0 ? upstreamParallelism : configuredWriters;
    assertRouting(nativeGraph, upstreamParallelism, writers);
    List<Integer> expected = jobParallelisms(stock);
    assertEquals(expected, jobParallelisms(nativeGraph), "stock/native JobGraph chains");
    assertEquals(writers == upstreamParallelism ? 2 : 3, expected.size());
  }

  @ParameterizedTest
  @CsvSource({"3,0,true", "3,2,false", "3,5,true", "786,0,true", "786,786,false"})
  void routingIgnoresTheEnvironmentDefault(
      int upstreamParallelism, int configuredWriters, boolean partitioned) {
    StreamGraph graph = plan(true, upstreamParallelism, configuredWriters, partitioned, 1);
    int writers = configuredWriters == 0 ? upstreamParallelism : configuredWriters;
    assertRouting(graph, upstreamParallelism, writers);
    List<Integer> expected = new ArrayList<>(List.of(1, writers));
    if (writers != upstreamParallelism) expected.add(upstreamParallelism);
    Collections.sort(expected);
    assertEquals(expected, jobParallelisms(graph));
  }

  private static void assertRouting(StreamGraph nativeGraph, int upstreamParallelism, int writers) {
    StreamNode writer = writer(nativeGraph);
    assertEquals(writers, writer.getParallelism());
    assertEquals(
        upstreamParallelism, node(nativeGraph, "native-paimon-bucket-route").getParallelism());

    if (writers == upstreamParallelism) {
      assertFalse(hasNode(nativeGraph, "native-paimon-round-robin-route"));
      assertFalse(hasNode(nativeGraph, "Strip Channel"));
      assertEquals(1, writer.getInEdges().size());
      var edge = writer.getInEdges().get(0);
      assertEquals(node(nativeGraph, "native-paimon-bucket-route").getId(), edge.getSourceId());
      assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
    } else {
      StreamNode roundRobin = node(nativeGraph, "native-paimon-round-robin-route");
      assertEquals(upstreamParallelism, roundRobin.getParallelism());
      assertEquals(1, roundRobin.getInEdges().size());
      var edge = roundRobin.getInEdges().get(0);
      assertEquals(node(nativeGraph, "native-paimon-bucket-route").getId(), edge.getSourceId());
      assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
      assertEquals(writers, node(nativeGraph, "Strip Channel").getParallelism());
    }
  }

  private StreamGraph plan(
      boolean accelerated, int parallelism, int configuredWriters, boolean partitioned) {
    // Paimon 2.1's stock row adapter inherits the environment default.
    return plan(accelerated, parallelism, configuredWriters, partitioned, parallelism);
  }

  private StreamGraph plan(
      boolean accelerated,
      int parallelism,
      int configuredWriters,
      boolean partitioned,
      int environmentParallelism) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(environmentParallelism);
    var table = StreamTableEnvironment.create(execution);
    var scan = accelerated ? NativePlanner.install(table) : null;
    table.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='"
            + directory.resolve(accelerated ? "native" : "stock").toUri()
            + "')");
    table.useCatalog("p");
    String schema =
        " (id INT, label STRING, pt STRING)"
            + (partitioned ? " PARTITIONED BY (pt)" : "")
            + " WITH ";
    table.executeSql(
        "CREATE TABLE input_rows"
            + schema
            + "('bucket'='-1', 'file.format'='parquet', 'scan.parallelism'='"
            + parallelism
            + "')");
    table.executeSql(
        "CREATE TABLE output_rows"
            + schema
            + "('bucket'='-1', 'file.format'='parquet', 'write-only'='true'"
            + (configuredWriters == 0 ? "" : ", 'sink.parallelism'='" + configuredWriters + "'")
            + ")");
    table
        .createStatementSet()
        .addInsertSql("INSERT INTO output_rows SELECT * FROM input_rows")
        .attachAsDataStream();
    StreamGraph graph = execution.getStreamGraph();
    if (accelerated) {
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(node -> node.getOperatorName().contains("NativeSink(")));
    }
    return graph;
  }

  private static List<Integer> jobParallelisms(StreamGraph graph) {
    List<Integer> parallelisms = new ArrayList<>();
    for (var vertex : graph.getJobGraph().getVertices()) parallelisms.add(vertex.getParallelism());
    Collections.sort(parallelisms);
    return parallelisms;
  }

  private static StreamNode writer(StreamGraph graph) {
    return graph.getStreamNodes().stream()
        .filter(node -> node.getOperatorName().startsWith("Writer"))
        .findFirst()
        .orElseThrow();
  }

  private static boolean hasNode(StreamGraph graph, String name) {
    return graph.getStreamNodes().stream().anyMatch(node -> node.getOperatorName().equals(name));
  }

  private static StreamNode node(StreamGraph graph, String name) {
    return graph.getStreamNodes().stream()
        .filter(node -> node.getOperatorName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing native routing operator: " + name));
  }
}
