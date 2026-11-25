package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
  @CsvSource({"3,0", "3,2", "3,5", "786,0"})
  void appendRoutingKeepsUpstreamParallelismAndConfiguredWriters(
      int upstreamParallelism, int configuredWriters) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(1);
    var table = StreamTableEnvironment.create(execution);
    var scan = NativePlanner.install(table);
    table.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + directory.toUri() + "')");
    table.useCatalog("p");
    String schema = " (id INT, label STRING, pt STRING) PARTITIONED BY (pt) WITH ";
    table.executeSql(
        "CREATE TABLE input_rows"
            + schema
            + "('bucket'='-1', 'file.format'='parquet', 'scan.parallelism'='"
            + upstreamParallelism
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
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
    StreamNode buckets = node(graph, "native-paimon-bucket-route");
    StreamNode roundRobin = node(graph, "native-paimon-round-robin-route");
    assertEquals(upstreamParallelism, buckets.getParallelism());
    assertEquals(upstreamParallelism, roundRobin.getParallelism());
    assertEquals(1, roundRobin.getInEdges().size());
    var routeInput = roundRobin.getInEdges().get(0);
    assertEquals(buckets.getId(), routeInput.getSourceId());
    assertTrue(routeInput.getPartitioner() instanceof ForwardPartitioner);

    int writers = configuredWriters == 0 ? upstreamParallelism : configuredWriters;
    assertEquals(writers, node(graph, "Strip Channel").getParallelism());
    StreamNode writer =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().startsWith("Writer"))
            .findFirst()
            .orElseThrow();
    assertEquals(writers, writer.getParallelism());
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("NativeSink(")));
  }

  private static StreamNode node(StreamGraph graph, String name) {
    return graph.getStreamNodes().stream()
        .filter(node -> node.getOperatorName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing native routing operator: " + name));
  }
}
