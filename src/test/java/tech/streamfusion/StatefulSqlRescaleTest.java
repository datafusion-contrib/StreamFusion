package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ListCollectors;

@org.junit.jupiter.api.extension.ExtendWith(tech.streamfusion.operator.CoalescingOff.class)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class StatefulSqlRescaleTest {
  private static final Map<String, Object> RESULTS = new TreeMap<>();

  @ParameterizedTest(name = "rescale-rocks={0}")
  @ValueSource(booleans = {false, true})
  void groupedStateAcrossBothDirections(boolean rocks, @TempDir Path directory) throws Exception {
    List<Row> input = new ArrayList<>();
    String text = "long-duplicate-é🙂".repeat(128);
    for (int k = 0; k < 128; k++) {
      input.add(Row.ofKind(RowKind.INSERT, k, new BigDecimal("1.25"), text));
      input.add(Row.ofKind(RowKind.INSERT, k, new BigDecimal("1.25"), text));
    }
    for (int k = 0; k < 128; k++) {
      input.add(Row.ofKind(RowKind.DELETE, k, new BigDecimal("1.25"), text));
      input.add(Row.ofKind(RowKind.INSERT, k, new BigDecimal("-0.50"), "other"));
    }
    for (int k = 0; k < 128; k++) {
      input.add(Row.ofKind(RowKind.DELETE, k, new BigDecimal("1.25"), text));
      input.add(Row.ofKind(RowKind.INSERT, k, null, "other"));
    }
    Map<String, Long> expected = new TreeMap<>();
    for (int k = 0; k < 128; k++) expected.put(k + "|-0.50|1", 1L);
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    Map<String, Object> report = new java.util.LinkedHashMap<>();
    List<Map<String, Object>> stageEvidence = new ArrayList<>();
    report.put("passed", false);
    report.put("backend", backend);
    report.put("parallelisms", List.of(1, 3, 2));
    report.put("maxParallelism", 128);
    report.put("sourceOffsets", List.of(256, 512, 768));
    report.put("physicalBatchRows", 5);
    report.put("logicalMiniBatchRows", 3);
    report.put("stages", stageEvidence);
    RESULTS.put(rocks ? "rocksdb" : "memory", report);
    try {
      var reference =
          runStage(
              backend,
              input,
              1,
              directory.resolve("host-checkpoints"),
              directory.resolve("host-output"),
              null,
              false,
              expected);
      report.put("reference", reference.evidence());
      List<Stage> stages = new ArrayList<>();
      Path restore = null;
      int[] parallelisms = {1, 3, 2};
      for (int i = 0; i < 3; i++) {
        var stage =
            runStage(
                backend,
                input.subList(0, (i + 1) * 256),
                parallelisms[i],
                directory.resolve("native-checkpoints-" + i),
                directory.resolve("native-output"),
                restore,
                true,
                i == 2 ? expected : null);
        stages.add(stage);
        stageEvidence.add(stage.evidence());
        restore = stage.checkpoint();
        assertEquals(
            i == 0 ? List.of() : List.of(i * 256), stage.recovery().get(0).get("restoredOffsets"));
        assertEquals(
            stages.get(0).operatorIds(), stage.operatorIds(), "operator identities changed");
        assertEquals(reference.evidence().get("resultTypes"), stage.evidence().get("resultTypes"));
      }
      var actual = materialized(directory.resolve("native-output"));
      assertEquals(expected, materialized(directory.resolve("host-output")));
      assertEquals(expected, actual);
      report.put("materializedResultsEqual", true);
      report.put("passed", true);
    } catch (Exception | AssertionError failure) {
      report.put("failure", failure.toString());
      throw failure;
    }
  }

  private static final class Stage {
    private final Path checkpoint;
    private final List<Map<String, Object>> recovery;
    private final List<String> operatorIds;
    private final Map<String, Object> evidence;

    private Stage(
        Path checkpoint,
        List<Map<String, Object>> recovery,
        List<String> operatorIds,
        Map<String, Object> evidence) {
      this.checkpoint = checkpoint;
      this.recovery = recovery;
      this.operatorIds = operatorIds;
      this.evidence = evidence;
    }

    public Path checkpoint() {
      return checkpoint;
    }

    public List<Map<String, Object>> recovery() {
      return recovery;
    }

    public List<String> operatorIds() {
      return operatorIds;
    }

    public Map<String, Object> evidence() {
      return evidence;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Stage that = (Stage) other;
      return java.util.Objects.equals(checkpoint, that.checkpoint)
          && java.util.Objects.equals(recovery, that.recovery)
          && java.util.Objects.equals(operatorIds, that.operatorIds)
          && java.util.Objects.equals(evidence, that.evidence);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(checkpoint);
      result = 31 * result + java.util.Objects.hashCode(recovery);
      result = 31 * result + java.util.Objects.hashCode(operatorIds);
      result = 31 * result + java.util.Objects.hashCode(evidence);
      return result;
    }

    @Override
    public String toString() {
      return "Stage[checkpoint="
          + checkpoint
          + ", recovery="
          + recovery
          + ", operatorIds="
          + operatorIds
          + ", evidence="
          + evidence
          + "]";
    }
  }

  private static Stage runStage(
      String backend,
      List<Row> input,
      int parallelism,
      Path checkpoints,
      Path output,
      Path restore,
      boolean nativeRun,
      Map<String, Long> expected)
      throws Exception {
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "amount", "text_value"}, Types.INT, Types.BIG_DEC, Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, true)
            .withExecutionMetrics()
            .holdAfterInput()
            .withRescaleDeployment(parallelism, checkpoints, restore)) {
      var table = (StreamTableEnvironment) recovery.uninterrupted();
      table.getConfig().set("streamfusion.transpose.batchRows", "5");
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", "3");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
      if (nativeRun) tech.streamfusion.planner.NativePlanner.install(table);
      var query =
          table.sqlQuery(
              "SELECT k, SUM(amount), COUNT(DISTINCT text_value) FROM recovery_input GROUP BY k");
      String plan = query.explain();
      if (nativeRun) assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      var rows = table.toChangelogStream(query);
      int index = 0;
      for (var transformation : rows.getTransformation().getTransitivePredecessors()) {
        if (!"portable-recovery-source".equals(transformation.getUid()))
          transformation.setUid("rescale-sql-" + index);
        index++;
      }
      var encoded =
          rows.map(
                  row ->
                      row.getKind().shortString()
                          + "|"
                          + row.getField(0)
                          + "|"
                          + row.getField(1)
                          + "|"
                          + row.getField(2))
              .returns(Types.STRING)
              .uid("rescale-encode");
      tech.streamfusion.compat.CheckpointFileSink.attach(encoded, output, "rescale-output");
      var env = recovery.executionEnvironment();
      var graph = env.getStreamGraph();
      var hashes =
          new org.apache.flink.streaming.api.graph.StreamGraphHasherV2()
              .traverseStreamGraphAndGenerateHashes(graph);
      List<String> ids =
          hashes.values().stream()
              .map(OperatorID::new)
              .map(Object::toString)
              .sorted()
              .collect(ListCollectors.toList());
      List<OperatorID> aggregateIds =
          graph.getStreamNodes().stream()
              .filter(n -> n.getOperatorName().contains("NativeColumnarGroupAggExecNode"))
              .map(n -> new OperatorID(hashes.get(n.getId())))
              .collect(ListCollectors.toList());
      if (nativeRun) assertEquals(1, aggregateIds.size(), graph.getStreamNodes().toString());
      var client = env.executeAsync(graph);
      try {
        recovery.awaitCompletedInput();
        if (nativeRun)
          RecoveryExecutionMetrics.assertConsumed(
              recovery.nativeExecution(), "NativeColumnarGroupAggregate");
        else assertTrue(recovery.nativeExecution().isEmpty());
        if (expected != null) {
          long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
          while (!expected.equals(materialized(output)) && System.nanoTime() < deadline)
            Thread.sleep(10);
          assertEquals(expected, materialized(output), "committed result files");
        }
      } finally {
        client.cancel().get(30, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        var status = client.getJobStatus().get(10, TimeUnit.SECONDS);
        while (!status.isGloballyTerminalState() && System.nanoTime() < deadline) {
          Thread.sleep(10);
          status = client.getJobStatus().get(10, TimeUnit.SECONDS);
        }
        assertEquals(org.apache.flink.api.common.JobStatus.CANCELED, status);
      }
      assertEquals(0, recovery.observations().get(0).get("activeSources"));
      SharedFlinkCluster.assertNativeMemoryReleased("rescale-stage-" + parallelism);
      Path checkpoint;
      try (var paths = Files.walk(checkpoints)) {
        checkpoint =
            paths
                .filter(p -> p.getFileName().toString().equals("_metadata"))
                .map(Path::getParent)
                .max(
                    java.util.Comparator.comparingLong(
                        p -> Long.parseLong(p.getFileName().toString().substring(4))))
                .orElseThrow();
      }
      var metadata =
          org.apache.flink.test.util.TestUtils.loadCheckpointMetadata(checkpoint.toString());
      List<String> ranges = new ArrayList<>();
      if (nativeRun) {
        var aggregate =
            metadata.getOperatorStates().stream()
                .filter(s -> aggregateIds.contains(s.getOperatorID()))
                .findFirst()
                .orElseThrow();
        assertEquals(parallelism, aggregate.getParallelism());
        assertEquals(128, aggregate.getMaxParallelism());
        assertEquals(parallelism, aggregate.getSubtaskStates().size());
        for (var entry : aggregate.getSubtaskStates().entrySet()) {
          var state = entry.getValue();
          assertFalse(
              state.getManagedKeyedState().isEmpty(), "aggregate has no checkpointed keyed state");
          var expectedRange =
              org.apache.flink.runtime.state.KeyGroupRangeAssignment
                  .computeKeyGroupRangeForOperatorIndex(128, parallelism, entry.getKey());
          var owned = new java.util.TreeSet<Integer>();
          for (var handle : state.getManagedKeyedState()) {
            assertTrue(handle.getStateSize() > 0, "empty native state handle");
            handle.getKeyGroupRange().forEach(owned::add);
            ranges.add(entry.getKey() + ":" + handle.getKeyGroupRange());
          }
          var expectedGroups = new java.util.TreeSet<Integer>();
          expectedRange.forEach(expectedGroups::add);
          assertEquals(expectedGroups, owned, "checkpoint key-group ownership");
        }
      }
      Map<String, Object> evidence =
          Map.of(
              "parallelism",
              parallelism,
              "sourceOffset",
              input.size(),
              "recovery",
              recovery.observations(),
              "operatorIds",
              ids,
              "nativeExecution",
              recovery.nativeExecution(),
              "keyGroupRanges",
              ranges,
              "plan",
              plan,
              "resultTypes",
              query.getResolvedSchema().getColumnDataTypes().stream()
                  .map(Object::toString)
                  .collect(ListCollectors.toList()),
              "cleanup",
              Map.of(
                  "taskReservedBytes",
                  tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes(),
                  "arrowAllocatedBytes",
                  tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory(),
                  "liveNativeHandles",
                  NativeExtensionLoader.liveNativeHandles()));
      return new Stage(checkpoint, recovery.observations(), ids, evidence);
    }
  }

  private static Map<String, Long> materialized(Path directory) throws Exception {
    Map<String, Long> rows = new TreeMap<>();
    if (!Files.exists(directory)) return rows;
    Files.walkFileTree(directory, committedOutputReader(rows));
    rows.values().removeIf(count -> count == 0);
    return rows;
  }

  private static SimpleFileVisitor<Path> committedOutputReader(Map<String, Long> rows) {
    return new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(Path path, BasicFileAttributes attributes)
          throws IOException {
        if (attributes.isRegularFile() && !path.getFileName().toString().startsWith(".")) {
          for (String line : Files.readAllLines(path)) {
            int separator = line.indexOf('|');
            String kind = line.substring(0, separator);
            rows.merge(
                line.substring(separator + 1),
                kind.equals("+I") || kind.equals("+U") ? 1L : -1L,
                Long::sum);
          }
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path path, IOException failure) throws IOException {
        String name = path.getFileName().toString();
        if (failure instanceof NoSuchFileException
            && name.startsWith(".part-")
            && name.contains(".inprogress.")) {
          return FileVisitResult.CONTINUE;
        }
        throw failure;
      }
    };
  }

  @org.junit.jupiter.api.Test
  void outputTraversalOnlyIgnoresMissingInProgressParts(@TempDir Path directory) throws Exception {
    var reader = committedOutputReader(new TreeMap<>());
    Path temporary = directory.resolve(".part-writer-0.inprogress.checkpoint");
    assertEquals(
        FileVisitResult.CONTINUE,
        reader.visitFileFailed(temporary, new NoSuchFileException(temporary.toString())));
    var committedFailure = new NoSuchFileException(directory.resolve("part-writer-0").toString());
    assertSame(
        committedFailure,
        assertThrows(
            IOException.class,
            () -> reader.visitFileFailed(directory.resolve("part-writer-0"), committedFailure)));
    var accessFailure = new java.nio.file.AccessDeniedException(temporary.toString());
    assertSame(
        accessFailure,
        assertThrows(IOException.class, () -> reader.visitFileFailed(temporary, accessFailure)));

    Path bucket = Files.createDirectories(directory.resolve("bucket"));
    Files.writeString(bucket.resolve("part-0"), "+I|value\n+U|other\n");
    Files.writeString(bucket.resolve("part-1"), "-D|value\n+I|kept\n");
    Files.writeString(temporary, "uncommitted bytes must not be read");
    assertEquals(Map.of("other", 1L, "kept", 1L), materialized(directory));
  }

  @org.junit.jupiter.api.AfterAll
  static void writeEvidence() throws Exception {
    Path output = Path.of("target", "sql-audit", "stateful-rescale.json");
    Files.createDirectories(output.getParent());
    new com.fasterxml.jackson.databind.ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(output.toFile(), RESULTS);
  }
}
