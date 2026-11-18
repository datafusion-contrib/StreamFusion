package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkTestContexts;

@DisabledIfSystemProperty(named = "streamfusion.experimental.nestedScalarUdfs", matches = "false")
class FlinkNestedScalarRemoteDeploymentTest {
  @TempDir Path temporary;

  @ParameterizedTest
  @CsvSource({"1,false", "3,false", "1,true", "3,true"})
  @SuppressWarnings("unchecked")
  void callbacksAndNestedKindsExecuteInAnotherJvm(int parallelism, boolean binary)
      throws Exception {
    runCallbacks(parallelism, binary, null);
  }

  @ParameterizedTest
  @CsvSource({"1,map", "3,map", "1,map_map", "3,map_map"})
  void mapCallbacksExecuteInAnotherJvm(int parallelism, String mapType) throws Exception {
    runCallbacks(parallelism, false, mapType);
  }

  @SuppressWarnings("unchecked")
  private void runCallbacks(int parallelism, boolean binary, String mapType) throws Exception {
    var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens=") || argument.startsWith("--add-exports="))
        command.add(argument);
    }
    command.addAll(
        List.of(
            "-Xmx2g",
            "-Darrow.enable_unsafe_memory_access=true",
            "-Darrow.enable_null_check_for_get=false",
            "-Dstreamfusion.exchange.zeroCopyLocal=false",
            "-Dstreamfusion.experimental.binaryElt="
                + tech.streamfusion.arrow.ArrowConversion.binaryEltEnabled(),
            "-Djava.library.path=" + System.getProperty("java.library.path"),
            "-cp",
            System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
            FlinkNestedScalarRemoteDeploymentTest.class.getName(),
            temporary.toString()));
    Path log = temporary.resolve("task-jvm.log");
    Process server =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      Path portFile = temporary.resolve("port");
      long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (!Files.exists(portFile) && server.isAlive() && System.nanoTime() < deadline)
        Thread.sleep(20);
      assertTrue(Files.exists(portFile), () -> "task JVM did not start: " + readTail(log));
      int port = Integer.parseInt(Files.readString(portFile));
      FlinkTestContexts.clearForRemoteSubmission();
      var runs =
          NativeFailureParity.run(
              () -> environment(port, parallelism, binary, mapType),
              mapType == null
                  ? "SELECT id, nested_identity(r), nested_reuse(r) FROM src"
                  : "SELECT id,map_udf(p),map_udf(p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(5003, runs.host().rows().size());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      Files.writeString(temporary.resolve("stop"), "stop");
      assertTrue(server.waitFor(30, TimeUnit.SECONDS), "task JVM did not stop");
      assertEquals(0, server.exitValue(), () -> readTail(log));
      try (var input = new ObjectInputStream(Files.newInputStream(temporary.resolve("proof")))) {
        Map<String, Object> proof = (Map<String, Object>) input.readObject();
        assertEquals(server.pid(), proof.get("pid"));
        assertNotEquals(ProcessHandle.current().pid(), proof.get("pid"));
        assertEquals(true, proof.get("nestedFlag"));
        assertEquals(
            tech.streamfusion.arrow.ArrowConversion.binaryEltEnabled(), proof.get("binaryEltFlag"));
        var operators = (List<Map<String, Object>>) proof.get("operators");
        RecoveryExecutionMetrics.assertConsumed(operators, "NativeCalc");
        assertEquals(
            5003,
            operators.stream()
                .filter(operator -> ((String) operator.get("operator")).startsWith("NativeCalc"))
                .mapToLong(operator -> ((Number) operator.get("recordsIn")).longValue())
                .sum());
        System.out.printf(
            "[nested-remote-proof] plannerPid=%d taskPid=%s parallelism=%d binary=%s rows=5003"
                + " operators=%s%n",
            ProcessHandle.current().pid(), proof.get("pid"), parallelism, binary, operators);
      }
    } finally {
      Files.writeString(temporary.resolve("stop"), "stop");
      if (!server.waitFor(30, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
    }
  }

  private static String readTail(Path log) {
    try {
      String text = Files.readString(log);
      return text.substring(Math.max(0, text.length() - 8000));
    } catch (java.io.IOException error) {
      return error.toString();
    }
  }

  private static TableEnvironment environment(
      int port, int parallelism, boolean binary, String mapType) {
    var config = new Configuration();
    config.setString("restart-strategy.type", "none");
    var env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", port, config);
    env.setParallelism(parallelism);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    if (mapType != null) {
      var type = FlinkMapScalarUdfSqlHarnessTest.rowType(mapType);
      table.createTemporaryView(
          "src",
          env.fromSequence(0, 5002)
              .map(
                  i ->
                      Row.of(
                          i,
                          (int) (i % 4),
                          i,
                          FlinkMapScalarUdfSqlHarnessTest.value(mapType, i.intValue())))
              .returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      return FlinkMapScalarUdfSqlHarnessTest.register(table, mapType);
    }
    if (binary) {
      var type =
          org.apache.flink.table.api.DataTypes.ROW(
              org.apache.flink.table.api.DataTypes.FIELD(
                  "id", org.apache.flink.table.api.DataTypes.INT()),
              org.apache.flink.table.api.DataTypes.FIELD("r", NestedBinaryFixtures.type()));
      table.createTemporaryView(
          "src",
          env.fromSequence(0, 5002)
              .map(i -> Row.of(i.intValue(), NestedBinaryFixtures.value(i)))
              .returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      NestedBinaryFixtures.register(table);
      return table;
    }

    var nestedType =
        Types.ROW_NAMED(
            new String[] {"value", "tags"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING));
    table.createTemporaryView(
        "src",
        env.fromSequence(0, 5002)
            .map(
                i ->
                    Row.of(
                        i.intValue(),
                        i % 11 == 0
                            ? null
                            : Row.ofKind(
                                RowKind.values()[(int) (i % 4)],
                                i % 5 == 0 ? null : i,
                                i % 7 == 0 ? null : new String[] {"é中😀-" + i, null, "tail-" + i})))
            .returns(Types.ROW_NAMED(new String[] {"id", "r"}, Types.INT, nestedType)));
    table.createTemporarySystemFunction("nested_identity", new NestedScalarBenchmark.Identity());
    table.createTemporarySystemFunction(
        "nested_reuse", new FlinkNestedScalarSqlHarnessTest.Reusable());
    return table;
  }

  public static void main(String[] arguments) throws Exception {
    Path directory = Path.of(arguments[0]);
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var config = reporter.addToConfiguration(new Configuration());
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("48g"));
    config.setString("rest.bind-address", "127.0.0.1");
    config.setString("rest.bind-port", "0");
    var cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(4)
                .build());
    try {
      cluster.start();
      Files.writeString(
          directory.resolve("port.pending"),
          Integer.toString(cluster.getRestAddress().get().getPort()));
      Files.move(directory.resolve("port.pending"), directory.resolve("port"));
      while (!Files.exists(directory.resolve("stop"))) Thread.sleep(20);
      var operators = new ArrayList<Map<String, Object>>();
      reporter
          .getMetricsByGroup()
          .forEach(
              (group, metrics) -> {
                var variables = group.getAllVariables();
                String operator = variables.getOrDefault("<operator_name>", "");
                if (operator.startsWith("Native")
                    && metrics.get("numRecordsIn") instanceof Counter) {
                  operators.add(
                      Map.of(
                          "operator",
                          operator,
                          "subtask",
                          variables.getOrDefault("<subtask_index>", ""),
                          "recordsIn",
                          ((Counter) metrics.get("numRecordsIn")).getCount()));
                }
              });
      try (var output = new ObjectOutputStream(Files.newOutputStream(directory.resolve("proof")))) {
        output.writeObject(
            Map.of(
                "pid",
                ProcessHandle.current().pid(),
                "nestedFlag",
                tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled(),
                "binaryEltFlag",
                tech.streamfusion.arrow.ArrowConversion.binaryEltEnabled(),
                "operators",
                operators));
      }
    } finally {
      cluster.closeAsync().get(30, TimeUnit.SECONDS);
      reporter.close();
    }
    SharedFlinkCluster.assertNativeMemoryReleased("nested remote task JVM");
  }
}
