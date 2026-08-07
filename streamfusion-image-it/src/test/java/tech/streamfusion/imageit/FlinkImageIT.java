package tech.streamfusion.imageit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.DockerImageName;

/** Runs a normal Session-cluster submission against the locally built Flink image. */
class FlinkImageIT {

  private static final String JOB_CLASS = NativeSqlSmokeJob.class.getName();
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
  private static final String FLINK_PROPERTIES =
      "jobmanager.rpc.address: jobmanager\n"
          + "jobmanager.memory.process.size: 1024m\n"
          + "taskmanager.memory.process.size: 2048m\n"
          + "taskmanager.memory.task.off-heap.size: 256m\n"
          + "state.backend.type: tech.streamfusion.state.RocksDBNativeStateBackendFactory\n"
          + "taskmanager.numberOfTaskSlots: 1";

  @Test
  void baseImageRunsAUserSqlJobThroughTheNativePlannerAndRuntime() throws Exception {
    Path jobJar = Path.of(requiredProperty("streamfusion.image.job.jar"));
    assertTrue(Files.isRegularFile(jobJar), "missing user job JAR: " + jobJar);

    DockerImageName image = DockerImageName.parse(requiredProperty("streamfusion.image.name"));
    try (Network network = Network.newNetwork();
        GenericContainer<?> jobManager = jobManager(image, network, jobJar);
        GenericContainer<?> taskManager = taskManager(image, network)) {
      jobManager.start();
      taskManager.start();
      awaitRegisteredTaskManager(jobManager);

      GenericContainer.ExecResult submission =
          jobManager.execInContainer(
              "/opt/flink/bin/flink",
              "run",
              "-m",
              "localhost:8081",
              "-c",
              JOB_CLASS,
              "/tmp/streamfusion-image-it-job.jar");

      assertEquals(
          0,
          submission.getExitCode(),
          () -> "Flink submission failed:\n" + submission.getStdout() + submission.getStderr());
      assertTrue(
          submission.getStdout().contains("StreamFusion native SQL image smoke test passed"),
          () -> "The job did not prove native planning and execution:\n" + submission.getStdout());
    }
  }

  private static GenericContainer<?> jobManager(
      DockerImageName image, Network network, Path jobJar) {
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withNetworkAliases("jobmanager")
        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
        .withCopyFileToContainer(
            MountableFile.forHostPath(jobJar), "/tmp/streamfusion-image-it-job.jar")
        .withCommand("jobmanager")
        .withExposedPorts(8081)
        .waitingFor(Wait.forHttp("/overview").forPort(8081).withStartupTimeout(STARTUP_TIMEOUT));
  }

  private static GenericContainer<?> taskManager(DockerImageName image, Network network) {
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
        .withCommand("taskmanager")
        .waitingFor(
            Wait.forLogMessage(".*Starting Task Manager.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
  }

  private static void awaitRegisteredTaskManager(GenericContainer<?> jobManager) throws Exception {
    HttpClient client = HttpClient.newHttpClient();
    URI taskManagers =
        URI.create(
            "http://"
                + jobManager.getHost()
                + ":"
                + jobManager.getMappedPort(8081)
                + "/taskmanagers");
    long deadlineNanos = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
    String lastResponse = "no response";
    while (System.nanoTime() < deadlineNanos) {
      try {
        HttpResponse<String> response =
            client.send(
                HttpRequest.newBuilder(taskManagers).GET().build(), HttpResponse.BodyHandlers.ofString());
        lastResponse = response.body();
        if (response.statusCode() == 200 && !lastResponse.contains("\"taskmanagers\":[]")) {
          return;
        }
      } catch (Exception ignored) {
        // The JobManager's REST endpoint can briefly restart while the TaskManager connects.
      }
      Thread.sleep(500);
    }
    throw new AssertionError(
        "TaskManager did not register. REST response: "
            + lastResponse
            + "\nJobManager logs:\n"
            + jobManager.getLogs());
  }

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required system property: " + name);
    }
    return value;
  }
}
