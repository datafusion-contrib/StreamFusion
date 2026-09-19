package tech.streamfusion.imageit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.dockerjava.api.DockerClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** Exercises startup rejection using real packaged JARs with a mismatched line identity. */
class MixedPayloadImageIT {
  @TempDir Path directory;

  @Test
  void matchingPayloadsStartTheJobManagerAfterValidation() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "No Docker-compatible container runtime is available (Docker or configured Podman)");
    DockerImageName image = DockerImageName.parse(System.getProperty("streamfusion.image.name"));
    try (GenericContainer<?> container =
        new GenericContainer<>(image)
            .withEnv("FLINK_PROPERTIES", "jobmanager.memory.process.size: 1024m")
            .withCommand("jobmanager")
            .withExposedPorts(8081)
            .waitingFor(
                Wait.forHttp("/overview")
                    .forPort(8081)
                    .withStartupTimeout(Duration.ofMinutes(1)))) {
      container.start();

      assertTrue(
          container
              .getLogs()
              .contains(
                  "StreamFusion payloads verified for Flink "
                      + System.getProperty("streamfusion.flink.line")),
          container.getLogs());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"loader", "core", "json"})
  void refusesMixedPayloadsBeforeStartingTheJobManager(String module) throws Exception {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "No Docker-compatible container runtime is available (Docker or configured Podman)");
    String line = System.getProperty("streamfusion.flink.line");
    String otherLine = line.equals("1.18") ? "2.2" : "1.18";
    Path mismatched = mismatchedPayload(module, otherLine);
    String destination =
        switch (module) {
          case "loader" -> "00-streamfusion-loader.jar";
          case "core" -> "streamfusion-core.jar";
          default -> "renamed-extension.jar";
        };
    DockerImageName image = DockerImageName.parse(System.getProperty("streamfusion.image.name"));
    try (GenericContainer<?> container =
        new GenericContainer<>(image)
            .withCopyFileToContainer(
                MountableFile.forHostPath(mismatched), "/opt/flink/lib/" + destination)
            .withCommand("jobmanager")
            .withStartupCheckStrategy(
                new ExitedContainerCheck().withTimeout(Duration.ofSeconds(30)))) {
      container.start();

      String logs = container.getLogs();
      var state =
          container
              .getDockerClient()
              .inspectContainerCmd(container.getContainerId())
              .exec()
              .getState();
      assertNotEquals(0L, state.getExitCodeLong(), logs);
      assertTrue(logs.contains("StreamFusion"), logs);
      assertTrue(logs.contains("Flink " + line), logs);
      assertTrue(logs.contains("Flink " + otherLine), logs);
      assertFalse(logs.contains("Starting Job Manager"), logs);
    }
  }

  private Path mismatchedPayload(String module, String otherLine) throws Exception {
    Path root = Path.of(System.getProperty("streamfusion.project.dir"));
    String suffix = System.getProperty("streamfusion.artifact.suffix", "");
    String version = System.getProperty("streamfusion.version");
    String classifier = module.equals("core") ? "-runtime" : "";
    Path original =
        root.resolve("streamfusion-" + module)
            .resolve("target")
            .resolve("streamfusion-" + module + suffix + "-" + version + classifier + ".jar");
    assertTrue(Files.isRegularFile(original), "Missing packaged artifact: " + original);
    Path modified = directory.resolve(module + ".jar");
    try (JarFile source = new JarFile(original.toFile())) {
      Manifest manifest = new Manifest(source.getManifest());
      manifest.getMainAttributes().putValue("StreamFusion-Flink-Line", otherLine);
      manifest
          .getMainAttributes()
          .putValue(
              "StreamFusion-Module",
              "streamfusion-" + module + (otherLine.equals("1.18") ? "-flink1.18" : ""));
      try (var output = new JarOutputStream(Files.newOutputStream(modified), manifest)) {
        var entries = source.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          if (entry.getName().equalsIgnoreCase(JarFile.MANIFEST_NAME)) continue;
          output.putNextEntry(new JarEntry(entry.getName()));
          try (var input = source.getInputStream(entry)) {
            if (entry.getName().endsWith("/streamfusion-loader.properties")) {
              Properties properties = new Properties();
              properties.load(input);
              properties.setProperty("flink.line", otherLine);
              properties.store(output, null);
            } else {
              input.transferTo(output);
            }
          }
          output.closeEntry();
        }
      }
    }
    return modified;
  }

  private static final class ExitedContainerCheck extends StartupCheckStrategy {
    @Override
    public StartupStatus checkStartupState(DockerClient client, String containerId) {
      return "exited".equals(getCurrentState(client, containerId).getStatus())
          ? StartupStatus.SUCCESSFUL
          : StartupStatus.NOT_YET_KNOWN;
    }
  }
}
