package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.example.canonical.CanonicalFixtureFactory;
import org.example.canonical.CanonicalJobWorker;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs identical portable plans in isolated engine processes with an independent user connector.
 */
class CanonicalJobRunner {
  @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS)
  Path directory;

  CanonicalJobRunner() {}

  CanonicalJobRunner(Path directory) {
    this.directory = directory;
  }

  void run(
      Path fixture,
      boolean nativeRun,
      Path root,
      int limit,
      String restore,
      String stage,
      int batchRows)
      throws Exception {
    run(fixture, nativeRun, root, limit, restore, stage, batchRows, "hashmap");
  }

  void run(
      Path fixture,
      boolean nativeRun,
      Path root,
      int limit,
      String restore,
      String stage,
      int batchRows,
      String backend)
      throws Exception {
    String sourceClasspath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Path tests =
        Path.of(
            CanonicalJobWorker.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    List<String> classpath = new ArrayList<>();
    classpath.add(fixture.toString());
    for (String entry : sourceClasspath.split(java.util.regex.Pattern.quote(File.pathSeparator))) {
      if (Path.of(entry).equals(tests)) continue;
      boolean streamfusion =
          entry.contains(File.separator + "tech" + File.separator + "streamfusion" + File.separator)
              || Path.of(entry).getFileName().toString().startsWith("streamfusion-")
              || Files.isDirectory(Path.of(entry))
                  && Files.exists(Path.of(entry).resolve("tech/streamfusion/Native.class"));
      if (nativeRun || !streamfusion) classpath.add(entry);
    }
    List<String> command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx1g",
                "-Dstreamfusion.native.development=true",
                "-Darrow.enable_unsafe_memory_access=true",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.io=ALL-UNNAMED",
                "--add-opens=java.base/java.net=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp",
                String.join(File.pathSeparator, classpath),
                CanonicalJobWorker.class.getName(),
                nativeRun ? "native" : "stock",
                root.toString(),
                Integer.toString(limit),
                restore,
                stage,
                Integer.toString(batchRows),
                backend));
    Path log = root.resolve(stage + ".log");
    Process process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      boolean completed = process.waitFor(Duration.ofMinutes(4).toMillis(), TimeUnit.MILLISECONDS);
      if (!completed) {
        Path dump = root.resolve(stage + "-threads.txt");
        Process diagnostic =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                    Long.toString(process.pid()),
                    "Thread.print",
                    "-l")
                .redirectErrorStream(true)
                .redirectOutput(dump.toFile())
                .start();
        if (!diagnostic.waitFor(10, TimeUnit.SECONDS)) diagnostic.destroyForcibly();
      }
      assertTrue(completed, () -> "worker did not finish; log: " + log);
      assertEquals(
          0,
          process.exitValue(),
          () -> {
            try {
              return Files.readString(log);
            } catch (Exception failure) {
              return failure.toString();
            }
          });
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  Path fixtureJar() throws Exception {
    Path jar = directory.resolve("user-canonical-fixtures.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
      Set<Class<?>> classes = new java.util.LinkedHashSet<>();
      collect(CanonicalFixtureFactory.class, classes);
      collect(CanonicalJobWorker.class, classes);
      for (Class<?> type : classes) {
        String resource = type.getName().replace('.', '/') + ".class";
        output.putNextEntry(new JarEntry(resource));
        try (var input = type.getClassLoader().getResourceAsStream(resource)) {
          input.transferTo(output);
        }
        output.closeEntry();
      }
      output.putNextEntry(
          new JarEntry("META-INF/services/org.apache.flink.table.factories.Factory"));
      output.write(
          (CanonicalFixtureFactory.class.getName() + "\n")
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      output.closeEntry();
      output.putNextEntry(
          new JarEntry(
              "META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory"));
      output.write(
          (CanonicalJobWorker.EvidenceReporterFactory.class.getName() + "\n")
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      output.closeEntry();
    }
    return jar;
  }

  private static void collect(Class<?> type, Set<Class<?>> classes) {
    classes.add(type);
    for (Class<?> child : type.getDeclaredClasses()) collect(child, classes);
  }
}
