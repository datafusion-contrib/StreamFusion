package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.example.canonical.CanonicalJobWorker;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs identical portable plans in isolated engine processes with an independent user connector.
 */
class CanonicalJobRunner {
  @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS)
  Path directory;

  private Class<?> workerClass = CanonicalJobWorker.class;

  CanonicalJobRunner() {}

  CanonicalJobRunner(Path directory) {
    this.directory = directory;
  }

  CanonicalJobRunner(Path directory, Class<?> workerClass) {
    this.directory = directory;
    this.workerClass = workerClass;
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
    run(fixture, nativeRun, root, limit, restore, stage, batchRows, backend, 0);
  }

  void run(
      Path fixture,
      boolean nativeRun,
      Path root,
      int limit,
      String restore,
      String stage,
      int batchRows,
      String backend,
      int expectedExitCode)
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
                  && Files.isDirectory(Path.of(entry).resolve("tech/streamfusion"));
      if (nativeRun || !streamfusion) classpath.add(entry);
    }
    var cache = CanonicalWorkerCache.configured();
    if (cache != null) classpath = cache.classpath(classpath);
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
                workerClass.getName(),
                nativeRun ? "native" : "stock",
                root.toString(),
                Integer.toString(limit),
                restore,
                stage,
                Integer.toString(batchRows),
                backend));
    if (nativeRun) command.add(2, "-Djava.library.path=" + System.getProperty("java.library.path"));
    Path log = root.resolve(stage + ".log");
    try (var archive = cache == null ? null : cache.archive(command)) {
      Process process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(log.toFile())
              .start();
      try {
        boolean completed =
            process.waitFor(Duration.ofMinutes(4).toMillis(), TimeUnit.MILLISECONDS);
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
            expectedExitCode,
            process.exitValue(),
            () -> {
              try {
                return Files.readString(log);
              } catch (Exception failure) {
                return failure.toString();
              }
            });
        if (archive != null && expectedExitCode == 0) archive.trained();
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
      }
    }
  }

  void verifyJoinSwitch(
      Path baseline, Path switched, boolean nativeFirst, String backend, String mode, String family)
      throws Exception {
    var fixture = fixtureJar();
    if (Files.exists(baseline.resolve("same-cut-stock-reference")))
      runStockAtSavepointCuts(fixture, baseline, mode);
    else run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), family);
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) Files.createFile(switched.resolve("prefix-native-checkpoint"));
    run(
        fixture,
        nativeFirst,
        switched,
        mode.equals("rescale") ? 128 : 4,
        "none",
        "prefix",
        3,
        nativeFirst ? backend : "hashmap");
    String restore = "prefix";
    if (mode.equals("checkpoint")) {
      run(fixture, true, switched, 4, restore, "canonical", 3, backend);
      restore = "canonical";
    } else if (mode.equals("no-input")) {
      run(fixture, !nativeFirst, switched, 4, restore, "no-input", 3, backend);
      run(fixture, nativeFirst, switched, 4, "no-input", "return", 3, backend);
      restore = "return";
    }
    run(
        fixture,
        !nativeFirst,
        switched,
        mode.equals("rescale") ? 256 : 8,
        restore,
        "middle",
        3,
        nativeFirst ? "hashmap" : backend);
    run(
        fixture,
        nativeFirst,
        switched,
        Integer.MAX_VALUE,
        "middle",
        "suffix",
        3,
        nativeFirst ? backend : "hashmap");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var expected =
        CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type).stream()
            .sorted()
            .toArray();
    assertTrue(expected.length > 0);
    assertArrayEquals(
        expected,
        CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type).stream()
            .sorted()
            .toArray());
    for (String stage : List.of("prefix", "middle")) {
      if (mode.equals("checkpoint") && stage.equals("prefix")) continue;
      var savepoint =
          Path.of(
              java.net.URI.create(Files.readString(switched.resolve(stage + "-savepoint")).trim()));
      var metadata =
          new String(
              Files.readAllBytes(savepoint.resolve("_metadata")),
              java.nio.charset.StandardCharsets.ISO_8859_1);
      assertFalse(metadata.contains("tech.streamfusion"));
    }
    assertTrue(
        Files.exists(
            switched.resolve((nativeFirst ? "middle" : "suffix") + "-stock-classpath-verified")));
  }

  private void runStockAtSavepointCuts(Path fixture, Path root, String mode) throws Exception {
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("rescale"));
      Files.writeString(root.resolve("prefix-parallelism"), "1");
      Files.writeString(root.resolve("middle-parallelism"), "3");
      Files.writeString(root.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) Files.createFile(root.resolve("prefix-native-checkpoint"));
    run(fixture, false, root, mode.equals("rescale") ? 128 : 4, "none", "prefix", 3, "hashmap");
    String restore = "prefix";
    if (mode.equals("checkpoint")) {
      run(fixture, false, root, 4, restore, "canonical", 3, "hashmap");
      restore = "canonical";
    } else if (mode.equals("no-input")) {
      run(fixture, false, root, 4, restore, "no-input", 3, "hashmap");
      run(fixture, false, root, 4, "no-input", "return", 3, "hashmap");
      restore = "return";
    }
    run(fixture, false, root, mode.equals("rescale") ? 256 : 8, restore, "middle", 3, "hashmap");
    run(fixture, false, root, Integer.MAX_VALUE, "middle", "suffix", 3, "hashmap");
  }

  Path fixtureJar() throws Exception {
    Path jar = directory.resolve("user-canonical-fixtures.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
      Set<Class<?>> classes = new java.util.LinkedHashSet<>();
      collect(CanonicalFixtureFactory.class, classes);
      collect(CanonicalJobWorker.class, classes);
      collect(workerClass, classes);
      for (Class<?> type :
          classes.stream()
              .sorted(java.util.Comparator.comparing(Class::getName))
              .collect(java.util.stream.Collectors.toList())) {
        String resource = type.getName().replace('.', '/') + ".class";
        output.putNextEntry(entry(resource));
        try (var input = type.getClassLoader().getResourceAsStream(resource)) {
          input.transferTo(output);
        }
        output.closeEntry();
      }
      output.putNextEntry(entry("META-INF/services/org.apache.flink.table.factories.Factory"));
      output.write(
          (CanonicalFixtureFactory.class.getName() + "\n")
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      output.closeEntry();
      output.putNextEntry(
          entry("META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory"));
      output.write(
          (classes.stream()
                  .filter(
                      org.apache.flink.metrics.reporter.MetricReporterFactory.class
                          ::isAssignableFrom)
                  .map(Class::getName)
                  .sorted()
                  .collect(java.util.stream.Collectors.joining("\n", "", "\n")))
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      output.closeEntry();
    }
    var cache = CanonicalWorkerCache.configured();
    return cache == null ? jar : cache.fixture(jar);
  }

  private static JarEntry entry(String name) {
    var entry = new JarEntry(name);
    entry.setTime(0);
    return entry;
  }

  private static void collect(Class<?> type, Set<Class<?>> classes) {
    classes.add(type);
    for (Class<?> child : type.getDeclaredClasses()) collect(child, classes);
  }
}
