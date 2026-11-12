package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalWorkerCacheTest {
  @TempDir Path directory;

  @Test
  void immutableSnapshotsRetainResourcesAndDoNotRewriteAnExistingClasspath() throws Exception {
    Path source = directory.resolve("classes");
    Files.createDirectories(source.resolve("META-INF/services"));
    Files.writeString(source.resolve("Example.class"), "compiled bytes");
    Files.writeString(source.resolve("META-INF/services/Factory"), "provider");
    Path cache = directory.resolve("cache");
    Path jar =
        Path.of(new CanonicalWorkerCache(cache).classpath(List.of(source.toString())).get(0));
    var timestamp = FileTime.fromMillis(1_000);
    Files.setLastModifiedTime(jar, timestamp);
    assertEquals(
        jar.toString(),
        new CanonicalWorkerCache(cache).classpath(List.of(source.toString())).get(0));
    assertEquals(timestamp, Files.getLastModifiedTime(jar));
    try (var archive = new JarFile(jar.toFile())) {
      assertTrue(archive.getJarEntry("META-INF/services/").isDirectory());
      assertEquals(
          "provider",
          new String(
              archive
                  .getInputStream(archive.getJarEntry("META-INF/services/Factory"))
                  .readAllBytes()));
      assertEquals(
          "compiled bytes",
          new String(archive.getInputStream(archive.getJarEntry("Example.class")).readAllBytes()));
    }
  }

  @Test
  void metadataSnapshotsOmitBundledNativeCopiesButPreserveOtherResources() throws Exception {
    Path source = directory.resolve("classes");
    Path resources = source.resolve("tech/streamfusion/native");
    Files.createDirectories(resources.resolve("json"));
    Files.writeString(resources.resolve("libstreamfusion.so"), "core");
    Files.writeString(resources.resolve("json/libstreamfusion_json.dylib"), "json");
    Files.writeString(resources.resolve("streamfusion.dll"), "windows");
    Files.writeString(resources.resolve("metadata.json"), "metadata");
    Files.writeString(source.resolve("Example.class"), "compiled bytes");
    Files.writeString(source.resolve("fixture.so"), "unrelated test resource");
    for (boolean includeNative : List.of(false, true)) {
      var cache =
          new CanonicalWorkerCache(directory.resolve("cache-" + includeNative), includeNative);
      Path snapshot = Path.of(cache.classpath(List.of(source.toString())).get(0));
      try (var jar = new JarFile(snapshot.toFile())) {
        for (String name :
            List.of("libstreamfusion.so", "json/libstreamfusion_json.dylib", "streamfusion.dll")) {
          assertEquals(includeNative, jar.getJarEntry("tech/streamfusion/native/" + name) != null);
        }
        assertNotNull(jar.getJarEntry("Example.class"));
        assertNotNull(jar.getJarEntry("fixture.so"));
        assertNotNull(jar.getJarEntry("tech/streamfusion/native/metadata.json"));
      }
    }
    assertEquals("core", Files.readString(resources.resolve("libstreamfusion.so")));
  }

  @Test
  void changedCompiledBytesProduceADifferentSnapshotForTheNextFork() throws Exception {
    Path source = directory.resolve("classes");
    Files.createDirectories(source);
    Files.writeString(source.resolve("Example.class"), "before");
    Path cache = directory.resolve("cache");
    String before = new CanonicalWorkerCache(cache).classpath(List.of(source.toString())).get(0);
    Files.writeString(source.resolve("Example.class"), "after");
    String after = new CanonicalWorkerCache(cache).classpath(List.of(source.toString())).get(0);
    assertNotEquals(before, after);
    assertTrue(Files.exists(Path.of(before)));
  }

  @Test
  void corruptSnapshotsFailBeforeTheyCanBeUsedAsWorkerCode() throws Exception {
    Path source = directory.resolve("classes");
    Files.createDirectories(source);
    Files.writeString(source.resolve("Example.class"), "expected");
    Path cache = directory.resolve("cache");
    Path jar =
        Path.of(new CanonicalWorkerCache(cache).classpath(List.of(source.toString())).get(0));
    Files.writeString(jar, "corrupt");
    assertThrows(
        java.io.IOException.class,
        () -> new CanonicalWorkerCache(cache).classpath(List.of(source.toString())));
  }

  @Test
  void fixtureStagingRemovesItsTemporaryCopyAndKeepsTheRetainedJarStable() throws Exception {
    Path source = directory.resolve("fixture.jar");
    Files.writeString(source, "fixture bytes");
    var cache = new CanonicalWorkerCache(directory.resolve("cache"));
    Path retained = cache.fixture(source);
    assertFalse(Files.exists(source));
    Files.writeString(source, "fixture bytes");
    assertEquals(retained, cache.fixture(source));
    assertEquals("fixture bytes", Files.readString(retained));
  }

  @Test
  void failedLockAcquisitionPreservesAnotherWorkersClassList() throws Exception {
    Path base = directory.resolve("archive");
    Path classes = Path.of(base + ".classes");
    try (var owner =
        new CanonicalWorkerCache.Archive(
            base, new java.util.ArrayList<>(), new java.util.ArrayList<>(List.of("java")))) {
      Files.writeString(classes, "owned training data");
      assertThrows(
          java.nio.channels.OverlappingFileLockException.class,
          () ->
              new CanonicalWorkerCache.Archive(
                  base, new java.util.ArrayList<>(), new java.util.ArrayList<>(List.of("java"))));
      assertEquals("owned training data", Files.readString(classes));
    }
    assertFalse(Files.exists(classes));
  }

  @Test
  void archivesReuseMetadataAcrossStageDirectoriesWhileKeepingRuntimePathsAndRoles()
      throws Exception {
    Path source = directory.resolve("source");
    Files.createDirectories(source);
    Path probe = source.resolve("CacheProbe.java");
    Files.writeString(
        probe,
        "import java.nio.file.*; public class CacheProbe {public static void main(String[] args)"
            + " throws Exception {Files.writeString(Path.of(args[1]),"
            + " Files.createTempFile(\"probe\", \".tmp\").getParent().toString());}}");
    var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
    assertNotNull(compiler);
    assertEquals(0, compiler.run(null, null, null, "-d", source.toString(), probe.toString()));
    Files.delete(probe);
    var cache = new CanonicalWorkerCache(directory.resolve("metadata"));
    String classpath = cache.classpath(List.of(source.toString())).get(0);
    for (String stage : List.of("prefix", "suffix")) {
      Path temporary = directory.resolve(stage);
      Files.createDirectories(temporary);
      Path output = directory.resolve(stage + ".txt");
      var command = probeCommand(classpath, temporary, "stock", output);
      try (var archive = cache.archive(command)) {
        assertEquals(stage.equals("suffix"), command.contains("-Xshare:on"));
        Path log = directory.resolve(stage + ".log");
        var process =
            new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        try {
          assertTrue(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
          assertEquals(
              0,
              process.exitValue(),
              () -> {
                try {
                  return Files.readString(log);
                } catch (java.io.IOException failure) {
                  return failure.toString();
                }
              });
          archive.trained();
        } finally {
          if (process.isAlive()) {
            process.destroyForcibly();
            process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
          }
        }
      }
      assertEquals(temporary.toString(), Files.readString(output));
    }
    try (var files = Files.list(directory.resolve("metadata"))) {
      assertEquals(1, files.filter(path -> path.toString().endsWith(".jsa")).count());
    }
    for (boolean differentRole : List.of(false, true)) {
      var command =
          probeCommand(
              classpath,
              directory.resolve("third"),
              differentRole ? "native" : "stock",
              directory.resolve("third.txt"));
      if (!differentRole) command.add(1, "-Ddifferent.worker.configuration=true");
      try (var archive = cache.archive(command)) {
        assertFalse(command.contains("-Xshare:on"));
      }
    }
  }

  private static java.util.ArrayList<String> probeCommand(
      String classpath, Path temporary, String role, Path output) {
    return new java.util.ArrayList<>(
        List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx128m",
            "-Djava.io.tmpdir=" + temporary,
            "-cp",
            classpath,
            "CacheProbe",
            role,
            output.toString()));
  }
}
