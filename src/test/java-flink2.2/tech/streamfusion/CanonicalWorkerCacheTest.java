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
}
