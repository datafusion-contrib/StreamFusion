package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalReferenceCacheTest {
  @TempDir Path directory;

  private Path fixture(String name, String query) throws Exception {
    Path root = Files.createDirectories(directory.resolve(name));
    Files.writeString(root.resolve("query.sql"), query);
    return root;
  }

  private static void output(Path root) throws Exception {
    Files.createDirectories(root.resolve("committed"));
    Files.writeString(root.resolve("committed/rows"), "expected rows");
    Files.writeString(root.resolve("baseline-stock-classpath-verified"), "clean stock classpath");
    Files.writeString(root.resolve("baseline-singleton-verified"), "1");
    Files.writeString(root.resolve("baseline.log"), "diagnostic output");
    Files.createDirectories(root.resolve("savepoints"));
    Files.writeString(root.resolve("savepoints/state"), "restorable state");
  }

  @Test
  void identicalInputsReuseOnlyIndependentOracleOutputsAndTheirProofs() throws Exception {
    Path jar = directory.resolve("fixture.jar");
    Files.writeString(jar, "compiled user fixture");
    var cache = new CanonicalReferenceCache(directory.resolve("cache"));
    Path first = fixture("first", "SELECT k,SUM(v) GROUP BY k");
    cache.reference(jar, first, "continue", () -> output(first));
    Path second = fixture("second", Files.readString(first.resolve("query.sql")));
    cache.reference(
        jar, second, "continue", () -> fail("identical stock oracle was executed again"));
    assertEquals("expected rows", Files.readString(second.resolve("committed/rows")));
    assertEquals("1", Files.readString(second.resolve("baseline-singleton-verified")));
    assertTrue(Files.exists(second.resolve("baseline-stock-classpath-verified")));
    assertFalse(Files.exists(second.resolve("savepoints")));
    assertFalse(Files.exists(second.resolve("baseline.log")));
    Files.writeString(second.resolve("committed/rows"), "changed output in one test");
    Path third = fixture("third", Files.readString(first.resolve("query.sql")));
    cache.reference(jar, third, "continue", () -> fail("cached oracle should remain intact"));
    assertEquals("expected rows", Files.readString(third.resolve("committed/rows")));
  }

  @Test
  void changedQueriesInputsModesAndWorkerBytesRunIndependentStockReferences() throws Exception {
    Path jar = directory.resolve("fixture.jar");
    Files.writeString(jar, "compiled user fixture");
    var cache = new CanonicalReferenceCache(directory.resolve("cache"));
    var calls = new AtomicInteger();
    for (int index = 0; index < 5; index++) {
      Path root = fixture("root-" + index, index == 1 ? "SELECT COUNT(*)" : "SELECT SUM(v)");
      if (index == 2) Files.writeString(root.resolve("input.csv"), "different input");
      if (index == 4) Files.writeString(jar, "different compiled user fixture");
      cache.reference(
          jar,
          root,
          index == 3 ? "rescale" : "continue",
          () -> {
            calls.incrementAndGet();
            output(root);
          });
    }
    assertEquals(5, calls.get());
  }

  @Test
  void failedReferencesNeverPublishPartialRows() throws Exception {
    Path jar = directory.resolve("fixture.jar");
    Files.writeString(jar, "fixture");
    var cache = new CanonicalReferenceCache(directory.resolve("cache"));
    Path first = fixture("first", "SELECT SUM(v)");
    assertThrows(
        IllegalStateException.class,
        () ->
            cache.reference(
                jar,
                first,
                "continue",
                () -> {
                  output(first);
                  throw new IllegalStateException("stock worker failed");
                }));
    Path second = fixture("second", "SELECT SUM(v)");
    var executed = new AtomicInteger();
    cache.reference(
        jar,
        second,
        "continue",
        () -> {
          executed.incrementAndGet();
          output(second);
        });
    assertEquals(1, executed.get());
  }

  @Test
  void corruptedCachedRowsFailRatherThanSilentlyChangingTheOracle() throws Exception {
    Path jar = directory.resolve("fixture.jar");
    Files.writeString(jar, "fixture");
    Path retained = directory.resolve("cache");
    var cache = new CanonicalReferenceCache(retained);
    Path first = fixture("first", "SELECT SUM(v)");
    cache.reference(jar, first, "continue", () -> output(first));
    try (var files = Files.walk(retained)) {
      Path rows = files.filter(path -> path.endsWith("committed/rows")).findFirst().orElseThrow();
      Files.writeString(rows, "corrupt");
    }
    Path second = fixture("second", "SELECT SUM(v)");
    assertThrows(
        java.io.IOException.class,
        () -> cache.reference(jar, second, "continue", () -> fail("corruption must fail")));
  }

  @Test
  void independentParentProcessesPublishOneCompleteReference() throws Exception {
    Path jar = directory.resolve("fixture.jar");
    Files.writeString(jar, "fixture");
    Path first = fixture("first", "SELECT SUM(v)");
    Path second = fixture("second", "SELECT SUM(v)");
    var processes = new java.util.ArrayList<Process>();
    try {
      for (Path root : java.util.List.of(first, second)) {
        Path log = directory.resolve(root.getFileName() + ".log");
        processes.add(
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp",
                    System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path")),
                    Worker.class.getName(),
                    directory.toString(),
                    root.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start());
      }
      for (int index = 0; index < processes.size(); index++) {
        Process process = processes.get(index);
        assertTrue(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(
            0,
            process.exitValue(),
            Files.readString(directory.resolve((index == 0 ? "first" : "second") + ".log")));
      }
      assertEquals("one execution", Files.readString(directory.resolve("executed")));
      for (Path root : java.util.List.of(first, second))
        assertEquals("expected rows", Files.readString(root.resolve("committed/rows")));
    } finally {
      for (Process process : processes)
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
  }

  public static final class Worker {
    public static void main(String[] args) throws Exception {
      Path directory = Path.of(args[0]), root = Path.of(args[1]);
      new CanonicalReferenceCache(directory.resolve("cache"))
          .reference(
              directory.resolve("fixture.jar"),
              root,
              "continue",
              () -> {
                Files.writeString(
                    directory.resolve("executed"),
                    "one execution",
                    java.nio.file.StandardOpenOption.CREATE_NEW);
                Thread.sleep(100);
                output(root);
              });
    }
  }
}
