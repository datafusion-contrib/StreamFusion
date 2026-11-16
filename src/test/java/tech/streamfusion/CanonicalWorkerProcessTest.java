package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalWorkerProcessTest {
  @TempDir Path temporary;

  @Test
  void terminatingAParentAlsoStopsItsWorker() throws Exception {
    var marker = temporary.resolve("worker.pid");
    var parent = java(Parent.class, marker.toString())
        .redirectErrorStream(true).redirectOutput(temporary.resolve("parent.log").toFile()).start();
    ProcessHandle worker = null;
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!Files.exists(marker) && parent.isAlive() && System.nanoTime() < deadline)
        Thread.sleep(20);
      assertTrue(Files.exists(marker), () -> {
        try { return Files.readString(temporary.resolve("parent.log")); }
        catch (Exception failure) { return failure.toString(); }
      });
      worker = ProcessHandle.of(Long.parseLong(Files.readString(marker))).orElseThrow();
      assertTrue(worker.isAlive());
      parent.destroy();
      assertTrue(parent.waitFor(15, TimeUnit.SECONDS));
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (worker.isAlive() && System.nanoTime() < deadline) Thread.sleep(20);
      assertFalse(worker.isAlive(), "the canceled parent left its worker running");
    } finally {
      if (worker != null && worker.isAlive()) worker.destroyForcibly();
      if (parent.isAlive()) parent.destroyForcibly();
      parent.waitFor(10, TimeUnit.SECONDS);
    }
  }

  private static ProcessBuilder java(Class<?> type, String... arguments) {
    var command = new java.util.ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(type.getName());
    command.addAll(java.util.List.of(arguments));
    return new ProcessBuilder(command);
  }

  public static class Parent {
    public static void main(String[] args) throws Exception {
      var worker = java(Sleeper.class).start();
      try (var lifetime = new CanonicalWorkerProcess(worker)) {
        Files.writeString(Path.of(args[0]), Long.toString(worker.pid()));
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
      }
    }
  }

  public static class Sleeper {
    public static void main(String[] args) throws Exception {
      Thread.sleep(TimeUnit.MINUTES.toMillis(1));
    }
  }
}
