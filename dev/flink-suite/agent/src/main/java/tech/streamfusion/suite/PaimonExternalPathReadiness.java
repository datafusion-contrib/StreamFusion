package tech.streamfusion.suite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import net.bytebuddy.asm.Advice;

/** Starts the external-path change only after the fixture has exercised its original path. */
public final class PaimonExternalPathReadiness {
  private static final ThreadLocal<Path> ORIGINAL_PATH = new ThreadLocal<>();

  private PaimonExternalPathReadiness() {}

  public static void begin(Object fixture) throws ReflectiveOperationException {
    ORIGINAL_PATH.remove();
    var field = fixture.getClass().getDeclaredField("tempExternalPath1");
    field.setAccessible(true);
    Object path = field.get(fixture);
    if (!(path instanceof Path)) {
      throw new IllegalStateException("Unknown Paimon external-path fixture: " + path);
    }
    ORIGINAL_PATH.set((Path) path);
  }

  public static void clear() {
    ORIGINAL_PATH.remove();
  }

  public static void beforeSql(String sql) throws IOException, InterruptedException {
    Path path = ORIGINAL_PATH.get();
    if (path != null
        && sql.matches("(?is)\\s*ALTER\\s+TABLE\\s+T2\\s+SET\\s*\\(.*")
        && sql.contains("'data-file.external-paths'")) {
      awaitFile(path, Duration.ofSeconds(60));
    }
  }

  static void awaitFile(Path path, Duration timeout) throws IOException, InterruptedException {
    long started = System.nanoTime();
    while (true) {
      try (var files = Files.walk(path)) {
        if (files.anyMatch(Files::isRegularFile)) return;
      }
      if (System.nanoTime() - started >= timeout.toNanos()) {
        throw new AssertionError("No data file reached the original Paimon external path: " + path);
      }
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }

  public static final class Scope {
    @Advice.OnMethodEnter
    public static void enter(@Advice.This Object fixture) throws ReflectiveOperationException {
      begin(fixture);
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void exit() {
      clear();
    }
  }

  public static final class BeforeSql {
    @Advice.OnMethodEnter
    public static void enter(@Advice.Argument(0) String sql)
        throws IOException, InterruptedException {
      beforeSql(sql);
    }
  }
}
