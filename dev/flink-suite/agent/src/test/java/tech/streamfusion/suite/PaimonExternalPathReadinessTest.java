package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(10)
class PaimonExternalPathReadinessTest {
  private static final String ALTER =
      "ALTER TABLE T2 SET ('data-file.external-paths' = 'traceable:///new-path')";

  @TempDir Path root;

  @Test
  void alterWaitsForAnActualFileRatherThanAnEmptyDirectory() throws Exception {
    var executor = Executors.newSingleThreadExecutor();
    var entered = new CountDownLatch(1);
    var waiting =
        executor.submit(
            () -> {
              PaimonExternalPathReadiness.begin(new Fixture(root));
              try {
                entered.countDown();
                PaimonExternalPathReadiness.beforeSql(ALTER);
              } finally {
                PaimonExternalPathReadiness.clear();
              }
              return null;
            });
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
      Files.createDirectory(root.resolve("bucket-0"));
      assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
      Files.writeString(root.resolve("bucket-0/data.parquet"), "fixture");
      waiting.get(5, TimeUnit.SECONDS);
    } finally {
      waiting.cancel(true);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void otherStatementsAndUnscopedInvocationsDoNotWait() throws Exception {
    PaimonExternalPathReadiness.beforeSql(ALTER);
    PaimonExternalPathReadiness.begin(new Fixture(root));
    try {
      PaimonExternalPathReadiness.beforeSql("SELECT * FROM T2");
      PaimonExternalPathReadiness.beforeSql("INSERT INTO T2 SELECT * FROM datagen_source");
      PaimonExternalPathReadiness.beforeSql(
          "ALTER TABLE other_table SET ('data-file.external-paths' = 'traceable:///new')");
      PaimonExternalPathReadiness.beforeSql("ALTER TABLE T2 SET ('bucket' = '1')");
    } finally {
      PaimonExternalPathReadiness.clear();
    }
    PaimonExternalPathReadiness.beforeSql(ALTER);
  }

  @Test
  void missingFilesFailRatherThanAllowingAnUnverifiedAlter() throws Exception {
    assertThrows(
        AssertionError.class,
        () -> PaimonExternalPathReadiness.awaitFile(root, Duration.ZERO));
    assertThrows(
        IOException.class,
        () -> PaimonExternalPathReadiness.awaitFile(root.resolve("missing"), Duration.ZERO));
    Files.writeString(root.resolve("data.parquet"), "fixture");
    PaimonExternalPathReadiness.awaitFile(root, Duration.ZERO);
  }

  @Test
  void interruptedWaitPropagatesTheFailure() {
    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedException.class,
          () -> PaimonExternalPathReadiness.awaitFile(root, Duration.ofSeconds(60)));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void advicePreservesTheStatementAndCleansUpSuccessfulAndExceptionalScopes() throws Exception {
    Class<?> type =
        new ByteBuddy()
            .redefine(Fixture.class)
            .visit(
                Advice.to(PaimonExternalPathReadiness.Scope.class)
                    .on(named("testTableReadWriteWithChangedExternalPath")))
            .visit(
                Advice.to(PaimonExternalPathReadiness.BeforeSql.class).on(named("executeSql")))
            .make()
            .load(Fixture.class.getClassLoader(), ClassLoadingStrategy.Default.CHILD_FIRST)
            .getLoaded();
    var fixture = type.getConstructor(Path.class).newInstance(root);
    Files.writeString(root.resolve("data.parquet"), "fixture");
    var test = type.getMethod("testTableReadWriteWithChangedExternalPath");
    test.invoke(fixture);
    assertEquals(ALTER, type.getField("statement").get(fixture));
    Files.delete(root.resolve("data.parquet"));
    type.getMethod("executeSql", String.class).invoke(fixture, ALTER);
    Files.writeString(root.resolve("data.parquet"), "fixture");
    var failure = new IllegalStateException("preserve test failure");
    type.getField("failure").set(fixture, failure);
    var actual =
        assertThrows(java.lang.reflect.InvocationTargetException.class, () -> test.invoke(fixture));
    assertSame(failure, actual.getCause());
    Files.delete(root.resolve("data.parquet"));
    type.getMethod("executeSql", String.class).invoke(fixture, ALTER);
    assertEquals(ALTER, type.getField("statement").get(fixture));
  }

  @Test
  void incompatibleFixtureFailsClosedAndClearsAnEarlierScope() throws Exception {
    PaimonExternalPathReadiness.begin(new Fixture(root));
    assertThrows(
        NoSuchFieldException.class, () -> PaimonExternalPathReadiness.begin(new Object()));
    PaimonExternalPathReadiness.beforeSql(ALTER);
  }

  public static class Fixture {
    public Path tempExternalPath1;
    public String statement;
    public RuntimeException failure;

    public Fixture(Path path) {
      tempExternalPath1 = path;
    }

    public void testTableReadWriteWithChangedExternalPath() {
      executeSql(
          "ALTER TABLE T2 SET ('data-file.external-paths' = 'traceable:///new-path')");
      if (failure != null) throw failure;
    }

    public void executeSql(String sql) {
      statement = sql;
    }
  }
}
