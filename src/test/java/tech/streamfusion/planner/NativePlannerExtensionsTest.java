package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.extensions.HealthyPlannerExtension;
import tech.streamfusion.planner.common.StreamRule;

class NativePlannerExtensionsTest {
  private static final String SERVICE =
      "META-INF/services/" + NativePlannerExtension.class.getName();

  @TempDir Path directory;

  static Stream<Supplier<Error>> optionalProviderFailures() {
    return Stream.of(
        () -> new NoClassDefFoundError("optional/module/Class"),
        () -> new NoSuchMethodError("optional.method()"),
        () -> new NoSuchFieldError("optional.field"),
        () -> new AbstractMethodError("optional.method()"),
        () -> new UnsatisfiedLinkError("optional_native_symbol"),
        () -> new ServiceConfigurationError("optional provider unavailable"));
  }

  static Stream<Supplier<Throwable>> unexpectedFailures() {
    return Stream.of(
        () -> new IllegalStateException("provider bug"),
        () -> new OutOfMemoryError("not optional"),
        ThreadDeath::new,
        () -> new AssertionError("provider invariant"));
  }

  @ParameterizedTest
  @MethodSource("optionalProviderFailures")
  void registrationIsAtomicAndHealthyProvidersRetainTheirOrder(Supplier<Error> failure) {
    StreamRule<?> first = rule();
    StreamRule<?> second = rule();
    StreamRule<?> third = rule();
    NativePlannerExtension before = entries -> entries.addAll(List.of(first, second));
    NativePlannerExtension broken =
        new NativePlannerExtension() {
          @Override
          public void addSubstitutions(List<StreamRule<?>> entries) {
            assertTrue(entries.isEmpty(), "a provider must not see another provider's rules");
            entries.add(rule());
            throw failure.get();
          }

          @Override
          public String sourceSharingKey(RelNode node) {
            return fail("a failed provider must never participate in source sharing");
          }
        };
    NativePlannerExtension after = entries -> entries.add(third);

    NativePlannerExtensions loaded =
        NativePlannerExtensions.load(List.of(before, broken, after).iterator());

    assertEquals(List.of(before, after), loaded.extensions());
    assertEquals(List.of(first, second, third), loaded.substitutions());
    loaded.extensions().forEach(extension -> assertNull(extension.sourceSharingKey(null)));
    assertThrows(UnsupportedOperationException.class, () -> loaded.extensions().clear());
    assertThrows(UnsupportedOperationException.class, () -> loaded.substitutions().clear());
  }

  @Test
  void publishedRulesDoNotRetainTheProvidersMutableList() {
    List<List<StreamRule<?>>> captured = new ArrayList<>();
    StreamRule<?> entry = rule();
    NativePlannerExtension provider =
        entries -> {
          entries.add(entry);
          captured.add(entries);
        };
    NativePlannerExtensions loaded = NativePlannerExtensions.load(List.of(provider).iterator());
    captured.get(0).clear();
    assertEquals(List.of(entry), loaded.substitutions());
  }

  @Test
  void anEmptyDeploymentNeedsNoOptionalProviders() {
    NativePlannerExtensions loaded = NativePlannerExtensions.load(Collections.emptyIterator());
    assertTrue(loaded.extensions().isEmpty());
    assertTrue(loaded.substitutions().isEmpty());
  }

  @Test
  void discoveryCanContinueAfterServiceAndLinkageFailures() {
    NativePlannerExtension healthy = entries -> entries.add(rule());
    Iterator<NativePlannerExtension> providers =
        new Iterator<NativePlannerExtension>() {
          private int step;

          @Override
          public boolean hasNext() {
            if (step == 0) {
              step++;
              throw new ServiceConfigurationError("missing provider");
            }
            return step < 3;
          }

          @Override
          public NativePlannerExtension next() {
            if (step++ == 1) {
              throw new NoClassDefFoundError("optional/dependency");
            }
            return healthy;
          }
        };
    NativePlannerExtensions loaded = NativePlannerExtensions.load(providers);
    assertEquals(List.of(healthy), loaded.extensions());
    assertEquals(1, loaded.substitutions().size());
  }

  @ParameterizedTest
  @MethodSource("unexpectedFailures")
  void programmingAndFatalFailuresPropagateEvenWhenWrapped(Supplier<Throwable> failures) {
    for (boolean wrapped : List.of(false, true)) {
      Throwable failure = failures.get();
      NativePlannerExtension provider =
          entries -> {
            if (wrapped) throw new ServiceConfigurationError("provider constructor", failure);
            throwFailure(failure);
          };
      assertSame(
          failure,
          assertThrows(
              failure.getClass(),
              () -> NativePlannerExtensions.load(List.of(provider).iterator())));

      Iterator<NativePlannerExtension> discovery =
          new Iterator<NativePlannerExtension>() {
            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public NativePlannerExtension next() {
              throw new ServiceConfigurationError("provider constructor", failure);
            }
          };
      assertSame(
          failure,
          assertThrows(failure.getClass(), () -> NativePlannerExtensions.load(discovery)));
    }
  }

  @Test
  void realServiceLoaderFailuresDoNotPoisonThePlannerOrSourceSharing() throws Exception {
    Path service = directory.resolve(SERVICE);
    Files.createDirectories(service.getParent());
    Files.writeString(
        service,
        "missing.optional.Provider\n"
            + ConstructorLinkageExtension.class.getName()
            + "\n"
            + MissingDependencyExtension.class.getName()
            + "\n"
            + HealthyPlannerExtension.class.getName()
            + "\n");
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (OptionalDependencyClassLoader isolated =
        new OptionalDependencyClassLoader(service.toUri().toURL())) {
      Thread.currentThread().setContextClassLoader(isolated);
      assertThrows(
          ClassNotFoundException.class,
          () -> isolated.loadClass(OptionalDependency.class.getName()));
      Class<?> fixture = Class.forName(IsolatedPlannerFixture.class.getName(), true, isolated);
      fixture.getMethod("verify").invoke(null);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static StreamRule<?> rule() {
    return StreamRule.of(RelNode.class, (node, context) -> node);
  }

  private static void throwFailure(Throwable failure) {
    if (failure instanceof RuntimeException) throw (RuntimeException) failure;
    throw (Error) failure;
  }

  public static final class ConstructorLinkageExtension implements NativePlannerExtension {
    public ConstructorLinkageExtension() {
      new OptionalDependency().touch();
    }

    @Override
    public void addSubstitutions(List<StreamRule<?>> entries) {
      fail("the provider constructor should have failed");
    }
  }

  public static final class MissingDependencyExtension implements NativePlannerExtension {
    @Override
    public void addSubstitutions(List<StreamRule<?>> entries) {
      entries.add(StreamRule.of(RelNode.class, (node, context) -> fail("partial rule leaked")));
      new OptionalDependency().touch();
    }

    @Override
    public String sourceSharingKey(RelNode node) {
      return fail("the unavailable extension's sharing hook must not run");
    }
  }

  public static final class OptionalDependency {
    public void touch() {}
  }

  public static final class IsolatedPlannerFixture {
    public static void verify() {
      TableEnvironment sql = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
      PhysicalPlanScan scan = NativePlanner.install(sql);
      sql.executeSql("CREATE TABLE output (doubled INT) WITH ('connector'='blackhole')");
      String plan =
          sql.explainSql(
              "INSERT INTO output SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)");
      assertTrue(plan.contains("NativeCalc"), plan);
      assertTrue(scan.substitutions() > 0, scan.explainSummary());
      assertTrue(
          HealthyPlannerExtension.sharingCalls > 0, "the healthy sharing hook must remain active");
    }
  }

  private static final class OptionalDependencyClassLoader extends URLClassLoader {
    private final URL service;

    OptionalDependencyClassLoader(URL service) throws Exception {
      super(classpath(), NativePlannerExtensionsTest.class.getClassLoader());
      this.service = service;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return SERVICE.equals(name)
          ? Collections.enumeration(List.of(service))
          : super.getResources(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.equals(OptionalDependency.class.getName())) throw new ClassNotFoundException(name);
      // Share the JNI owner so separate planner loaders do not load the same DSO twice.
      if (!name.startsWith("tech.streamfusion.")
          || name.equals("tech.streamfusion.Native")
          || name.startsWith("tech.streamfusion.Native$")
          || name.equals("tech.streamfusion.NativeException")) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) loaded = findClass(name);
        if (resolve) resolveClass(loaded);
        return loaded;
      }
    }

    private static URL[] classpath() throws Exception {
      ClassLoader own = NativePlannerExtensionsTest.class.getClassLoader();
      if (own instanceof URLClassLoader) return ((URLClassLoader) own).getURLs();
      List<URL> urls = new ArrayList<>();
      for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
        urls.add(Paths.get(entry).toUri().toURL());
      }
      return urls.toArray(new URL[0]);
    }
  }
}
