package tech.streamfusion;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Selects the deployed planner factory without installing a planner-loader shim in the test JVM.
 */
public final class NativePlannerTestEnvironment {
  private NativePlannerTestEnvironment() {}

  public static StreamTableEnvironment create(StreamExecutionEnvironment env) throws IOException {
    return withPlannerClassLoader(
        loader ->
            StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .withClassLoader(loader)
                    .build()));
  }

  public static TableEnvironment create(Configuration configuration) throws IOException {
    return create(configuration, new Class<?>[0]);
  }

  public static TableEnvironment create(Configuration configuration, Class<?>... factories)
      throws IOException {
    return create(configuration, false, factories);
  }

  public static TableEnvironment createBatch(Configuration configuration, Class<?>... factories)
      throws IOException {
    return create(configuration, true, factories);
  }

  private static TableEnvironment create(Configuration configuration, boolean batch,
      Class<?>... factories) throws IOException {
    return withPlannerClassLoader(
        loader ->
            TableEnvironment.create(
                (batch ? EnvironmentSettings.newInstance().inBatchMode()
                    : EnvironmentSettings.newInstance().inStreamingMode())
                    .withConfiguration(configuration)
                    .withClassLoader(loader)
                    .build()),
        java.util.Arrays.stream(factories).map(Class::getName).toArray(String[]::new));
  }

  private static <T> T withPlannerClassLoader(Function<ClassLoader, T> create, String... factories)
      throws IOException {
    String service = "META-INF/services/org.apache.flink.table.factories.Factory";
    ClassLoader parent = NativePlannerTestEnvironment.class.getClassLoader();
    var providers = new LinkedHashSet<String>();
    for (URL resource : Collections.list(parent.getResources(service))) {
      try (var input = resource.openStream()) {
        new String(input.readAllBytes(), StandardCharsets.UTF_8)
            .lines()
            .map(line -> line.split("#", 2)[0].trim())
            .filter(line -> !line.isEmpty())
            .filter(line -> !line.endsWith(".DefaultPlannerFactory"))
            .filter(line -> !line.endsWith(".DelegatePlannerFactory"))
            .forEach(providers::add);
      }
    }
    providers.addAll(java.util.Arrays.asList(factories));
    providers.add("tech.streamfusion.planner.StreamFusionPlannerFactory");
    var file = Files.createTempFile("streamfusion-planner-service", ".txt");
    file.toFile().deleteOnExit();
    Files.write(file, providers);
    ClassLoader loader =
        new ClassLoader(parent) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            return service.equals(name)
                ? Collections.enumeration(java.util.List.of(file.toUri().toURL()))
                : super.getResources(name);
          }
        };
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      return create.apply(loader);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
