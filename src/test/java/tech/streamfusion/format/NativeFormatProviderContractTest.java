package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Pins the SPI's linkage contract: the extension-JAR smoke test instantiates providers through
 * ServiceLoader over the platform classloader — no Flink jars — so every provider class must LINK
 * without Flink. Two ways this has broken CI while passing every in-JVM test: a Flink type in an
 * SPI method signature (reflection resolves declared signatures eagerly), and a provider method
 * body passing a subtype where a Flink API declares a supertype (e.g. a {@code RowType} argument
 * for a {@code LogicalType} parameter — the bytecode verifier must load both to prove the
 * assignability, at class-link time). Flink types must ride inside carrier classes like
 * {@link NativeFormatContext}, and any Flink call that crosses types belongs in a helper class the
 * provider only invokes at runtime (see {@code AvroEncodeGate.derivedSchema}).
 *
 * <p>The linking test below reproduces the probe's world exactly — it loads each registered
 * provider from {@code target/classes} through a classloader that refuses everything outside the
 * JDK and this project, then asks for its constructor, which links the class.
 */
class NativeFormatProviderContractTest {

  @Test
  void spiSignaturesReferenceOnlyProbeSafeTypes() {
    for (Method method : NativeFormatProvider.class.getDeclaredMethods()) {
      Stream.concat(
              Stream.of(method.getReturnType()), Arrays.stream(method.getParameterTypes()))
          .forEach(
              type ->
                  assertTrue(
                      probeSafe(type),
                      () ->
                          "NativeFormatProvider."
                              + method.getName()
                              + " references "
                              + type.getName()
                              + ", which the Flink-less extension-JAR probe cannot load; carry it"
                              + " inside NativeFormatContext or another format-package class"));
    }
  }

  @Test
  void providersLinkWithoutFlinkOnTheClasspath() throws Exception {
    Path classes = Path.of("target", "classes").toAbsolutePath();
    assertTrue(Files.isDirectory(classes), "compiled classes expected at " + classes);
    for (String provider : registeredProviders()) {
      try (FlinkLessClassLoader loader = new FlinkLessClassLoader(classes)) {
        Class.forName(provider, false, loader).getConstructor();
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        fail(
            provider
                + " does not link without Flink on the classpath (the extension-JAR probe world):"
                + " move the offending reference into a runtime-only helper class — "
                + e);
      }
    }
  }

  /** Every provider registered by a format artifact's service file. */
  private static List<String> registeredProviders() throws IOException {
    Path root = Path.of("..").toAbsolutePath().normalize();
    try (Stream<Path> modules = Files.list(root)) {
      return modules
          .filter(module -> module.getFileName().toString().startsWith("streamfusion-"))
          .map(
              module ->
                  module.resolve(
                      "src/main/resources/META-INF/services/"
                          + NativeFormatProvider.class.getName()))
          .filter(Files::isRegularFile)
          .flatMap(NativeFormatProviderContractTest::lines)
          .map(String::trim)
          .filter(line -> !line.isEmpty() && !line.startsWith("#"))
          .distinct()
          .toList();
    }
  }

  private static Stream<String> lines(Path file) {
    try {
      return Files.readAllLines(file).stream();
    } catch (IOException e) {
      throw new IllegalStateException("cannot read service registration " + file, e);
    }
  }

  private static boolean probeSafe(Class<?> type) {
    if (type.isPrimitive()) {
      return true;
    }
    if (type.isArray()) {
      return probeSafe(type.getComponentType());
    }
    String name = type.getName();
    return name.startsWith("java.") || name.startsWith("tech.streamfusion.format.");
  }

  /**
   * The probe's classloader shape: project classes resolve from {@code target/classes}, the JDK
   * from the platform loader, and everything else — Flink above all — does not exist.
   */
  private static final class FlinkLessClassLoader extends ClassLoader implements AutoCloseable {
    private final Path classes;

    private FlinkLessClassLoader(Path classes) {
      super(ClassLoader.getPlatformClassLoader());
      this.classes = classes;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.startsWith("tech.streamfusion.")) {
        throw new ClassNotFoundException(name);
      }
      Path file = classes.resolve(name.replace('.', '/') + ".class");
      try {
        byte[] bytes = Files.readAllBytes(file);
        return defineClass(name, bytes, 0, bytes.length);
      } catch (IOException e) {
        throw new ClassNotFoundException(name, e);
      }
    }

    @Override
    public void close() {}
  }
}
