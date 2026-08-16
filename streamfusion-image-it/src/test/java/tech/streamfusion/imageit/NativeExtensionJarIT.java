package tech.streamfusion.imageit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Proves each deployable extension JAR can load the matching native library it packages. */
class NativeExtensionJarIT {

  private static final Duration LOAD_TIMEOUT = Duration.ofSeconds(30);

  @Test
  void extensionJarsLoadTheirNativeLibraries() throws Exception {
    for (String extension :
        List.of(
            "kafka",
            "json",
            "csv",
            "raw",
            "avro",
            "avro-confluent-registry",
            "protobuf",
            "fluss",
            "parquet")) {
      Process process = extensionProcess(extension);
      assertTrue(
          process.waitFor(LOAD_TIMEOUT.toSeconds(), TimeUnit.SECONDS), extension + " probe timed out");
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), () -> extension + " probe failed:\n" + output);
    }
  }

  private static Process extensionProcess(String extension) throws IOException {
    String javaHome = System.getProperty("java.home");
    ProcessBuilder process =
        new ProcessBuilder(
                Path.of(javaHome, "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ExtensionProbe.class.getName(),
                requiredProperty("streamfusion.project.dir"),
                requiredProperty("streamfusion.version"),
                extension)
            .redirectErrorStream(true);
    process.environment().put("GLIBC_TUNABLES", "glibc.rtld.optional_static_tls=131072");
    return process.start();
  }

  /** Runs in a fresh JVM so each extension gets its own isolated core/native-library class loader. */
  public static final class ExtensionProbe {

    private ExtensionProbe() {}

    public static void main(String[] args) throws Exception {
      Path projectDirectory = Path.of(args[0]);
      String version = args[1];
      String extension = args[2];
      Path core = artifact(projectDirectory, "streamfusion-core", version);
      Path extensionJar = artifact(projectDirectory, "streamfusion-" + extension, version);
      URL[] classpath = extensionClasspath(projectDirectory, version, extension, core, extensionJar);
      try (URLClassLoader loader =
          new URLClassLoader(
              classpath, ClassLoader.getPlatformClassLoader())) {
        Class<?> facade = Class.forName(facadeClass(extension), true, loader);
        Object loaded = facade.getMethod("isLoaded").invoke(null);
        if (!Boolean.TRUE.equals(loaded)) {
          throw new IllegalStateException("Native " + extension + " extension did not report loaded");
        }
        String format = formatIdentifier(extension);
        if (format != null && !providesFormat(loader, format)) {
          throw new IllegalStateException(extension + " did not register a provider for " + format);
        }
      }
    }

    private static URL[] extensionClasspath(
        Path projectDirectory, String version, String extension, Path core, Path extensionJar)
        throws IOException {
      if ("avro-confluent-registry".equals(extension)) {
        Path avro = artifact(projectDirectory, "streamfusion-avro", version);
        return new URL[] {core.toUri().toURL(), avro.toUri().toURL(), extensionJar.toUri().toURL()};
      }
      if ("parquet".equals(extension)) {
        URL flinkCore =
            org.apache.flink.core.fs.FSDataOutputStream.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();
        return new URL[] {core.toUri().toURL(), extensionJar.toUri().toURL(), flinkCore};
      }
      return new URL[] {core.toUri().toURL(), extensionJar.toUri().toURL()};
    }

    private static Path artifact(Path projectDirectory, String module, String version) {
      Path artifact =
          projectDirectory
              .resolve(module)
              .resolve("target")
              .resolve(module + "-" + version + ".jar");
      if (!Files.isRegularFile(artifact)) {
        throw new IllegalStateException("Missing packaged extension artifact: " + artifact);
      }
      return artifact;
    }

    private static String facadeClass(String extension) {
      return switch (extension) {
        case "kafka" -> "tech.streamfusion.kafka.NativeKafka";
        case "json" -> "tech.streamfusion.format.json.NativeJsonFormat";
        case "csv" -> "tech.streamfusion.format.csv.NativeCsvFormat";
        case "raw" -> "tech.streamfusion.format.raw.NativeRawFormat";
        case "avro" -> "tech.streamfusion.format.avro.NativeAvroFormat";
        case "avro-confluent-registry" -> "tech.streamfusion.format.avro.NativeAvroFormat";
        case "protobuf" -> "tech.streamfusion.format.protobuf.NativeProtobufFormat";
        case "fluss" -> "tech.streamfusion.fluss.NativeFluss";
        case "parquet" -> "tech.streamfusion.parquet.NativeParquet";
        default -> throw new IllegalArgumentException("Unknown extension: " + extension);
      };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean providesFormat(ClassLoader loader, String expected) throws Exception {
      Class provider = Class.forName("tech.streamfusion.format.NativeFormatProvider", true, loader);
      for (Object candidate : ServiceLoader.load(provider, loader)) {
        String identifier = (String) provider.getMethod("formatIdentifier").invoke(candidate);
        if (expected.equals(identifier)) {
          return true;
        }
      }
      return false;
    }

    private static String formatIdentifier(String extension) {
      return switch (extension) {
        case "json" -> "json";
        case "csv" -> "csv";
        case "raw" -> "raw";
        case "avro" -> "avro";
        case "avro-confluent-registry" -> "avro-confluent";
        case "protobuf" -> "protobuf";
        default -> null;
      };
    }
  }

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required system property: " + name);
    }
    return value;
  }
}
