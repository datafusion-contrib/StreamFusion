package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeFormatProvidersTest {

  @Test
  void incompleteOptionalProviderFallsBackInsteadOfFailingPlannerDiscovery() throws Exception {
    Path root = Files.createTempDirectory("streamfusion-broken-provider");
    Path services = root.resolve("META-INF/services");
    Files.createDirectories(services);
    Files.writeString(
        services.resolve("tech.streamfusion.format.NativeFormatProvider"),
        BrokenNativeFormatProvider.class.getName() + "\n",
        StandardCharsets.UTF_8);

    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader =
        new URLClassLoader(
            new URL[] {root.toUri().toURL()}, original)) {
      Thread.currentThread().setContextClassLoader(loader);
      RowType rowType = RowType.of(new BigIntType());
      NativeFormatContext context =
          new NativeFormatContext(rowType, rowType, Map.of("format", "broken"), false);

      assertTrue(NativeFormatProviders.find(context).isEmpty());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
