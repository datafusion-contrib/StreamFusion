package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeEncoderAdmissionTest {
  @Test
  void missingFormatIsAPlanningDecision() {
    NativeEncoderPlan plan =
        EncodeFormat.plan("not-installed", Map.of(), RowType.of(new IntType()));
    assertNull(plan.format);
    assertTrue(plan.fallbackReason.contains("artifact is not installed"));
  }

  @Test
  void textProvidersRejectUnsupportedSchemasBeforeLoadingNativeCode() {
    RowType mapWithIntegerKeys = RowType.of(new MapType(new IntType(), new IntType()));
    for (String name : new String[] {"json", "debezium-json", "csv"}) {
      NativeEncoderPlan plan = EncodeFormat.plan(name, Map.of(), mapWithIntegerKeys);
      assertNull(plan.format);
      assertTrue(plan.fallbackReason.contains("type"), plan.fallbackReason);
    }
  }

  @Test
  void missingNativeLibraryFallsBackBeforeTaskExecution(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path emptyLibraryPath) throws Exception {
    var process =
        new ProcessBuilder(
                java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dstreamfusion.native.development=true",
                "-Djava.library.path=" + emptyLibraryPath,
                "-cp",
                System.getProperty(
                    "surefire.test.class.path", System.getProperty("java.class.path")),
                NativeEncoderAdmissionTest.class.getName())
            .redirectErrorStream(true)
            .start();
    try {
      assertTrue(
          process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS),
          "library fallback probe did not exit");
      String output =
          new String(
              process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), output);
      assertTrue(output.contains("optional native encoder is unavailable"), output);
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor();
    }
  }

  public static void main(String[] args) {
    var plan = EncodeFormat.plan("json", Map.of(), RowType.of(new IntType()));
    if (plan.format != null) throw new AssertionError("missing JSON native library was admitted");
    System.out.println(plan.fallbackReason);
  }

  @Test
  void installedProvidersShipFactoriesWithoutCapturingTheirInstances() throws Exception {
    int checked = 0;
    for (NativeFormatProvider provider : ServiceLoader.load(NativeFormatProvider.class)) {
      var factory = provider.encoderFactory();
      assertNotNull(factory, provider.formatIdentifier());
      try (var bytes = new ByteArrayOutputStream();
          var out = new ObjectOutputStream(bytes)) {
        out.writeObject(factory);
        out.flush();
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
          assertInstanceOf(NativeMessageEncoderFactory.class, in.readObject());
        }
      }
      checked++;
    }
    assertEquals(11, checked);
  }
}
