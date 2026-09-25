package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqlInventoryTest {
  @TempDir Path directory;

  @Test
  void preservesSeparateStatementRootsAndInvocationIdentity() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    Identifier identifier = new Identifier();
    try {
      SqlInventory.started(identifier);
      SqlInventory.translating(this);
      SqlInventory.plan(
          new Scan(),
          List.of(
              new StreamPhysicalNativeCalc(),
              new StreamPhysicalSink(false),
              new StreamPhysicalSink(true)));
      SqlInventory.translated();
      SqlInventory.finished(identifier, new Result());
      try (var paths = Files.list(directory)) {
        String json = Files.readString(paths.findFirst().orElseThrow());
        assertTrue(json.contains("\"junit_id\":\"case[backend=heap]\""));
        assertTrue(json.contains("\"native_operators\":1"));
        assertTrue(json.contains("\"native_operators\":0"));
        assertTrue(json.contains("\"during_translation\":true"));
        assertTrue(json.contains("\"connector\":\"kafka\""));
        assertTrue(json.contains("\"format\":\"json\""));
        assertTrue(!json.contains("password"));
        assertTrue(
            json.contains(
                "\"options_error\":\"java.lang.UnsupportedOperationException: internal collect"
                    + " table\""));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  public static class Identifier {
    public boolean isTest() {
      return true;
    }

    public String getUniqueId() {
      return "case[backend=heap]";
    }

    public String getDisplayName() {
      return "case";
    }

    public Optional<String> getSource() {
      return Optional.of("fixture");
    }
  }

  public static class Result {
    public String getStatus() {
      return "SUCCESSFUL";
    }

    public Optional<Throwable> getThrowable() {
      return Optional.empty();
    }
  }

  public static class Scan {
    public int substitutions() {
      return 1;
    }

    public List<String> operatorTypes() {
      return List.of("StreamPhysicalCalc");
    }

    public List<String> fallbackReasons() {
      return List.of("Calc: unsupported function/operator: AS");
    }
  }

  public static class StreamPhysicalCalc {
    public List<Object> getInputs() {
      return List.of();
    }
  }

  public static class StreamPhysicalNativeCalc extends StreamPhysicalCalc {}

  public static class StreamPhysicalSink extends StreamPhysicalCalc {
    private final boolean internal;

    StreamPhysicalSink(boolean internal) {
      this.internal = internal;
    }

    public Object tableSink() {
      return this;
    }

    public Object contextResolvedTable() {
      return this;
    }

    public Object getResolvedTable() {
      return this;
    }

    public Map<String, String> getOptions() {
      if (internal) throw new UnsupportedOperationException("internal collect table");
      return Map.of("connector", "kafka", "format", "json", "password", "not-in-inventory");
    }
  }
}
