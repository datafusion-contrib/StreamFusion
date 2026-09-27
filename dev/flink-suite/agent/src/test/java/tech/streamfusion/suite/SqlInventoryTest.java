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

  @Test
  void collectsUncontractedWorkWithoutCreditingOpeningOrLateCallbacks() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    Identifier identifier = new Identifier();
    Object operator = new NativeCalcOperator();
    try {
      SqlInventory.started(identifier);
      NativeExecution.opened(operator);
      org.junit.jupiter.api.Assertions.assertEquals(
          7, NativeExecution.batchRows(operator, new Batch()));
      NativeExecution.completed(operator, 0);
      Thread task = new Thread(() -> NativeExecution.completed(operator, 7));
      task.start();
      task.join();
      SqlInventory.finished(identifier, new Result());
      SqlInventory.started(identifier);
      NativeExecution.completed(operator, 99);
      NativeExecution.completed(new NativeCalcOperator(), 99);
      NativeExecution.opened(new NativeCalcOperator());
      org.junit.jupiter.api.Assertions.assertEquals(
          0, NativeExecution.batchRows(operator, new Batch()));
      SqlInventory.finished(identifier, new Result());
      try (var paths = Files.list(directory)) {
        var reports =
            paths
                .map(
                    path -> {
                      try {
                        return Files.readString(path);
                      } catch (java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                      }
                    })
                .toList();
        org.junit.jupiter.api.Assertions.assertEquals(2, reports.size());
        assertTrue(
            reports.stream()
                .anyMatch(json -> json.contains("\"native_work\":{\"NativeCalcOperator\":7}")));
        assertTrue(reports.stream().anyMatch(json -> json.contains("\"native_work\":{}")));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  @Test
  void linksContractWitnessToExactInventoryInvocation() throws Exception {
    Path witnesses = directory.resolve("witnesses");
    Path inventory = directory.resolve("inventory");
    System.setProperty("streamfusion.flink-suite.sql-inventory", inventory.toString());
    System.setProperty("streamfusion.flink-suite.native-reports", witnesses.toString());
    try {
      var identifier = new Identifier();
      SqlInventory.started(identifier);
      var scope =
          NativeExecution.begin(
              "org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testLongProjectionList",
              new Object());
      Object operator = new NativeCalcOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 7);
      NativeExecution.finish(scope);
      SqlInventory.finished(identifier, new Result());
      try (var files = Files.list(witnesses);
          var records = Files.list(inventory)) {
        String witness =
            files.findFirst().orElseThrow().getFileName().toString().replace(".tsv", "");
        String json = Files.readString(records.findFirst().orElseThrow());
        assertTrue(json.contains("\"record_id\":\"" + witness + "\""));
        assertTrue(json.contains("\"native_work\":{\"NativeCalcOperator\":7}"));
        assertTrue(json.contains("\"variant\":\"*\""));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
      System.clearProperty("streamfusion.flink-suite.native-reports");
    }
  }

  public static class NativeCalcOperator {}

  public static class Batch {
    public int getRowCount() {
      return 7;
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
