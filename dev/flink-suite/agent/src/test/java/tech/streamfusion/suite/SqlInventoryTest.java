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

  @Test
  void jobResultsAreDeduplicatedAndStayWithTheirSubmittingInvocation() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    try {
      var identifier = new Identifier();
      SqlInventory.started(identifier);
      Object scope = SqlInventory.submitting();
      var success =
          new JobClient("success", java.util.concurrent.CompletableFuture.completedFuture(null));
      SqlInventory.submitted(scope, success, null);
      SqlInventory.submitted(scope, success, null);
      SqlInventory.submitted(
          scope, new JobClient("pending", new java.util.concurrent.CompletableFuture<>()), null);
      SqlInventory.submitted(
          scope,
          new JobClient(
              "failed",
              java.util.concurrent.CompletableFuture.failedFuture(
                  new IllegalStateException("job failed"))),
          null);
      SqlInventory.submitted(
          scope,
          new JobClient("unavailable", null) {
            @Override
            public java.util.concurrent.CompletableFuture<?> getJobExecutionResult() {
              throw new UnsupportedOperationException("detached client");
            }
          },
          null);
      SqlInventory.finished(identifier, new Result());
      SqlInventory.started(identifier);
      SqlInventory.submitted(
          scope,
          new JobClient("late", java.util.concurrent.CompletableFuture.completedFuture(null)),
          null);
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
        assertTrue(
            reports.stream()
                .anyMatch(json -> json.contains("\"success\":{\"status\":\"SUCCEEDED\"}")));
        assertTrue(
            reports.stream()
                .anyMatch(json -> json.contains("\"pending\":{\"status\":\"SUBMITTED\"}")));
        assertTrue(
            reports.stream()
                .anyMatch(json -> json.contains("java.lang.IllegalStateException: job failed")));
        assertTrue(reports.stream().anyMatch(json -> json.contains("\"status\":\"UNAVAILABLE\"")));
        assertTrue(reports.stream().anyMatch(json -> json.contains("\"jobs\":{}")));
        assertTrue(reports.stream().noneMatch(json -> json.contains("\"late\"")));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  @Test
  void distinguishesFailedJobsFromCancelledAndUnavailableStatusRequests() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    try {
      var identifier = new Identifier();
      SqlInventory.started(identifier);
      Object scope = SqlInventory.submitting();
      for (String status : List.of("FAILED", "CANCELED", "unavailable", "pending")) {
        SqlInventory.submitted(
            scope,
            new JobClient(
                status,
                java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("result request failed"))) {
              @Override
              public java.util.concurrent.CompletableFuture<?> getJobStatus() {
                if (status.equals("unavailable"))
                  return java.util.concurrent.CompletableFuture.failedFuture(
                      new IllegalStateException("status request failed"));
                if (status.equals("pending")) return new java.util.concurrent.CompletableFuture<>();
                return java.util.concurrent.CompletableFuture.completedFuture(status);
              }
            },
            null);
      }
      SqlInventory.finished(identifier, new Result());
      try (var files = Files.list(directory)) {
        String json = Files.readString(files.findFirst().orElseThrow());
        assertTrue(json.contains("\"job_status\":\"FAILED\""));
        assertTrue(json.contains("\"job_status\":\"CANCELED\""));
        assertTrue(json.contains("\"job_status_error\":"));
        assertTrue(json.contains("status request failed"));
        assertTrue(json.contains("\"pending\":{\"status\":\"RESULT_FAILED\",\"failure\":"));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  @Test
  void readsGeneratedOperatorIdentityWithoutCompilingItsSource() throws Exception {
    var generated =
        new org.apache.flink.table.runtime.generated.GeneratedOperator<
            org.apache.flink.streaming.api.operators.StreamOperator<Integer>>(
            "GeneratedCalc",
            "not valid Java: compilation must not run",
            new Object[0],
            new org.apache.flink.configuration.Configuration());
    var factory = new org.apache.flink.table.runtime.operators.CodeGenOperatorFactory<>(generated);
    assertTrue(SqlInventory.operatorClass(factory).equals("GeneratedCalc"));
  }

  @Test
  void attachesReleasedFlinkGraphToItsSubmittedJob() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    try {
      var env =
          org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
              .getExecutionEnvironment();
      env.setParallelism(2);
      env.getConfig().disableClosureCleaner();
      env.fromElements(1, 2)
          .map(value -> value + 1)
          .returns(org.apache.flink.api.common.typeinfo.Types.INT)
          .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());
      var graph = env.getStreamGraph();
      var identifier = new Identifier();
      SqlInventory.started(identifier);
      Object outer = SqlInventory.submitting();
      Object inner = SqlInventory.submitting(new Object[] {graph});
      var client =
          new JobClient("graph-job", java.util.concurrent.CompletableFuture.completedFuture(null));
      SqlInventory.submitted(inner, client, null);
      SqlInventory.submitted(outer, client, null);
      SqlInventory.finished(identifier, new Result());
      try (var paths = Files.list(directory)) {
        String json = Files.readString(paths.findFirst().orElseThrow());
        assertTrue(json.contains("\"job_type\":\"STREAMING\""), json);
        assertTrue(json.contains("org.apache.flink.streaming.api.operators.StreamMap"), json);
        assertTrue(json.contains("\"parallelism\":2"), json);
        assertTrue(json.contains("\"status\":\"SUCCEEDED\""), json);
        assertTrue(!json.contains("observation_error"), json);
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  @Test
  void associatesEarlyOperatorWorkByJobIdAndRejectsLateOpenings() throws Exception {
    System.setProperty("streamfusion.flink-suite.sql-inventory", directory.toString());
    try {
      var identifier = new Identifier();
      SqlInventory.started(identifier);
      Object submission = SqlInventory.submitting();
      var owned = new JobOperator("owned-job");
      NativeExecution.opened(owned);
      NativeExecution.completed(owned, 5);
      var unmatched = new JobOperator("missing-job");
      NativeExecution.opened(unmatched);
      NativeExecution.completed(unmatched, 3);
      var withoutJob = new NativeCalcOperator();
      NativeExecution.opened(withoutJob);
      NativeExecution.completed(withoutJob, 1);
      SqlInventory.submitted(
          submission,
          new JobClient("owned-job", java.util.concurrent.CompletableFuture.completedFuture(null)),
          null);
      SqlInventory.finished(identifier, new Result());
      SqlInventory.started(identifier);
      var late = new JobOperator("owned-job");
      NativeExecution.opened(late);
      NativeExecution.completed(late, 100);
      SqlInventory.finished(identifier, new Result());
      try (var paths = Files.list(directory)) {
        var records =
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
        assertTrue(
            records.stream()
                .anyMatch(
                    json ->
                        json.contains(
                            "\"owned-job\":{\"status\":\"SUCCEEDED\",\"native_work\":{\"JobOperator\":5}}")));
        assertTrue(
            records.stream()
                .anyMatch(
                    json ->
                        json.contains(
                            "\"unmatched_native_jobs\":{\"missing-job\":{\"JobOperator\":3}}")));
        assertTrue(
            records.stream()
                .anyMatch(
                    json ->
                        json.contains("\"unattributed_native_work\":{\"NativeCalcOperator\":1}")));
        assertTrue(records.stream().anyMatch(json -> json.contains("\"native_work\":{}")));
      }
    } finally {
      System.clearProperty("streamfusion.flink-suite.sql-inventory");
    }
  }

  public static class JobOperator {
    private final String job;

    JobOperator(String job) {
      this.job = job;
    }

    public JobMetrics getMetricGroup() {
      return new JobMetrics(job);
    }
  }

  public static class JobMetrics {
    private final String job;

    JobMetrics(String job) {
      this.job = job;
    }

    public Map<String, String> getAllVariables() {
      return Map.of("<job_id>", job);
    }
  }

  public static class JobClient {
    private final String id;
    private final java.util.concurrent.CompletableFuture<?> result;

    JobClient(String id, java.util.concurrent.CompletableFuture<?> result) {
      this.id = id;
      this.result = result;
    }

    public String getJobID() {
      return id;
    }

    public java.util.concurrent.CompletableFuture<?> getJobExecutionResult() {
      return result;
    }

    public java.util.concurrent.CompletableFuture<?> getJobStatus() {
      return new java.util.concurrent.CompletableFuture<>();
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
