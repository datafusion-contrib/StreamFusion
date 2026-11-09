package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Independent executions: a host exception must never prevent the native-enabled run. */
final class NativeFailureParity {
  enum Phase {
    SETUP,
    PLANNING,
    SUBMISSION,
    INITIALIZATION,
    ROW_EVALUATION,
    COLLECTION
  }

  enum Route {
    HOST,
    NATIVE,
    FALLBACK,
    UNPLANNED
  }

  static final class Outcome {
    private final List<List<Object>> rows;
    private final Exception failure;
    private final Phase phase;
    private final Route route;
    private final List<String> fallbackReasons;
    private final List<String> operatorTypes;
    private final int substitutions;
    private final List<String> resultTypes;
    private final String plan;
    private final String rootCauseMessage;

    Outcome(
        List<List<Object>> rows,
        Exception failure,
        Phase phase,
        Route route,
        List<String> fallbackReasons,
        List<String> operatorTypes,
        int substitutions,
        List<String> resultTypes,
        String plan) {
      this.rows = rows;
      this.failure = failure;
      this.phase = phase;
      this.route = route;
      this.fallbackReasons = fallbackReasons;
      this.operatorTypes = operatorTypes;
      this.substitutions = substitutions;
      this.resultTypes = resultTypes;
      this.plan = plan;
      // Materialize JVM-generated details before another execution can unload generated code.
      this.rootCauseMessage = rootCause() == null ? null : rootCause().getMessage();
    }

    public List<List<Object>> rows() {
      return rows;
    }

    public Exception failure() {
      return failure;
    }

    public Phase phase() {
      return phase;
    }

    public Route route() {
      return route;
    }

    public List<String> fallbackReasons() {
      return fallbackReasons;
    }

    public List<String> operatorTypes() {
      return operatorTypes;
    }

    public int substitutions() {
      return substitutions;
    }

    public List<String> resultTypes() {
      return resultTypes;
    }

    public String plan() {
      return plan;
    }

    @Override
    public String toString() {
      return "Outcome[phase="
          + phase
          + ", route="
          + route
          + ", cause="
          + rootCause()
          + ", capturedMessage="
          + rootCauseMessage
          + ", collected="
          + rows.size()
          + ", rows="
          + rows.stream().limit(10).collect(ListCollectors.toList())
          + ", fallbackReasons="
          + fallbackReasons
          + "]";
    }

    Throwable rootCause() {
      Throwable cause = failure;
      while (cause != null && cause.getCause() != null) cause = cause.getCause();
      return cause;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Outcome that = (Outcome) other;
      return java.util.Objects.equals(rows, that.rows)
          && java.util.Objects.equals(failure, that.failure)
          && java.util.Objects.equals(phase, that.phase)
          && java.util.Objects.equals(route, that.route)
          && java.util.Objects.equals(fallbackReasons, that.fallbackReasons)
          && java.util.Objects.equals(operatorTypes, that.operatorTypes)
          && substitutions == that.substitutions
          && java.util.Objects.equals(resultTypes, that.resultTypes)
          && java.util.Objects.equals(plan, that.plan);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(rows);
      result = 31 * result + java.util.Objects.hashCode(failure);
      result = 31 * result + java.util.Objects.hashCode(phase);
      result = 31 * result + java.util.Objects.hashCode(route);
      result = 31 * result + java.util.Objects.hashCode(fallbackReasons);
      result = 31 * result + java.util.Objects.hashCode(operatorTypes);
      result = 31 * result + substitutions;
      result = 31 * result + java.util.Objects.hashCode(resultTypes);
      result = 31 * result + java.util.Objects.hashCode(plan);
      return result;
    }
  }

  static final class Comparison {
    private final Outcome host;
    private final Outcome nativeRun;

    Comparison(Outcome host, Outcome nativeRun) {
      this.host = host;
      this.nativeRun = nativeRun;
    }

    public Outcome host() {
      return host;
    }

    public Outcome nativeRun() {
      return nativeRun;
    }

    void assertFailure(Class<? extends Throwable> causeType, Phase phase, Route nativeRoute) {
      assertFailure(causeType, null, phase, nativeRoute);
    }

    void assertFailure(
        Class<? extends Throwable> causeType, String message, Phase phase, Route nativeRoute) {
      assertEquals(host.failure() == null, nativeRun.failure() == null, this::toString);
      for (Outcome outcome : List.of(host, nativeRun)) {
        assertNotNull(outcome.failure(), outcome.toString());
        Throwable cause = outcome.rootCause();
        assertTrue(causeType.isInstance(cause), outcome.toString());
        if (message != null) {
          assertTrue(
              String.valueOf(outcome.rootCauseMessage).contains(message), outcome.toString());
        }
        assertEquals(phase, outcome.phase(), outcome.toString());
      }
      assertEquals(host.rootCause().getClass(), nativeRun.rootCause().getClass(), this::toString);
      assertEquals(nativeRoute, nativeRun.route(), this::toString);
    }

    void assertCompetingFailures(
        java.util.function.Predicate<Throwable> expected, Phase phase, Route nativeRoute) {
      for (Outcome outcome : List.of(host, nativeRun)) {
        assertNotNull(outcome.failure(), outcome.toString());
        assertTrue(
            expected.test(outcome.rootCause()),
            () ->
                outcome
                    + "\n"
                    + org.apache.flink.util.ExceptionUtils.stringifyException(outcome.failure()));
        assertEquals(phase, outcome.phase(), outcome.toString());
      }
      assertEquals(nativeRoute, nativeRun.route(), this::toString);
    }

    void assertNullDereferenceFailure(String operation, Phase phase, Route nativeRoute) {
      assertFailure(NullPointerException.class, phase, nativeRoute);
      for (Outcome outcome : List.of(host, nativeRun)) {
        // Helpful NPE text is optional on every JDK, independently of SQL failure semantics.
        if (outcome.rootCauseMessage != null) {
          assertTrue(outcome.rootCauseMessage.contains(operation), this::toString);
        }
      }
    }

    void assertNullUnboxingFailure(Phase phase, Route nativeRoute) {
      assertNullDereferenceFailure("booleanValue", phase, nativeRoute);
      for (Outcome outcome : List.of(host, nativeRun)) {
        assertEquals(List.of(), outcome.rows(), this::toString);
      }
    }

    void assertSuccess(Route nativeRoute) {
      assertEquals(null, host.failure(), this::toString);
      assertEquals(null, nativeRun.failure(), this::toString);
      assertEquals(
          NativeParity.multiset(host.rows()),
          NativeParity.multiset(nativeRun.rows()),
          this::toString);
      assertEquals(nativeRoute, nativeRun.route(), this::toString);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Comparison that = (Comparison) other;
      return java.util.Objects.equals(host, that.host)
          && java.util.Objects.equals(nativeRun, that.nativeRun);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(host);
      result = 31 * result + java.util.Objects.hashCode(nativeRun);
      return result;
    }

    @Override
    public String toString() {
      return "Comparison[host=" + host + ", nativeRun=" + nativeRun + "]";
    }
  }

  static boolean isDivisionByZero(Throwable cause) {
    return (cause instanceof ArithmeticException || cause instanceof NativeException)
        && String.valueOf(cause.getMessage())
            .toLowerCase(java.util.Locale.ROOT)
            .contains("by zero");
  }

  static Comparison run(Supplier<TableEnvironment> environment, String sql) {
    return run(environment, environment, sql);
  }

  static Comparison runSmallBounded(Supplier<TableEnvironment> environment, String sql) {
    boolean awaitSmallResults =
        Boolean.parseBoolean(System.getProperty("sf.parity.awaitSmallResults", "true"));
    return new Comparison(
        execute(environment, sql, false, false, awaitSmallResults),
        execute(environment, sql, true, false, awaitSmallResults));
  }

  static Comparison run(
      Supplier<TableEnvironment> hostEnvironment,
      Supplier<TableEnvironment> nativeEnvironment,
      String sql) {
    Outcome host = execute(hostEnvironment, sql, false);
    Outcome nativeRun = execute(nativeEnvironment, sql, true);
    return new Comparison(host, nativeRun);
  }

  static Comparison runRecovery(
      Supplier<TableEnvironment> hostEnvironment,
      Supplier<TableEnvironment> nativeEnvironment,
      String sql) {
    return new Comparison(
        execute(hostEnvironment, sql, false, true), execute(nativeEnvironment, sql, true, true));
  }

  private static Outcome execute(
      Supplier<TableEnvironment> environment, String sql, boolean nativeRun) {
    return execute(environment, sql, nativeRun, false);
  }

  private static Outcome execute(
      Supplier<TableEnvironment> environment, String sql, boolean nativeRun, boolean recovery) {
    return execute(environment, sql, nativeRun, recovery, false);
  }

  private static Outcome execute(
      Supplier<TableEnvironment> environment,
      String sql,
      boolean nativeRun,
      boolean recovery,
      boolean awaitSmallResults) {
    List<List<Object>> rows = new ArrayList<>();
    PhysicalPlanScan scan = null;
    Phase phase = Phase.SETUP;
    Exception failure = null;
    TableResult result = null;
    List<String> resultTypes = List.of();
    String plan = "";
    try {
      String timing = nativeRun ? "parity.native." : "parity.host.";
      TableEnvironment table = CiFixtureTimings.measure(timing + "environment", environment::get);
      if (nativeRun) scan = NativePlanner.install(table);
      phase = Phase.PLANNING;
      var query = CiFixtureTimings.measure(timing + "sql-query", () -> table.sqlQuery(sql));
      resultTypes =
          query.getResolvedSchema().getColumnDataTypes().stream()
              .map(Object::toString)
              .collect(ListCollectors.toList());
      plan = CiFixtureTimings.measure(timing + "explain", query::explain);
      phase = Phase.SUBMISSION;
      if (recovery) {
        result =
            CiFixtureTimings.measure(
                timing + "submit",
                () ->
                    query.executeInsert(
                        org.apache.flink.table.api.TableDescriptor.forConnector("recovery-results")
                            .build()));
        phase = Phase.COLLECTION;
        TableResult submitted = result;
        var completed =
            CiFixtureTimings.measure(
                timing + "completion",
                () -> submitted.getJobClient().orElseThrow().getJobExecutionResult().get());
        rows.addAll(
            CiFixtureTimings.measure(
                timing + "decode-results",
                () ->
                    RecoveryResultTableFactory.results(
                        completed, query.getResolvedSchema().toPhysicalRowDataType())));
      } else {
        result = CiFixtureTimings.measure(timing + "submit", query::execute);
        phase = Phase.COLLECTION;
        long collectedStarted = CiFixtureTimings.start();
        if (awaitSmallResults) {
          try {
            result
                .getJobClient()
                .orElseThrow()
                .getJobExecutionResult()
                .handle((completed, error) -> null)
                .get(100, TimeUnit.MILLISECONDS);
          } catch (TimeoutException stillRunning) {
            // Drain larger or slower jobs normally instead of waiting on a full collect buffer.
          }
        }
        try (var iterator = result.collect()) {
          while (iterator.hasNext()) {
            var row = iterator.next();
            List<Object> fields = new ArrayList<>();
            fields.add(row.getKind().shortString());
            for (int i = 0; i < row.getArity(); i++) {
              fields.add(NativeParity.comparableValue(row.getField(i)));
            }
            rows.add(fields);
          }
        } finally {
          CiFixtureTimings.record(timing + "collect", collectedStarted);
        }
      }
    } catch (Exception error) {
      failure = error;
      if (result != null && result.getJobClient().isPresent()) {
        failure =
            terminalFailure(result.getJobClient().orElseThrow().getJobExecutionResult(), error);
      }
      if (phase == Phase.SUBMISSION || phase == Phase.COLLECTION) {
        phase = failurePhase(failure, phase);
      }
    }
    Route route =
        !nativeRun
            ? Route.HOST
            : scan == null || scan.operatorTypes().isEmpty()
                ? Route.UNPLANNED
                : scan.substitutions() > 0 ? Route.NATIVE : Route.FALLBACK;
    return new Outcome(
        rows,
        failure,
        phase,
        route,
        scan == null ? List.of() : List.copyOf(scan.fallbackReasons()),
        scan == null ? List.of() : List.copyOf(scan.operatorTypes()),
        scan == null ? 0 : scan.substitutions(),
        resultTypes,
        plan);
  }

  static Exception terminalFailure(CompletableFuture<?> completion, Exception collectionFailure) {
    try {
      completion.get(30, TimeUnit.SECONDS);
    } catch (ExecutionException jobFailure) {
      // Closing the iterator can cancel an otherwise running job after a local collection error.
      for (Throwable cause = jobFailure; cause != null; cause = cause.getCause()) {
        if (cause instanceof JobCancellationException) return collectionFailure;
      }
      return jobFailure;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      collectionFailure.addSuppressed(interrupted);
    } catch (TimeoutException | CancellationException unavailable) {
      collectionFailure.addSuppressed(unavailable);
    }
    return collectionFailure;
  }

  /**
   * Prefer the originating operator frame; the collect API also transports remote task failures.
   */
  static Phase failurePhase(Throwable error, Phase observed) {
    List<Throwable> causes = new ArrayList<>();
    for (Throwable cause = error; cause != null; cause = cause.getCause()) causes.add(cause);
    for (int i = causes.size() - 1; i >= 0; i--) {
      for (StackTraceElement frame : causes.get(i).getStackTrace()) {
        String method = frame.getMethodName();
        if (method.equals("open")
            || method.equals("initializeState")
            || method.equals("createStreamOperator")) return Phase.INITIALIZATION;
        if (method.equals("processElement")
            || method.equals("processElement1")
            || method.equals("processElement2")
            || method.equals("endInput")
            || method.equals("finishBundle")
            || method.equals("flushBundle")
            || (frame.getClassName().equals("tech.streamfusion.operator.BatchCoalescer")
                && method.equals("flush"))
            || method.equals("eval")
            || method.equals("accumulate")) return Phase.ROW_EVALUATION;
      }
    }
    return observed;
  }

  private NativeFailureParity() {}
}
