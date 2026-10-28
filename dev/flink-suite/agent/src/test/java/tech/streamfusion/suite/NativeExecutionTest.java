package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeExecutionTest {
  private static final String CALC =
      "org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testLongProjectionList";
  private static final String WINDOW =
      "org.apache.flink.table.planner.runtime.stream.sql.WindowDistinctAggregateITCase#testHopWindow";
  private static final String FALLBACK =
      "org.apache.flink.table.planner.runtime.stream.sql.RankITCase#testTopNWithVariableTopSize";
  private static final String FALLBACK_REASON =
      "Top-N: nullable variable rank bounds require Flink's row-access semantics";

  @TempDir Path reports;

  @BeforeEach
  void configureReports() {
    System.setProperty("streamfusion.flink-suite.native-reports", reports.toString());
  }

  @Test
  void requiresWorkInsteadOfOpeningAnOperator() {
    NativeExecution.Scope scope = begin(CALC);
    NativeExecution.opened(new NativeCalcOperator());
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void emptyBatchesAndOtherOperatorsDoNotSatisfyTheContract() {
    NativeExecution.Scope scope = begin(CALC);
    Object calc = new NativeCalcOperator();
    Object window = new NativeColumnarWindowAggregateOperator();
    NativeExecution.opened(calc);
    NativeExecution.opened(window);
    NativeExecution.completed(calc, 0);
    NativeExecution.completed(window, 10);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void collectsTaskThreadWorkAndWritesEvidence() throws Exception {
    NativeExecution.Scope scope = begin(CALC);
    Object calc = new NativeCalcOperator();
    Thread task =
        new Thread(
            () -> {
              NativeExecution.opened(calc);
              NativeExecution.completed(calc, 3);
              NativeExecution.completed(calc, 4);
            });
    task.start();
    task.join();
    NativeExecution.finish(scope);
    try (var files = Files.list(reports)) {
      assertEquals(
          CALC + "\t*\tNativeCalcOperator=7\t\n",
          Files.readString(files.findFirst().orElseThrow()));
    }
  }

  @Test
  void cannotCreditLateWorkFromThePreviousInvocation() {
    Object oldOperator = new NativeCalcOperator();
    NativeExecution.Scope first = begin(CALC);
    NativeExecution.opened(oldOperator);
    NativeExecution.completed(oldOperator, 1);
    NativeExecution.finish(first);
    NativeExecution.Scope second = begin(CALC);
    NativeExecution.completed(oldOperator, 10);
    NativeExecution.completed(new NativeCalcOperator(), 10);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(second));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void twoPhaseWindowsRequireBothHalves(boolean splitDistinct) {
    NativeExecution.Scope first = NativeExecution.begin(WINDOW, new WindowFixture(splitDistinct));
    Object local = new NativeColumnarLocalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.completed(local, 5);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(first));
    NativeExecution.Scope second = NativeExecution.begin(WINDOW, new WindowFixture(splitDistinct));
    Object global = new NativeColumnarGlobalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.opened(global);
    NativeExecution.completed(local, 5);
    NativeExecution.completed(global, 2);
    NativeExecution.finish(second);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void splitDistinctCannotBeProvenByCalcOrSinglePhaseWork(boolean calcOnly) {
    NativeExecution.Scope scope = NativeExecution.begin(WINDOW, new WindowFixture(true));
    Object operator =
        calcOnly ? new NativeCalcOperator() : new NativeColumnarWindowAggregateOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 5);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void alsoAcceptsSinglePhaseWindows() {
    NativeExecution.Scope scope = begin(WINDOW);
    Object window = new NativeColumnarWindowAggregateOperator();
    NativeExecution.opened(window);
    NativeExecution.completed(window, 5);
    NativeExecution.finish(scope);
  }

  @Test
  void retractingRankRequiresWorkFromBothAggregateAndTopN() {
    String test =
        "org.apache.flink.table.planner.runtime.stream.sql.RankITCase#testTopNWithGroupByAndRetract";
    NativeExecution.Scope scope = begin(test);
    Object aggregate = new NativeColumnarGroupAggregateOperator();
    Object rank = new NativeColumnarTopNOperator();
    NativeExecution.opened(aggregate);
    NativeExecution.opened(rank);
    NativeExecution.completed(aggregate, 8);
    NativeExecution.Scope missingRank = scope;
    assertThrows(AssertionError.class, () -> NativeExecution.finish(missingRank));
    scope = begin(test);
    NativeExecution.opened(aggregate);
    NativeExecution.opened(rank);
    NativeExecution.completed(aggregate, 8);
    NativeExecution.completed(rank, 12);
    NativeExecution.finish(scope);
  }

  @Test
  void rejectsOverlappingInvocations() {
    NativeExecution.Scope scope = begin(CALC);
    assertThrows(AssertionError.class, () -> begin(WINDOW));
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void reportsAreRequiredEvenWhenNativeWorkSucceeded() {
    NativeExecution.Scope scope = begin(CALC);
    Object calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 1);
    System.clearProperty("streamfusion.flink-suite.native-reports");
    assertTrue(
        assertThrows(AssertionError.class, () -> NativeExecution.finish(scope))
            .getMessage()
            .contains("Missing streamfusion.flink-suite.native-reports"));
  }

  private static NativeExecution.Scope begin(String test) {
    return NativeExecution.begin(test, new WindowFixture(false));
  }

  private static class WindowFixture {
    private final String state = "HEAP";
    private final FakeScalaEnvironment env = new FakeScalaEnvironment(false);
    private final boolean splitDistinct;

    WindowFixture(boolean splitDistinct) {
      this.splitDistinct = splitDistinct;
    }
  }

  @Test
  void expectedFallbackRequiresItsSpecificReasonAndNoNativeWork() {
    NativeExecution.Scope scope = begin(FALLBACK);
    NativeExecution.fallback(FALLBACK_REASON);
    NativeExecution.finish(scope);
    scope = begin(FALLBACK);
    NativeExecution.fallback("different unsupported function");
    NativeExecution.Scope wrongReason = scope;
    assertThrows(AssertionError.class, () -> NativeExecution.finish(wrongReason));
    scope = begin(FALLBACK);
    NativeExecution.fallback(FALLBACK_REASON);
    Object window = new NativeColumnarWindowAggregateOperator();
    NativeExecution.opened(window);
    NativeExecution.completed(window, 1);
    NativeExecution.Scope unexpectedlyNative = scope;
    assertThrows(AssertionError.class, () -> NativeExecution.finish(unexpectedlyNative));
  }

  @Test
  void aChangedUpstreamFixtureFailsInsteadOfSkippingItsContract() {
    assertThrows(AssertionError.class, () -> NativeExecution.begin(WINDOW, new Object()));
  }

  @Test
  void selectorsReadInheritedStateAndConjoinFixtureParameters() {
    class BackendFixture {
      private final String state = "HEAP";
    }
    class WindowBackendFixture extends BackendFixture {
      private final boolean splitDistinct = false;
    }
    Object fixture = new WindowBackendFixture();
    assertTrue(NativeExecution.matches("state=HEAP&splitDistinct=false", fixture));
    assertFalse(NativeExecution.matches("state=ROCKSDB&splitDistinct=false", fixture));
    assertFalse(NativeExecution.matches("state=HEAP&splitDistinct=true", fixture));
    assertThrows(AssertionError.class, () -> NativeExecution.matches("missing=true", fixture));
  }

  @Test
  void selectorsObserveTheFixturesActualRandomizedChangelogConfiguration() {
    class Fixture {
      private final FakeScalaEnvironment env;

      Fixture(boolean enabled) {
        env = new FakeScalaEnvironment(enabled);
      }
    }
    assertTrue(NativeExecution.matches("changelog=true", new Fixture(true)));
    assertFalse(NativeExecution.matches("changelog=false", new Fixture(true)));
    assertTrue(NativeExecution.matches("changelog=false", new Fixture(false)));
    assertThrows(
        AssertionError.class, () -> NativeExecution.matches("changelog=true", new Object()));
  }

  public static final class FakeScalaEnvironment {
    private final FakeJavaEnvironment javaEnvironment;

    FakeScalaEnvironment(boolean enabled) {
      javaEnvironment = new FakeJavaEnvironment(enabled);
    }

    public FakeJavaEnvironment getJavaEnv() {
      return javaEnvironment;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selectorsReadThePinnedMiniBatchModeInsteadOfItsDisplayName(boolean enabled) {
    class Fixture {
      private final FakeMiniBatchMode miniBatch = new FakeMiniBatchMode(enabled);
      private final boolean enableAsyncState = false;
    }
    var fixture = new Fixture();
    assertTrue(
        NativeExecution.matches("miniBatch=" + enabled + "&enableAsyncState=false", fixture));
    assertFalse(NativeExecution.matches("miniBatch=" + !enabled, fixture));
    assertThrows(
        AssertionError.class, () -> NativeExecution.matches("miniBatch=true", new Object()));
  }

  public static final class FakeMiniBatchMode {
    private final boolean enabled;

    FakeMiniBatchMode(boolean enabled) {
      this.enabled = enabled;
    }

    public boolean on() {
      return enabled;
    }

    @Override
    public String toString() {
      return "MiniBatch=" + (enabled ? "ON" : "OFF");
    }
  }

  @Test
  void collectRequiresItsOwnConsumedGroupWork() {
    class Fixture {
      private final FakeMiniBatchMode miniBatch = new FakeMiniBatchMode(false);
      private final boolean enableAsyncState = false;
      private final FakeScalaEnvironment env = new FakeScalaEnvironment(false);
    }
    String test =
        "org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase#testUnboundedGroupByCollect";
    var first = NativeExecution.begin(test, new Fixture());
    Object calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 21);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(first));
    var second = NativeExecution.begin(test, new Fixture());
    Object group = new NativeColumnarGroupAggregateOperator();
    NativeExecution.opened(group);
    NativeExecution.completed(group, 21);
    NativeExecution.finish(second);
  }

  public static final class FakeJavaEnvironment {
    private final boolean enabled;

    FakeJavaEnvironment(boolean enabled) {
      this.enabled = enabled;
    }

    public String isChangelogStateBackendEnabled() {
      return enabled ? "TRUE" : "FALSE";
    }
  }

  @Test
  void assumptionSkippedInvocationsLeaveNoEvidence() throws Exception {
    NativeExecution.Scope skipped = begin(CALC);
    assertTrue(NativeExecution.skipped(new org.opentest4j.TestAbortedException("assumption")));
    assertTrue(NativeExecution.skipped(new VersionAssumption()));
    assertFalse(NativeExecution.skipped(new AssertionError("failure")));

    NativeExecution.abandon(skipped);

    try (var files = Files.list(reports)) {
      assertEquals(0, files.count());
    }
    NativeExecution.Scope next = begin(CALC);
    Object calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 1);
    NativeExecution.finish(next);
  }

  private static final class VersionAssumption extends org.opentest4j.TestAbortedException {}

  @Test
  void arrowBatchesAreCountedWithoutTakingTheirRoot() throws Exception {
    NativeExecution.Scope scope = begin(CALC);
    Object encoder = new NativeCalcOperator();
    NativeExecution.opened(encoder);

    assertEquals(5, NativeExecution.arrowBatchRows(encoder, new MeasuredBatch(5)));
    assertEquals(0, NativeExecution.arrowBatchRows(new NativeCalcOperator(), new MeasuredBatch(5)));
    NativeExecution.completed(encoder, 5);
    NativeExecution.finish(scope);
  }

  public static final class MeasuredBatch {
    private final int rows;

    MeasuredBatch(int rows) {
      this.rows = rows;
    }

    public int rowCount() {
      return rows;
    }

    public Object root() {
      throw new AssertionError("Measuring a batch must not take its root");
    }
  }

  private static class NativeCalcOperator {}

  private static class NativeColumnarGroupAggregateOperator {}

  private static class NativeColumnarJvmGroupOperator {}

  private static class NativeColumnarTopNOperator {}

  private static class NativeColumnarWindowAggregateOperator {}

  private static class NativeColumnarLocalWindowAggregateOperator {}

  private static class NativeColumnarGlobalWindowAggregateOperator {}
}
