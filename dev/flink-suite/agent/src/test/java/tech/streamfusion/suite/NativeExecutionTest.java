package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeExecutionTest {
  private static final String CALC =
      "org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testLongProjectionList";
  private static final String WINDOW =
      "org.apache.flink.table.planner.runtime.stream.sql.WindowDistinctAggregateITCase#testHopWindow";

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

  @Test
  void twoPhaseWindowsRequireBothHalves() {
    NativeExecution.Scope first = begin(WINDOW);
    Object local = new NativeColumnarLocalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.completed(local, 5);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(first));
    NativeExecution.Scope second = begin(WINDOW);
    Object global = new NativeColumnarGlobalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.opened(global);
    NativeExecution.completed(local, 5);
    NativeExecution.completed(global, 2);
    NativeExecution.finish(second);
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
    private final boolean splitDistinct;

    WindowFixture(boolean splitDistinct) {
      this.splitDistinct = splitDistinct;
    }
  }

  @Test
  void expectedFallbackRequiresItsSpecificReasonAndNoNativeWork() {
    NativeExecution.Scope scope = NativeExecution.begin(WINDOW, new WindowFixture(true));
    NativeExecution.fallback("window aggregate: attached-window aggregation requires two-phase execution");
    NativeExecution.finish(scope);
    scope = NativeExecution.begin(WINDOW, new WindowFixture(true));
    NativeExecution.fallback("different unsupported function");
    NativeExecution.Scope wrongReason = scope;
    assertThrows(AssertionError.class, () -> NativeExecution.finish(wrongReason));
    scope = NativeExecution.begin(WINDOW, new WindowFixture(true));
    NativeExecution.fallback("window aggregate: attached-window aggregation requires two-phase execution");
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

  private static class NativeCalcOperator {}

  private static class NativeColumnarGroupAggregateOperator {}

  private static class NativeColumnarTopNOperator {}

  private static class NativeColumnarWindowAggregateOperator {}

  private static class NativeColumnarLocalWindowAggregateOperator {}

  private static class NativeColumnarGlobalWindowAggregateOperator {}
}
