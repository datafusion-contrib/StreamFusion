package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "streamfusion.flink-suite.canonical", matches = "true")
class CanonicalRestoreExecutionTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path reports;

  @org.junit.jupiter.api.BeforeEach
  void configureReports() {
    System.setProperty("streamfusion.flink-suite.native-reports", reports.toString());
  }

  private static final String GROUP =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.GroupAggregateRestoreTest";
  private static final String JOIN =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.JoinRestoreTest";
  private static final String RESTORE_ONLY_JOIN = "semi-anti-join-with-literal-agg";
  private static final String NORMALIZE =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.ChangelogNormalizeRestoreTest";
  private static final String FILTER_REASON =
      "changelog normalize: a pushed filter condition is not supported";

  @Test
  void windowDeduplicationRequiresItsNativeSelectionOperator() {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowDeduplicateEventTimeRestoreTest",
            new Object(),
            "window-deduplicate-asc-tumble-first-row");
    var operator = new NativeColumnarWindowRankOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @Test
  void anotherNativeFamilyCannotProveWindowDeduplication() {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowDeduplicateEventTimeRestoreTest",
            new Object(),
            "window-deduplicate-asc-tumble-first-row");
    var operator = new NativeCalcOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void openingWindowSelectionWithoutInputDoesNotProveDeduplication() {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowDeduplicateEventTimeRestoreTest",
            new Object(),
            "window-deduplicate-asc-tumble-first-row");
    NativeExecution.opened(new NativeColumnarWindowRankOperator());
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void emptyInputFixtureRequiresCompletedNativeImportAndOpenedOperator() throws Exception {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    NativeExecution.restored("NativeColumnarUpdatingJoinOperator", 3);
    NativeExecution.opened(new NativeColumnarUpdatingJoinOperator());
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\t", -1);
      assertEquals("", fields[2], "Imported state must not be reported as input records");
      assertEquals("NativeColumnarUpdatingJoinOperator=3", fields[4]);
    }
  }

  @Test
  void importWithoutSuccessfulOpenDoesNotProveRestore() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    NativeExecution.restored("NativeColumnarUpdatingJoinOperator", 3);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void operatorOpenWithoutImportedStateDoesNotProveRestore() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    NativeExecution.opened(new NativeColumnarUpdatingJoinOperator());
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void anotherNativeFamilyCannotProveJoinRestore() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    NativeExecution.restored("NativeCalcOperator", 3);
    NativeExecution.opened(new NativeCalcOperator());
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void restoredStateCannotReplaceConsumedInputInAContinuationFixture() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), "join-left-join");
    NativeExecution.restored("NativeColumnarUpdatingJoinOperator", 3);
    NativeExecution.opened(new NativeColumnarUpdatingJoinOperator());
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void restoreOnlyFixtureCannotSilentlyStartConsumingRecords() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    var operator = new NativeColumnarUpdatingJoinOperator();
    NativeExecution.restored("NativeColumnarUpdatingJoinOperator", 3);
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 1);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void aPlannerFallbackCannotSatisfyTheRestoreOnlyFixture() {
    var scope = NativeExecution.beginRestore(JOIN, new Object(), RESTORE_ONLY_JOIN);
    NativeExecution.restored("NativeColumnarUpdatingJoinOperator", 3);
    NativeExecution.opened(new NativeColumnarUpdatingJoinOperator());
    NativeExecution.fallback("join unsupported");
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void supportedRestoreProgramRequiresItsNativeStateFamily() {
    var scope = NativeExecution.beginRestore(NORMALIZE, new Object(), "changelog-normalize-upsert");
    var operator = new NativeColumnarChangelogNormalizeOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @Test
  void anUnsupportedStateFamilyCanFallBackAlongsideOtherNativeWork() {
    var scope =
        NativeExecution.beginRestore(NORMALIZE, new Object(), "changelog-normalize-upsert-filter");
    var operator = new NativeCalcOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    NativeExecution.fallback(FILTER_REASON);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @Test
  void unexpectedNativeExecutionCannotSatisfyAnExpectedStateFamilyFallback() {
    var scope =
        NativeExecution.beginRestore(NORMALIZE, new Object(), "changelog-normalize-upsert-filter");
    var operator = new NativeColumnarChangelogNormalizeOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    NativeExecution.fallback(FILTER_REASON);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void missingFallbackEvidenceCannotBeHiddenByUnrelatedNativeWork() {
    var scope =
        NativeExecution.beginRestore(NORMALIZE, new Object(), "changelog-normalize-upsert-filter");
    var operator = new NativeCalcOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void inheritedRestoreMethodUsesItsConcreteStateFamily() {
    var scope = NativeExecution.beginRestore(GROUP, new Object());
    assertNotNull(scope);
    var operator = new NativeColumnarGroupAggregateOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @Test
  void unrelatedNativeWorkCannotSatisfyAStateRestore() {
    var scope = NativeExecution.beginRestore(GROUP, new Object());
    var operator = new NativeCalcOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    var failure = assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
    assertTrue(failure.getMessage().contains("Execution contract failed"));
  }

  @Test
  void connectorOnlyFixturesDoNotRequireANativeStateOperator() {
    assertNull(
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.TableSourceScanRestoreTest",
            new Object()));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"true,false", "false,true", "true,true"})
  void splitWindowsRequireBothSliceAndFinalGlobalWork(boolean slice, boolean finalWindow) {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowAggregateEventTimeRestoreTest",
            new Object(),
            "window-aggregate-hop-event-time-two-phase-distinct-split");
    var local = new NativeColumnarLocalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.completed(local, 4);
    if (slice) {
      var operator = new NativeColumnarGlobalWindowAggregateOperator(false);
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 3);
    }
    if (finalWindow) {
      var operator = new NativeColumnarGlobalWindowAggregateOperator(true);
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 2);
    }
    if (slice && finalWindow) assertDoesNotThrow(() -> NativeExecution.finish(scope));
    else assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void unknownWindowProgramsCannotFallThroughToASubsetWitness() {
    assertThrows(
        AssertionError.class,
        () ->
            NativeExecution.beginRestore(
                "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowAggregateEventTimeRestoreTest",
                new Object(),
                "new-unqualified-window-program"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "window-aggregate-session-event-time",
        "window-aggregate-session-event-time-two-phase",
        "window-aggregate-session-event-time-two-phase-distinct-split"
      })
  void unpartitionedSessionsRequireBothStockStateAndNativeGroupWork(String program)
      throws Exception {
    var scope = windowScope(program);
    var aggregate = new NativeColumnarGroupAggregateOperator();
    NativeExecution.opened(aggregate);
    NativeExecution.completed(aggregate, 3);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
    var onlyStock = windowScope(program);
    var tableFunction = new UnalignedWindowTableFunctionOperator();
    NativeExecution.stockOpened(tableFunction);
    NativeExecution.stockCompleted(tableFunction);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(onlyStock));
    var both = windowScope(program);
    NativeExecution.opened(aggregate);
    NativeExecution.completed(aggregate, 3);
    NativeExecution.stockOpened(tableFunction);
    NativeExecution.stockCompleted(tableFunction);
    assertDoesNotThrow(() -> NativeExecution.finish(both));
    try (var files = java.nio.file.Files.list(reports)) {
      assertTrue(
          files.anyMatch(
              path -> {
                try {
                  String[] fields =
                      java.nio.file.Files.readString(path).stripTrailing().split("\t", -1);
                  return fields.length == 6
                      && fields[2].equals("NativeColumnarGroupAggregateOperator=3")
                      && fields[5].equals("StockSessionTableFunction=1");
                } catch (java.io.IOException failure) {
                  throw new java.io.UncheckedIOException(failure);
                }
              }));
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"reason", "stock", "calc", "unexpected", "none"})
  void unsupportedRetractingSessionsRequireTheirRealOwnerAndExactFallback(String missing) {
    var scope =
        windowScope("window-aggregate-session-partition-event-time-two-phase-distinct-split");
    if (!missing.equals("reason"))
      NativeExecution.fallback("window aggregate: retracting state contract is not bound");
    if (!missing.equals("calc")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 3);
    }
    if (!missing.equals("stock")) {
      var stock = new WindowAggOperator();
      NativeExecution.stockOpened(stock);
      NativeExecution.stockCompleted(stock);
    }
    if (missing.equals("unexpected")) {
      var nativeWindow = new NativeColumnarSessionWindowAggregateOperator();
      NativeExecution.opened(nativeWindow);
      NativeExecution.completed(nativeWindow, 1);
    }
    if (missing.equals("none")) assertDoesNotThrow(() -> NativeExecution.finish(scope));
    else assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void stockSessionRowsCannotReplaceAnAdmittedNativeSessionAggregate() {
    var scope = windowScope("window-aggregate-session-partition-event-time");
    var stock = new WindowAggOperator();
    NativeExecution.stockOpened(stock);
    NativeExecution.stockCompleted(stock);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void stockWorkFromACompletedInvocationCannotProveTheNextInvocation() {
    String program = "window-aggregate-session-event-time";
    var first = windowScope(program);
    var group = new NativeColumnarGroupAggregateOperator();
    var stock = new UnalignedWindowTableFunctionOperator();
    NativeExecution.opened(group);
    NativeExecution.completed(group, 3);
    NativeExecution.stockOpened(stock);
    NativeExecution.stockCompleted(stock);
    NativeExecution.finish(first);
    var second = windowScope(program);
    NativeExecution.opened(group);
    NativeExecution.completed(group, 3);
    NativeExecution.stockCompleted(stock);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(second));
  }

  private static NativeExecution.Scope windowScope(String program) {
    return NativeExecution.beginRestore(
        "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowAggregateEventTimeRestoreTest",
        new Object(),
        program);
  }

  private static final class SessionWindowAssigner {}

  private static final class UnalignedWindowTableFunctionOperator {
    private final Object windowAssigner = new SessionWindowAssigner();
    private final Object windowState = new Object();
    private final Object counterState = new Object();
  }

  private static final class WindowAggOperator {
    private final Object windowProcessor = new UnsliceSyncStateWindowAggProcessor();
  }

  private static final class UnsliceSyncStateWindowAggProcessor {
    private final Object windowState = new Object();
  }

  private static final class NativeColumnarSessionWindowAggregateOperator {}

  private static final class NativeColumnarLocalWindowAggregateOperator {}

  public static final class NativeColumnarGlobalWindowAggregateOperator {
    private final boolean finalWindow;

    public NativeColumnarGlobalWindowAggregateOperator(boolean value) {
      finalWindow = value;
    }

    public boolean isFinalWindow() {
      return finalWindow;
    }
  }

  private static final class NativeColumnarGroupAggregateOperator {}

  private static final class NativeCalcOperator {}

  private static final class NativeColumnarChangelogNormalizeOperator {}

  private static final class NativeColumnarUpdatingJoinOperator {}

  private static final class NativeColumnarWindowRankOperator {}
}
