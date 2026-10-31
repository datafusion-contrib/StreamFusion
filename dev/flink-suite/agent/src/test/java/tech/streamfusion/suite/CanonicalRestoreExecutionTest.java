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

  private static final class NativeColumnarGroupAggregateOperator {}

  private static final class NativeCalcOperator {}

  private static final class NativeColumnarChangelogNormalizeOperator {}

  private static final class NativeColumnarUpdatingJoinOperator {}
}
