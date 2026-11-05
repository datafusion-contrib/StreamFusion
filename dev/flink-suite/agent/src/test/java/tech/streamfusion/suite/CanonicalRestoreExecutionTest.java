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

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> windowScopes() {
    return java.util.stream.Stream.of(
        org.junit.jupiter.params.provider.Arguments.of(
            "WindowRankRestoreTest", "window-rank-tumble-tvf-min-top-n"),
        org.junit.jupiter.params.provider.Arguments.of(
            "WindowDeduplicateEventTimeRestoreTest", "window-deduplicate-asc-tumble-first-row"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("windowScopes")
  void windowImportAndNewInputAreBothRequired(String owner, String program) throws Exception {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream." + owner,
            new Object(),
            program);
    var operator = new NativeColumnarWindowRankOperator();
    StreamFusionSuiteAgent.RecordCanonicalWindowRankImport.exit(java.util.List.of(1, 2));
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 4);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("NativeColumnarWindowRankOperator=4", fields[2]);
      assertEquals("NativeColumnarWindowRankOperator=2", fields[4]);
    }
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> missingWindowWork() {
    return windowScopes()
        .flatMap(
            args ->
                java.util.stream.Stream.of(
                        "none", "openOnly", "importOnly", "inputOnly", "otherFamily")
                    .map(
                        mode ->
                            org.junit.jupiter.params.provider.Arguments.of(
                                args.get()[0], args.get()[1], mode)));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("missingWindowWork")
  void unrelatedOrIncompleteWindowWorkCannotProveRestore(
      String owner, String program, String mode) {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream." + owner,
            new Object(),
            program);
    var operator = new NativeColumnarWindowRankOperator();
    if (!mode.equals("none")) NativeExecution.opened(operator);
    if (mode.equals("importOnly"))
      StreamFusionSuiteAgent.RecordCanonicalWindowRankImport.exit(java.util.List.of(1, 2));
    if (mode.equals("inputOnly")) NativeExecution.completed(operator, 4);
    if (mode.equals("otherFamily")) {
      var other = new NativeColumnarTopNOperator();
      NativeExecution.opened(other);
      NativeExecution.restored("NativeColumnarTopNOperator", 2);
      NativeExecution.completed(other, 4);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
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

  private static final String WINDOW_JOIN =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowJoinEventTimeRestoreTest";
  private static final String WINDOW_REASON =
      "window join: semi/anti retain the released stock state owner";

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"SEMI", "ANTI"})
  void windowSemiAntiRequiresTheActualReleasedStockListsAndTimers(String kind) {
    var scope =
        NativeExecution.beginRestore(
            WINDOW_JOIN,
            new Object(),
            "window-join-" + kind.toLowerCase(java.util.Locale.ROOT) + "-tumble-event-time");
    var join = new WindowJoinOperator(kind, true);
    NativeExecution.stockOpened(join);
    NativeExecution.stockCompleted(join);
    NativeExecution.fallback(WINDOW_REASON);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"empty", "openOnly", "missingFallback", "nativeJoin", "wrongKind", "missingState"})
  void windowSemiAntiCannotPassFromMissingOrUnrelatedWork(String mode) {
    var scope =
        NativeExecution.beginRestore(
            WINDOW_JOIN, new Object(), "window-join-semi-tumble-event-time");
    if (!mode.equals("empty")) {
      var join =
          new WindowJoinOperator(
              mode.equals("wrongKind") ? "INNER" : "SEMI", !mode.equals("missingState"));
      if (mode.equals("wrongKind") || mode.equals("missingState")) {
        assertThrows(AssertionError.class, () -> NativeExecution.stockOpened(join));
      } else {
        NativeExecution.stockOpened(join);
        if (!mode.equals("openOnly")) NativeExecution.stockCompleted(join);
      }
    }
    if (mode.equals("nativeJoin")) {
      var join = new NativeWindowJoinOperator();
      NativeExecution.opened(join);
      NativeExecution.completed(join, 3);
    }
    if (!mode.equals("missingFallback")) NativeExecution.fallback(WINDOW_REASON);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private static final class WindowJoinOperator {
    final WindowJoinKind joinType;
    final Object leftWindowState, rightWindowState, windowTimerService, helper;

    WindowJoinOperator(String kind, boolean initialized) {
      joinType = WindowJoinKind.valueOf(kind);
      leftWindowState = initialized ? new Object() : null;
      rightWindowState = initialized ? new Object() : null;
      windowTimerService = initialized ? new Object() : null;
      helper = initialized ? new Object() : null;
    }
  }

  private enum WindowJoinKind {
    INNER,
    SEMI,
    ANTI
  }

  private static final class NativeWindowJoinOperator {}

  private static final String TEMPORAL =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.TemporalJoinRestoreTest";

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "temporal-join-table-join",
        "temporal-join-table-join-nested-key",
        "temporal-join-table-join-key-from-map",
        "temporal-join-temporal-function"
      })
  void temporalRestoreRequiresActualNativeTemporalInput(String program) {
    var scope = NativeExecution.beginRestore(TEMPORAL, new Object(), program);
    var join = new NativeTemporalJoinOperator();
    NativeExecution.opened(join);
    NativeExecution.restored("NativeTemporalJoinOperator", 3);
    NativeExecution.completed(join, 2);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"none", "open", "importOnly", "calc", "interval", "fallback"})
  void missingOrUnrelatedWorkCannotQualifyTemporalRestore(String mode) {
    var scope = NativeExecution.beginRestore(TEMPORAL, new Object(), "temporal-join-table-join");
    if (mode.equals("open") || mode.equals("importOnly")) {
      NativeExecution.opened(new NativeTemporalJoinOperator());
      if (mode.equals("importOnly")) NativeExecution.restored("NativeTemporalJoinOperator", 3);
    } else if (mode.equals("calc")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 2);
    } else if (mode.equals("interval")) {
      var join = new NativeIntervalJoinOperator();
      NativeExecution.opened(join);
      NativeExecution.completed(join, 2);
    } else if (mode.equals("fallback")) NativeExecution.fallback("temporal unsupported");
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void unknownTemporalProgramsCannotInheritAKnownWitness() {
    assertThrows(
        AssertionError.class,
        () -> NativeExecution.beginRestore(TEMPORAL, new Object(), "temporal-join-unknown"));
  }

  private static final class NativeTemporalJoinOperator {}

  private static final String INTERVAL =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.IntervalJoinRestoreTest";

  @Test
  void intervalRestoreRequiresItsActualNativeInputOwner() {
    var scope = NativeExecution.beginRestore(INTERVAL, new Object(), "interval-join-event-time");
    var interval = new NativeIntervalJoinOperator();
    NativeExecution.opened(interval);
    NativeExecution.restored("NativeIntervalJoinOperator", 3);
    NativeExecution.completed(interval, 2);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"none", "open", "importOnly", "calc", "window"})
  void unrelatedWorkCannotSatisfyIntervalRestore(String mode) {
    var scope = NativeExecution.beginRestore(INTERVAL, new Object(), "interval-join-event-time");
    if (mode.equals("open") || mode.equals("importOnly")) {
      NativeExecution.opened(new NativeIntervalJoinOperator());
      if (mode.equals("importOnly")) NativeExecution.restored("NativeIntervalJoinOperator", 3);
    } else if (mode.equals("calc")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 3);
    } else if (mode.equals("window")) {
      var window = new NativeWindowJoinOperator();
      NativeExecution.opened(window);
      NativeExecution.completed(window, 3);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void impossibleIntervalRequiresSubmittedJoinFreeGraphAndActualCalcWork() throws Exception {
    var scope =
        NativeExecution.beginRestore(INTERVAL, new Object(), "interval-join-negative-interval");
    NativeExecution.submitted(
        NativeExecution.submitting(new Object[] {passthroughGraph()}), new SubmittedClient(), null);
    var calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 3);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "empty",
        "noCalc",
        "noGraph",
        "failedSubmission",
        "nativeJoin",
        "import",
        "fallback"
      })
  void impossibleIntervalRejectsMissingProofAndStatefulWork(String mode) throws Exception {
    var scope =
        NativeExecution.beginRestore(INTERVAL, new Object(), "interval-join-negative-interval");
    if (!mode.equals("empty") && !mode.equals("noGraph"))
      NativeExecution.submitted(
          NativeExecution.submitting(new Object[] {passthroughGraph()}),
          new SubmittedClient(),
          mode.equals("failedSubmission") ? new IllegalStateException() : null);
    if (!mode.equals("empty") && !mode.equals("noCalc")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 3);
    }
    if (mode.equals("nativeJoin")) NativeExecution.opened(new NativeIntervalJoinOperator());
    if (mode.equals("import")) NativeExecution.restored("NativeIntervalJoinOperator", 3);
    if (mode.equals("fallback")) NativeExecution.fallback("interval unsupported");
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator",
        "tech.streamfusion.operator.NativeIntervalJoinOperator",
        "org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin"
      })
  void impossibleIntervalGraphCannotHideAJoinOwner(String operator) {
    var graph =
        java.util.Map.<String, Object>of(
            "nodes",
            java.util.List.of(
                java.util.Map.of(
                    "id",
                    1,
                    "operator",
                    "SourceOperator",
                    "inputs",
                    java.util.List.of(),
                    "outputs",
                    java.util.List.of(2)),
                java.util.Map.of(
                    "id",
                    2,
                    "operator",
                    operator,
                    "inputs",
                    java.util.List.of(1),
                    "outputs",
                    java.util.List.of(3)),
                java.util.Map.of(
                    "id",
                    3,
                    "operator",
                    "StreamSink",
                    "inputs",
                    java.util.List.of(2),
                    "outputs",
                    java.util.List.of())));
    assertFalse(ElidedIntervalJoin.valid(graph));
  }

  @Test
  void unknownIntervalProgramsCannotClaimElision() {
    assertThrows(
        AssertionError.class,
        () -> NativeExecution.beginRestore(INTERVAL, new Object(), "interval-join-unknown"));
  }

  private static final class NativeIntervalJoinOperator {}

  private static final String SORT = ElidedTemporalSort.TEST.replace("#testRestore", "");

  private static final String SORT_REASON =
      "temporal sort: secondary order keys require the stock sorter";

  @Test
  void secondaryOrderingKeepsTheRealStockStateAndComparatorAlongsideNativeCalc() {
    var scope = NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-rowtime");
    var calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 3);
    var sort = new RowTimeSortOperator(true);
    NativeExecution.stockOpened(sort);
    NativeExecution.stockCompleted(sort);
    NativeExecution.fallback(SORT_REASON);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "empty",
        "calc",
        "nativeSort",
        "openStock",
        "stockOnly",
        "noFallback",
        "wrongFallback",
        "nativeImport"
      })
  void unrelatedWorkAndMissingStockStateCannotProveTheSecondaryOrder(String mode) {
    var scope = NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-rowtime");
    if (!mode.equals("empty") && !mode.equals("stockOnly")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 3);
    }
    if (!mode.equals("empty") && !mode.equals("calc")) {
      var sort = new RowTimeSortOperator(true);
      NativeExecution.stockOpened(sort);
      if (!mode.equals("openStock")) NativeExecution.stockCompleted(sort);
    }
    if (mode.equals("nativeSort") || mode.equals("nativeImport")) {
      var sort = new NativeColumnarTemporalSortOperator();
      NativeExecution.opened(sort);
      if (mode.equals("nativeSort")) NativeExecution.completed(sort, 3);
      else NativeExecution.restored("NativeColumnarTemporalSortOperator", 3);
    }
    if (!mode.equals("noFallback"))
      NativeExecution.fallback(mode.equals("wrongFallback") ? "other sort fallback" : SORT_REASON);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void stockSortMustHaveManagedStateAndARealSecondaryComparator() {
    var scope = NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-rowtime");
    assertThrows(
        AssertionError.class, () -> NativeExecution.stockOpened(new RowTimeSortOperator(false)));
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private static final class RowTimeSortOperator {
    final Object dataState = new Object();
    final Object lastTriggeringTsState = new Object();
    final Object comparator;

    RowTimeSortOperator(boolean comparator) {
      this.comparator = comparator ? new Object() : null;
    }
  }

  @Test
  void unknownTemporalSortProgramsCannotUseTheElidedOrderingContract() {
    assertThrows(
        AssertionError.class,
        () -> NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-unknown"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void proctimeOrderingRequiresARealSubmittedGraphWithoutASort(boolean calc) throws Exception {
    var scope = NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-proctime");
    var graph = passthroughGraph();
    NativeExecution.submitted(
        NativeExecution.submitting(new Object[] {graph}), new SubmittedClient(), null);
    if (calc) {
      var operator = new NativeCalcOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 3);
    }
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\t", -1);
      assertEquals(7, fields.length);
      String evidence =
          new String(
              java.util.Base64.getDecoder().decode(fields[6]),
              java.nio.charset.StandardCharsets.UTF_8);
      assertTrue(evidence.contains("submitted_graphs"), evidence);
      assertTrue(evidence.contains("StreamSink"), evidence);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "empty",
        "failedSubmission",
        "nullClient",
        "stockSort",
        "nativeSort",
        "import",
        "fallback"
      })
  void proctimeElisionRejectsMissingJobsAndAnySortState(String mode) throws Exception {
    var scope = NativeExecution.beginRestore(SORT, new Object(), "temporal-sort-proctime");
    if (!mode.equals("empty")) {
      var graph = passthroughGraph();
      if (mode.equals("stockSort")) graph = stockSortGraph();
      NativeExecution.submitted(
          NativeExecution.submitting(new Object[] {graph}),
          mode.equals("nullClient") ? null : new SubmittedClient(),
          mode.equals("failedSubmission") ? new IllegalStateException("submission failed") : null);
    }
    if (mode.equals("nativeSort")) NativeExecution.opened(new NativeColumnarTemporalSortOperator());
    if (mode.equals("import")) NativeExecution.restored("NativeColumnarTemporalSortOperator", 3);
    if (mode.equals("fallback")) NativeExecution.fallback("unexpected sort fallback");
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private static org.apache.flink.streaming.api.graph.StreamGraph passthroughGraph() {
    var env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().disableClosureCleaner();
    env.fromElements(1, 2)
        .map(value -> value + 1)
        .returns(org.apache.flink.api.common.typeinfo.Types.INT)
        .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());
    return env.getStreamGraph();
  }

  private static org.apache.flink.streaming.api.graph.StreamGraph stockSortGraph() {
    var env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment();
    env.setParallelism(1);
    var type =
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
            org.apache.flink.table.types.logical.RowType.of(
                new org.apache.flink.table.types.logical.BigIntType()));
    env.<org.apache.flink.table.data.RowData>fromCollection(
            java.util.List.of(org.apache.flink.table.data.GenericRowData.of(1L)), type)
        .keyBy(org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector.INSTANCE)
        .transform(
            "stock-sort",
            type,
            new org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator(type, 0, null))
        .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());
    return env.getStreamGraph();
  }

  public static final class SubmittedClient {
    public String getJobID() {
      return "submitted-graph-job";
    }
  }

  private static final class NativeColumnarTemporalSortOperator {}

  @Test
  void windowDeduplicationRequiresItsNativeSelectionOperator() {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowDeduplicateEventTimeRestoreTest",
            new Object(),
            "window-deduplicate-asc-tumble-first-row");
    var operator = new NativeColumnarWindowRankOperator();
    StreamFusionSuiteAgent.RecordCanonicalWindowRankImport.exit(java.util.List.of(1, 2));
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

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"deduplicate-asc-proctime", "deduplicate-desc"})
  void eagerDedupAuditsCommittedImportsSeparatelyFromValidationAndNewInput(String program)
      throws Exception {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.EagerDedupRestoreTest", new Object(), program);
    var operator = new NativeColumnarKeepLastDeduplicateOperator();
    StreamFusionSuiteAgent.RecordCanonicalEagerDedupImport.exit(java.util.List.of(1, 2, 3), true);
    StreamFusionSuiteAgent.RecordCanonicalEagerDedupImport.exit(java.util.List.of(1, 2), false);
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 4);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("NativeColumnarKeepLastDeduplicateOperator=4", fields[2]);
      assertEquals("NativeColumnarKeepLastDeduplicateOperator=2", fields[4]);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"empty", "openOnly", "importOnly", "otherFamily"})
  void eagerDedupCannotPassWithoutItsOwnNewInput(String mode) {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.EagerDedupRestoreTest", new Object(), "deduplicate-desc");
    if (mode.equals("otherFamily")) {
      var operator = new NativeColumnarChangelogNormalizeOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 3);
    } else if (!mode.equals("empty")) {
      NativeExecution.opened(new NativeColumnarKeepLastDeduplicateOperator());
      if (mode.equals("importOnly"))
        StreamFusionSuiteAgent.RecordCanonicalEagerDedupImport.exit(java.util.List.of(1, 2), false);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void bufferedImportAdviceCountsMutationsButCannotSatisfyTheLegacyEagerFixture() throws Exception {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.DeduplicationRestoreTest",
            new Object(),
            "deduplicate-asc");
    StreamFusionSuiteAgent.RecordCanonicalBufferedDedupImport.exit(
        java.util.List.of(1, 2, 3), true);
    StreamFusionSuiteAgent.RecordCanonicalBufferedDedupImport.exit(java.util.List.of(1, 2), false);
    var operator = new NativeColumnarDeduplicateOperator();
    NativeExecution.opened(operator);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("", fields[2]);
      assertEquals("NativeColumnarDeduplicateOperator=2", fields[4]);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"empty", "openOnly", "importOnly", "bufferedFamily"})
  void legacyEagerDedupCannotPassFromUnrelatedOrMissingInput(String mode) {
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.DeduplicationRestoreTest",
            new Object(),
            "deduplicate-asc");
    if (mode.equals("bufferedFamily")) {
      var operator = new NativeColumnarDeduplicateOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 4);
    } else if (!mode.equals("empty")) {
      NativeExecution.opened(new NativeColumnarKeepLastDeduplicateOperator());
      if (mode.equals("importOnly"))
        StreamFusionSuiteAgent.RecordCanonicalBufferedDedupImport.exit(java.util.List.of(1), false);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private static final class NativeColumnarDeduplicateOperator {}

  private static final class NativeColumnarKeepLastDeduplicateOperator {}

  @Test
  void appendRankAuditsCommittedImportsSeparatelyFromValidationAndNewInput() throws Exception {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.AppendRankRestoreTest",
            new Object(),
            "rank-test-append-fast-strategy");
    var operator = new NativeColumnarTopNOperator();
    StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1, 2, 3), true);
    StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1, 2), false);
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 4);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("NativeColumnarTopNOperator=4", fields[2]);
      assertEquals("NativeColumnarTopNOperator=2", fields[4]);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"empty", "openOnly", "importOnly", "firstN"})
  void appendRankCannotPassWithoutItsOwnNewInput(String mode) {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.AppendRankRestoreTest",
            new Object(),
            "rank-test-append-fast-strategy");
    if (mode.equals("firstN")) {
      var operator = new NativeColumnarFirstNOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 4);
    } else if (!mode.equals("empty")) {
      NativeExecution.opened(new NativeColumnarTopNOperator());
      if (mode.equals("importOnly"))
        StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1), false);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void updatingRankAuditsCommittedImportsSeparatelyFromValidationAndNewInput() throws Exception {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.UpdateRankRestoreTest",
            new Object(),
            "rank-test-update-fast-strategy");
    var operator = new NativeColumnarTopNOperator();
    StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1, 2, 3), true);
    StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1, 2), false);
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 4);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("NativeColumnarTopNOperator=4", fields[2]);
      assertEquals("NativeColumnarTopNOperator=2", fields[4]);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"empty", "openOnly", "importOnly", "firstN"})
  void updatingRankCannotPassWithoutItsOwnNewInput(String mode) {
    var scope =
        NativeExecution.beginRestore(
            "tech.streamfusion.canonical.UpdateRankRestoreTest",
            new Object(),
            "rank-test-update-fast-strategy");
    if (mode.equals("firstN")) {
      var operator = new NativeColumnarFirstNOperator();
      NativeExecution.opened(operator);
      NativeExecution.completed(operator, 4);
    } else if (!mode.equals("empty")) {
      NativeExecution.opened(new NativeColumnarTopNOperator());
      if (mode.equals("importOnly"))
        StreamFusionSuiteAgent.RecordCanonicalRankImport.exit(java.util.List.of(1), false);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void retractingRankCountsOnlyCommittedDataRowsAndRequiresNewInput() throws Exception {
    var scope = retractingRankScope();
    var contract = new RetractingContract();
    var rows =
        java.util.List.of(
            new TransferRow(-1),
            new TransferRow(-2),
            new TransferRow(-3),
            new TransferRow(-4),
            new TransferRow(0),
            new TransferRow(1),
            new TransferRow(2),
            new TransferRow(3));
    StreamFusionSuiteAgent.RecordCanonicalRetractingRankImport.exit(contract, rows, true);
    StreamFusionSuiteAgent.RecordCanonicalRetractingRankImport.exit(contract, rows, false);
    var operator = new NativeColumnarTopNOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 2);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
    try (var files = java.nio.file.Files.list(reports)) {
      var fields =
          java.nio.file.Files.readString(files.findFirst().orElseThrow())
              .stripTrailing()
              .split("\\t", -1);
      assertEquals("NativeColumnarTopNOperator=2", fields[2]);
      assertEquals("NativeColumnarTopNOperator=4", fields[4]);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "empty",
        "openOnly",
        "importOnly",
        "newInputOnly",
        "metadataOnly",
        "validationOnly",
        "firstN",
        "fallback"
      })
  void retractingRankRejectsMissingOrUnrelatedImportAndInput(String mode) throws Exception {
    var scope = retractingRankScope();
    var operator = new NativeColumnarTopNOperator();
    if (!mode.equals("empty")) NativeExecution.opened(operator);
    if (!java.util.Set.of("empty", "openOnly", "importOnly").contains(mode))
      NativeExecution.completed(operator, 2);
    if (!java.util.Set.of("empty", "openOnly", "newInputOnly").contains(mode)) {
      if (mode.equals("firstN")) {
        NativeExecution.opened(new NativeColumnarFirstNOperator());
        NativeExecution.restored("NativeColumnarFirstNOperator", 2);
      } else {
        var rows =
            mode.equals("metadataOnly")
                ? java.util.List.of(
                    new TransferRow(-1),
                    new TransferRow(-2),
                    new TransferRow(-3),
                    new TransferRow(-4))
                : java.util.List.of(new TransferRow(0));
        StreamFusionSuiteAgent.RecordCanonicalRetractingRankImport.exit(
            new RetractingContract(), rows, mode.equals("validationOnly"));
      }
    }
    if (mode.equals("fallback")) NativeExecution.fallback("unexpected fallback");
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private NativeExecution.Scope retractingRankScope() {
    return NativeExecution.beginRestore(
        "tech.streamfusion.canonical.RetractRankRestoreTest",
        new Object(),
        "rank-test-retract-strategy");
  }

  public static final class RetractingContract {
    private final org.apache.flink.table.types.logical.RowType rowType =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.IntType());
  }

  public static final class TransferRow {
    private final byte tag;

    public TransferRow(int tag) {
      this.tag = (byte) tag;
    }

    public byte getByte(int column) {
      assertEquals(4, column);
      return tag;
    }
  }

  private static final class NativeColumnarTopNOperator {}

  private static final class NativeColumnarFirstNOperator {}

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

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"group-aggregate-simple", "agg-with-state-ttl-hint"})
  void inheritedRestoreMethodUsesItsConcreteStateFamily(String program) {
    var scope = NativeExecution.beginRestore(GROUP, new Object(), program);
    assertNotNull(scope);
    var operator = new NativeColumnarGroupAggregateOperator();
    StreamFusionSuiteAgent.RecordCanonicalGroupImport.exit(77L, java.util.List.of(1, 2), false);
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    assertDoesNotThrow(() -> NativeExecution.finish(scope));
  }

  @Test
  void unrelatedNativeWorkCannotSatisfyAStateRestore() {
    var scope = NativeExecution.beginRestore(GROUP, new Object(), "group-aggregate-simple");
    var operator = new NativeCalcOperator();
    NativeExecution.opened(operator);
    NativeExecution.completed(operator, 3);
    var failure = assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
    assertTrue(failure.getMessage().contains("Execution contract failed"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "none",
        "openOnly",
        "inputOnly",
        "importOnly",
        "validating",
        "wrongHandle",
        "unopened",
        "otherFamily",
        "zeroHandle",
        "negativeHandle",
        "emptyRows"
      })
  void groupRestoreRequiresAppliedHeadersInTheOpenedOwnerAndNewInput(String mode) {
    var scope = NativeExecution.beginRestore(GROUP, new Object(), "group-aggregate-simple");
    var group = new NativeColumnarGroupAggregateOperator();
    if (!mode.equals("none") && !mode.equals("openOnly") && !mode.equals("inputOnly"))
      StreamFusionSuiteAgent.RecordCanonicalGroupImport.exit(
          mode.equals("wrongHandle")
              ? 78L
              : mode.equals("zeroHandle") ? 0L : mode.equals("negativeHandle") ? -1L : 77L,
          mode.equals("emptyRows") ? java.util.List.of() : java.util.List.of(1, 2),
          mode.equals("validating"));
    if (!mode.equals("none") && !mode.equals("unopened") && !mode.equals("otherFamily"))
      NativeExecution.opened(group);
    if (!mode.equals("none")
        && !mode.equals("openOnly")
        && !mode.equals("importOnly")
        && !mode.equals("unopened")) NativeExecution.completed(group, 3);
    if (mode.equals("otherFamily")) {
      var calc = new NativeCalcOperator();
      NativeExecution.opened(calc);
      NativeExecution.completed(calc, 3);
    }
    assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void unknownGroupProgramsCannotInheritTheSimpleRestoreWitness() {
    assertThrows(
        AssertionError.class,
        () -> NativeExecution.beginRestore(GROUP, new Object(), "group-aggregate-unknown"));
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

  private static final class NativeColumnarGroupAggregateOperator {
    final long handle = 77L;
  }

  private static final class NativeCalcOperator {}

  private static final class NativeColumnarChangelogNormalizeOperator {}

  private static final class NativeColumnarUpdatingJoinOperator {}

  private static final class NativeColumnarWindowRankOperator {}
}
