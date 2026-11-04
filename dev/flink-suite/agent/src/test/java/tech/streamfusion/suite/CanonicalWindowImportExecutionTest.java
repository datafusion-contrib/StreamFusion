package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@EnabledIfSystemProperty(named = "streamfusion.flink-suite.canonical", matches = "true")
class CanonicalWindowImportExecutionTest {
  @TempDir Path reports;

  @BeforeEach
  void reports() {
    System.setProperty("streamfusion.flink-suite.native-reports", reports.toString());
  }

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            session ->
                Stream.of(
                        "complete", "noImport", "noInput", "wrongHandle", "zeroRows", "neverOpened")
                    .map(mode -> Arguments.of(session, mode)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void successfulImportMustReachTheOpenedOwnerAndReceiveNewInput(boolean session, String mode)
      throws Exception {
    var scope =
        begin(
            session
                ? "window-aggregate-session-partition-event-time"
                : "window-aggregate-tumble-event-time");
    Object owner =
        session
            ? new NativeColumnarSessionWindowAggregateOperator(41)
            : new NativeColumnarWindowAggregateOperator(41);
    if (!mode.equals("noImport"))
      StreamFusionSuiteAgent.RecordCanonicalWindowImport.exit(
          mode.equals("zeroRows") ? List.of() : List.of(1, 2),
          mode.equals("wrongHandle") ? 42 : 41);
    if (!mode.equals("neverOpened")) NativeExecution.opened(owner);
    if (!mode.equals("noInput")) NativeExecution.completed(owner, 4);
    if (mode.equals("complete")) {
      assertDoesNotThrow(() -> NativeExecution.finish(scope));
      try (var files = Files.list(reports)) {
        String[] record =
            Files.readString(files.findFirst().orElseThrow()).stripTrailing().split("\t", -1);
        assertEquals(owner.getClass().getSimpleName() + "=2", record[4]);
      }
    } else assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  @Test
  void unopenedImportsDoNotLeakIntoTheNextInvocation() {
    var first = begin("window-aggregate-tumble-event-time");
    StreamFusionSuiteAgent.RecordCanonicalWindowImport.exit(List.of(1, 2), 41);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(first));
    var next = begin("window-aggregate-tumble-event-time");
    var owner = new NativeColumnarWindowAggregateOperator(41);
    NativeExecution.opened(owner);
    NativeExecution.completed(owner, 4);
    assertThrows(AssertionError.class, () -> NativeExecution.finish(next));
  }

  static Stream<Arguments> roles() {
    return Stream.of(Arguments.of(false), Arguments.of(true));
  }

  @ParameterizedTest
  @MethodSource("roles")
  void globalImportRecordsTheActualSliceOrFinalOwner(boolean finalWindow) throws Exception {
    var scope = begin("window-aggregate-tumble-event-time-two-phase-distinct-split");
    var local = new NativeColumnarLocalWindowAggregateOperator();
    NativeExecution.opened(local);
    NativeExecution.completed(local, 4);
    for (boolean last : new boolean[] {false, true}) {
      var owner = new NativeColumnarGlobalWindowAggregateOperator(last ? 42 : 41, last);
      if (last == finalWindow)
        StreamFusionSuiteAgent.RecordCanonicalWindowImport.exit(List.of(1, 2), last ? 42 : 41);
      NativeExecution.opened(owner);
      NativeExecution.completed(owner, 4);
    }
    NativeExecution.finish(scope);
    try (var files = Files.list(reports)) {
      String[] record =
          Files.readString(files.findFirst().orElseThrow()).stripTrailing().split("\t", -1);
      String role =
          "NativeColumnarGlobalWindowAggregateOperator" + (finalWindow ? "_Final" : "_Slice");
      assertTrue(record[4].contains(role + "=2"));
      assertFalse(record[4].contains("NativeColumnarLocalWindowAggregateOperator"));
      assertFalse(record[4].contains((finalWindow ? "_Slice" : "_Final") + "="));
    }
  }

  private static NativeExecution.Scope begin(String program) {
    return NativeExecution.beginRestore(
        "org.apache.flink.table.planner.plan.nodes.exec.stream.WindowAggregateEventTimeRestoreTest",
        new Object(),
        program);
  }

  private static final class NativeColumnarWindowAggregateOperator {
    private final long handle;

    NativeColumnarWindowAggregateOperator(long handle) {
      this.handle = handle;
    }
  }

  private static final class NativeColumnarSessionWindowAggregateOperator {
    private final long handle;

    NativeColumnarSessionWindowAggregateOperator(long handle) {
      this.handle = handle;
    }
  }

  private static final class NativeColumnarLocalWindowAggregateOperator {}

  public static final class NativeColumnarGlobalWindowAggregateOperator {
    private final long handle;
    private final boolean finalWindow;

    NativeColumnarGlobalWindowAggregateOperator(long handle, boolean finalWindow) {
      this.handle = handle;
      this.finalWindow = finalWindow;
    }

    public boolean isFinalWindow() {
      return finalWindow;
    }
  }
}
