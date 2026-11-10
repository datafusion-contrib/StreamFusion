package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

class PlannerDiagnosticsTest {
  @RegisterExtension
  final LoggerAuditingExtension logs =
      new LoggerAuditingExtension(PlannerDiagnostics.class, Level.INFO);

  @Test
  void defaultInfoCoalescesReasonsWithoutChangingTheirRecordedOrder() {
    var reasons =
        List.of("Calc: unsupported expression", "Join: disabled", "Calc: unsupported expression");

    PlannerDiagnostics.logOccurrences(
        LoggerFactory.getLogger(PlannerDiagnostics.class), "fallback: ", reasons);

    assertEquals(
        List.of(
            "fallback: Calc: unsupported expression (2 occurrences)", "fallback: Join: disabled"),
        logs.getMessages());
    assertEquals(
        List.of("Calc: unsupported expression", "Join: disabled", "Calc: unsupported expression"),
        reasons);
  }

  @Test
  void separatePlanningPassesHaveIndependentCounts() {
    var logger = LoggerFactory.getLogger(PlannerDiagnostics.class);
    PlannerDiagnostics.logOccurrences(logger, "fallback: ", List.of("Calc", "Calc"));
    PlannerDiagnostics.logOccurrences(logger, "fallback: ", List.of("Calc"));

    assertEquals(List.of("fallback: Calc (2 occurrences)", "fallback: Calc"), logs.getMessages());
  }

  @Test
  void emptyDiagnosticsDoNotEmitAnExtraLog() {
    PlannerDiagnostics.logOccurrences(
        LoggerFactory.getLogger(PlannerDiagnostics.class), "fallback: ", List.of());
    assertTrue(logs.getMessages().isEmpty());
  }
}
