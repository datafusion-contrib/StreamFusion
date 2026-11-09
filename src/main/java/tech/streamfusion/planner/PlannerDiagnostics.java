package tech.streamfusion.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

/** Emits completed planning diagnostics without requiring a separate configuration switch. */
final class PlannerDiagnostics {
  private PlannerDiagnostics() {}

  static void logOccurrences(Logger logger, String prefix, List<String> messages) {
    Map<String, Integer> occurrences = new LinkedHashMap<>();
    for (String message : messages) occurrences.merge(message, 1, Integer::sum);
    occurrences.forEach(
        (message, count) -> {
          if (count == 1) logger.info("{}{}", prefix, message);
          else logger.info("{}{} ({} occurrences)", prefix, message, count);
        });
  }

  static String functionExecution(String operator, RexExpression expression) {
    List<String> nativeCalls = expression.nativeFunctionCalls();
    List<String> jvmCalls = expression.jvmFunctionCalls();
    String execution = operator + " functions: native=" + nativeCalls + ", jvm=" + jvmCalls;
    if (!jvmCalls.isEmpty()) execution += ", reason=" + expression.jvmExecutionReason();
    return execution;
  }
}
