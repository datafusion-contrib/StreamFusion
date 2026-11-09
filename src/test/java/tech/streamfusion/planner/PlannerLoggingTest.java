package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

class PlannerLoggingTest {
  @RegisterExtension
  final LoggerAuditingExtension logs =
      new LoggerAuditingExtension(PhysicalPlanScan.class, Level.INFO);

  @Test
  void defaultInfoIncludesTheDisabledOperatorAndItsReason() {
    var table = environment();
    var scan = NativePlanner.install(table);
    table.getConfig().set("streamfusion.operator.calc.enabled", "false");
    table.getConfig().set("streamfusion.logFallbackReasons", "false");

    table.explainSql("SELECT CHAR_LENGTH(s) FROM input_table");

    assertFalse(scan.fallbackReasons().isEmpty());
    for (String reason : scan.fallbackReasons()) {
      assertTrue(
          logs.getMessages().stream().anyMatch(message -> message.contains(reason)),
          () -> "Missing INFO reason: " + reason + " in " + logs.getMessages());
    }
    assertTrue(
        logs.getMessages().stream()
            .anyMatch(message -> message.contains("streamfusion.operator.calc.enabled")),
        logs.getMessages().toString());

    int previousLogs = logs.getMessages().size();
    table.getConfig().set("streamfusion.operator.calc.enabled", "true");
    table.explainSql("SELECT id FROM input_table");
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
    assertTrue(
        logs.getMessages().subList(previousLogs, logs.getMessages().size()).stream()
            .noneMatch(message -> message.contains("disabled by config")),
        logs.getMessages().toString());
  }

  @Test
  void defaultInfoIncludesTheFinalNativeAndJvmFunctionAssignments() {
    var table = environment();
    var scan = NativePlanner.install(table);
    table.createTemporarySystemFunction("JVM_ONLY", JvmOnlyFunction.class);

    String plan = table.explainSql("SELECT JVM_ONLY(id), CHAR_LENGTH(s) FROM input_table");

    assertTrue(plan.contains("NativeCalc"), plan);
    assertFalse(scan.functionExecutions().isEmpty());
    for (String execution : scan.functionExecutions()) {
      assertTrue(logs.getMessages().contains(execution), logs.getMessages().toString());
    }
    assertTrue(
        logs.getMessages().stream()
            .anyMatch(
                message ->
                    message.contains("native=[CHAR_LENGTH], jvm=[JVM_ONLY]")
                        && message.contains("reason=")),
        logs.getMessages().toString());
  }

  @Test
  void discardedNativeCandidatesDoNotAppearAsFinalFunctionAssignments() {
    var table = environment();
    var scan = NativePlanner.install(table);
    table.getConfig().set("streamfusion.operator.groupAggregate.enabled", "false");
    table.createTemporarySystemFunction("JVM_ONLY", JvmOnlyFunction.class);

    String plan = table.explainSql("SELECT SUM(JVM_ONLY(id)) FROM input_table");

    assertFalse(plan.contains("NativeCalc"), plan);
    assertTrue(scan.functionExecutions().isEmpty(), scan.explainSummary());
    assertTrue(
        logs.getMessages().stream().noneMatch(message -> message.contains(" functions: ")),
        logs.getMessages().toString());
    assertTrue(
        logs.getMessages().stream()
            .anyMatch(message -> message.contains("streamfusion.operator.groupAggregate.enabled")),
        logs.getMessages().toString());
  }

  private static TableEnvironment environment() {
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE input_table(id INT,s STRING) WITH ('connector'='datagen')");
    return table;
  }

  public static class JvmOnlyFunction extends ScalarFunction {
    public Integer eval(Integer value) {
      return value;
    }
  }
}
