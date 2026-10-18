package tech.streamfusion.iceberg;

import java.lang.reflect.Field;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import tech.streamfusion.planner.NativePlanner;

/** Preserves upstream fixtures and assertions while selecting streaming/native execution. */
public final class IcebergUpstreamExtension
    implements ExecutionCondition, BeforeTestExecutionCallback {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (context
            .getTestClass()
            .map(c -> c.getSimpleName().equals("TestIcebergSourceSql"))
            .orElse(false)
        && context.getTestMethod().isPresent()
        && !java.util.Set.of("testWatermarkValidConfig", "testWatermarkInvalidConfig")
            .contains(context.getTestMethod().get().getName())) {
      return ConditionEvaluationResult.disabled("This published SQL case uses batch execution");
    }
    String name = context.getDisplayName();
    if (name.contains("isStreamingJob=false") || name.contains("isStreaming=false")) {
      return ConditionEvaluationResult.disabled("Iceberg acceleration scope is streaming only");
    }
    return ConditionEvaluationResult.enabled("Unchanged upstream streaming case");
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) throws Exception {
    if (!java.util.Set.of("TestFlinkTableSinkExtended", "TestIcebergSourceSql")
            .contains(context.getRequiredTestClass().getSimpleName())
        || !Boolean.getBoolean("streamfusion.iceberg.suite.native")) return;
    boolean installed = false;
    Object instance = context.getRequiredTestInstance();
    for (Class<?> type = instance.getClass(); type != null; type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (!field.getName().equals("tEnv") && !field.getName().equals("streamingTEnv")) continue;
        field.setAccessible(true);
        Object environment = field.get(instance);
        if (environment instanceof StreamTableEnvironment
            || (field.getName().equals("streamingTEnv")
                && environment instanceof org.apache.flink.table.api.TableEnvironment)) {
          NativePlanner.install((org.apache.flink.table.api.TableEnvironment) environment);
          installed = true;
        }
      }
    }
    if (installed) return;
    throw new IllegalStateException(
        "Selected upstream streaming fixture has no initialized table environment");
  }
}
