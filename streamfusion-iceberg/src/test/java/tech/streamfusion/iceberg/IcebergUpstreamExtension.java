package tech.streamfusion.iceberg;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.planner.NativePlanner;

/** Selects StreamFusion's planner without changing the published fixtures or assertions. */
public final class IcebergUpstreamExtension
    implements InvocationInterceptor, BeforeTestExecutionCallback {
  @Override
  public void interceptBeforeEachMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> method,
      ExtensionContext context)
      throws Throwable {
    if (Boolean.getBoolean("streamfusion.iceberg.suite.native")
        && method.getExecutable().getName().equals("before")) {
      installBatchEnvironment(context);
    }
    invocation.proceed();
  }

  private static void installBatchEnvironment(ExtensionContext context) throws Exception {
    var instance = context.getRequiredTestInstance();
    var type = context.getRequiredTestClass();
    Field environment;
    switch (type.getSimpleName()) {
      case "TestIcebergSourceSql":
        environment = type.getSuperclass().getDeclaredField("tEnv");
        break;
      case "TestFlinkTableSinkExtended":
        var mode = type.getDeclaredField("isStreamingJob");
        mode.setAccessible(true);
        if (mode.getBoolean(instance)) return;
        environment = type.getDeclaredField("tEnv");
        break;
      default:
        return;
    }
    environment.setAccessible(true);
    // The MiniCluster and parameter injection are ready before the fixture's own setup runs.
    environment.set(instance, NativePlannerTestEnvironment.createBatch(new Configuration()));
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
                && environment instanceof TableEnvironment)) {
          NativePlanner.install((TableEnvironment) environment);
          installed = true;
        } else if (environment instanceof org.apache.flink.table.api.internal.TableEnvironmentImpl
            && ((org.apache.flink.table.api.internal.TableEnvironmentImpl) environment).getPlanner()
                instanceof tech.streamfusion.planner.batch.NativeBatchPlanner) {
          installed = true;
        }
      }
    }
    if (installed) return;
    throw new IllegalStateException(
        "Selected upstream Iceberg fixture has no initialized StreamFusion table environment");
  }
}
