package tech.streamfusion.suite;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/** Installs StreamFusion when an untouched upstream Flink test creates a streaming planner. */
public final class StreamFusionSuiteAgent {

  private static final String PLANNER_FACTORY =
      "org.apache.flink.table.planner.delegation.DefaultPlannerFactory";
  private static final String DELEGATE_PLANNER_FACTORY =
      "org.apache.flink.table.planner.loader.DelegatePlannerFactory";
  private static final String JSON_PLAN_TEST_BASE =
      "org.apache.flink.table.planner.utils.JsonPlanTestBase";
  private static final AtomicBoolean ACTIVATION_REPORTED = new AtomicBoolean();
  private static final Set<Object> INSTALLED_CONFIGS =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
  private static final ThreadLocal<Boolean> UNMODIFIED_PLAN_SETUP = new ThreadLocal<>();
  private static final String JSON_PLAN_TEST_PACKAGE =
      "org.apache.flink.table.planner.runtime.stream.jsonplan.";

  private StreamFusionSuiteAgent() {}

  public static void premain(String arguments, Instrumentation instrumentation) {
    new AgentBuilder.Default()
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
        .type(named(PLANNER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallStreamFusion.class)
                        .on(named("create").and(takesArguments(1)))))
        .type(named(DELEGATE_PLANNER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallStreamFusion.class)
                        .on(named("create").and(takesArguments(1)))))
        .type(named(JSON_PLAN_TEST_BASE))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(MarkUnmodifiedPlanSetup.class)
                        .on(named("setup").and(takesArguments(0)))))
        .installOn(instrumentation);
  }

  public static boolean reportActivation() {
    return ACTIVATION_REPORTED.compareAndSet(false, true);
  }

  public static boolean markConfigForInstallation(Object tableConfig) {
    return INSTALLED_CONFIGS.add(tableConfig);
  }

  public static void unmarkConfigForInstallation(Object tableConfig) {
    INSTALLED_CONFIGS.remove(tableConfig);
  }

  public static void enterUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.set(Boolean.TRUE);
  }

  public static void exitUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.remove();
  }

  public static final class InstallStreamFusion {

    private InstallStreamFusion() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) Object context) {
      try {
        if (requiresUnmodifiedFlinkPlan()) {
          return;
        }
        Object tableConfig =
            Class.forName("org.apache.flink.table.delegation.PlannerFactory$Context")
                .getMethod("getTableConfig")
                .invoke(context);
        Object runtimeMode =
            tableConfig
                .getClass()
                .getMethod("get", Class.forName("org.apache.flink.configuration.ConfigOption"))
                .invoke(
                    tableConfig,
                    Class.forName("org.apache.flink.configuration.ExecutionOptions")
                        .getField("RUNTIME_MODE")
                        .get(null));
        if (!"STREAMING".equals(runtimeMode.toString())) {
          return;
        }
        if (!StreamFusionSuiteAgent.markConfigForInstallation(tableConfig)) {
          return;
        }

        try {
          ClassLoader classLoader = context.getClass().getClassLoader();
          Class<?> nativePlanner =
              Class.forName("tech.streamfusion.planner.NativePlanner", true, classLoader);
          nativePlanner
              .getMethod("install", Class.forName("org.apache.flink.table.api.TableConfig"))
              .invoke(null, tableConfig);
        } catch (ClassNotFoundException
            | NoSuchMethodException
            | IllegalAccessException
            | InvocationTargetException e) {
          StreamFusionSuiteAgent.unmarkConfigForInstallation(tableConfig);
          throw e;
        }
        if (StreamFusionSuiteAgent.reportActivation()) {
          System.err.println("StreamFusion enabled for upstream Flink streaming planner tests");
        }
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | NoSuchFieldException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalStateException("StreamFusion planner installation failed", e);
      }
    }

    public static boolean requiresUnmodifiedFlinkPlan() {
      if (Boolean.TRUE.equals(UNMODIFIED_PLAN_SETUP.get())) {
        return true;
      }
      for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
        if (frame.getClassName().startsWith(JSON_PLAN_TEST_PACKAGE)) {
          return true;
        }
        if (frame.getClassName().equals("org.apache.flink.table.api.TableEnvironmentITCase")
            && frame.getMethodName().equals("testFromToDataStreamAndExecuteSql")) {
          return true;
        }
      }
      return false;
    }
  }

  public static final class MarkUnmodifiedPlanSetup {

    private MarkUnmodifiedPlanSetup() {}

    @Advice.OnMethodEnter
    public static void enter() {
      StreamFusionSuiteAgent.enterUnmodifiedPlanSetup();
    }

    @Advice.OnMethodExit
    public static void exit() {
      StreamFusionSuiteAgent.exitUnmodifiedPlanSetup();
    }
  }
}
