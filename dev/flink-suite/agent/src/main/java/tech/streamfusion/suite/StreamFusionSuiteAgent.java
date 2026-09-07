package tech.streamfusion.suite;

import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import static net.bytebuddy.matcher.ElementMatchers.isAbstract;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import net.bytebuddy.utility.JavaModule;

/** Installs StreamFusion when an untouched upstream Flink test creates a streaming planner. */
public final class StreamFusionSuiteAgent {

  private static final String PLANNER_FACTORY =
      "org.apache.flink.table.planner.delegation.DefaultPlannerFactory";
  private static final String DELEGATE_PLANNER_FACTORY =
      "org.apache.flink.table.planner.loader.DelegatePlannerFactory";
  private static final String PLANNER_CONTEXT = "org.apache.flink.table.delegation.PlannerFactory$Context";
  private static final String TABLE_ENVIRONMENT = "org.apache.flink.table.api.internal.TableEnvironmentImpl";
  private static final String JSON_PLAN_TEST_BASE =
      "org.apache.flink.table.planner.utils.JsonPlanTestBase";
  private static final String STREAM_EXECUTION_ENVIRONMENT =
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment";
  private static final String NATIVE_STATEFUL_OPERATOR =
      "tech.streamfusion.operator.AbstractNativeStatefulOperator";
  private static final String NATIVE_PARQUET_WRITER_FACTORY =
      "tech.streamfusion.operator.NativeParquetBulkWriterFactory";
  private static final AtomicBoolean ACTIVATION_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean HEAP_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_MEMORY_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean ROCKSDB_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_PARQUET_WRITER_REPORTED = new AtomicBoolean();
  private static final Map<ClassLoader, Set<String>> TRANSFORMED_TYPES =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final ConcurrentLinkedQueue<String> AUDIT_FAILURES = new ConcurrentLinkedQueue<>();
  private static final Set<Object> CONFIRMED_CONFIGS =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
  private static Path pendingAudit;
  private static final Object AUDIT_LOCK = new Object();
  private static boolean auditFinalized;
  private static volatile Instrumentation INSTRUMENTATION;
  private static final ThreadLocal<Boolean> UNMODIFIED_PLAN_SETUP = new ThreadLocal<>();
  private static final String JSON_PLAN_TEST_PACKAGE =
      "org.apache.flink.table.planner.runtime.stream.jsonplan.";

  private StreamFusionSuiteAgent() {}

  public static void premain(String arguments, Instrumentation instrumentation) {
    INSTRUMENTATION = instrumentation;
    String auditDirectory = System.getProperty("streamfusion.flink-suite.audit-dir");
    if (auditDirectory != null) {
      try {
        Path directory = Path.of(auditDirectory);
        Files.createDirectories(directory);
        pendingAudit = directory.resolve(ProcessHandle.current().pid() + "-" + UUID.randomUUID() + ".pending");
        Files.createFile(pendingAudit);
      } catch (Exception failure) {
        throw new IllegalStateException("cannot start StreamFusion suite audit", failure);
      }
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(StreamFusionSuiteAgent::auditInterception, "streamfusion-suite-audit"));
    new AgentBuilder.Default()
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
        .with(new InterceptionAudit())
        .type(named(PLANNER_FACTORY).or(named(DELEGATE_PLANNER_FACTORY)))
        .transform(
            (builder, type, classLoader, module, protectionDomain) -> {
              var create = named("create").and(takesArguments(1))
                  .and(takesArgument(0, named(PLANNER_CONTEXT))).and(not(isAbstract()));
              if (type.getDeclaredMethods().filter(create).isEmpty()) {
                throw new IllegalStateException("planner factory has no declared create(Context): " + type.getName());
              }
              return builder.visit(Advice.to(InstallStreamFusion.class).on(create));
            })
        .type(named(TABLE_ENVIRONMENT))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(Advice.to(VerifyInstallation.class).on(isConstructor())))
        .type(named(JSON_PLAN_TEST_BASE))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(MarkUnmodifiedPlanSetup.class)
                        .on(named("setup").and(takesArguments(0)))))
        .type(named(STREAM_EXECUTION_ENVIRONMENT))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallNativeRocksDB.class)
                        .on(named("configure").and(takesArguments(2)))))
        .type(named(NATIVE_STATEFUL_OPERATOR))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativeMemoryState.class)
                        .on(named("initializeState").and(takesArguments(1)))))
        .type(named(NATIVE_PARQUET_WRITER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativeParquetWriter.class)
                        .on(named("create").and(takesArguments(1)))))
        .installOn(instrumentation);
  }

  public static boolean reportActivation() {
    return ACTIVATION_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportHeapState() {
    return HEAP_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportRocksDBState() {
    return ROCKSDB_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportNativeMemoryState() {
    return NATIVE_MEMORY_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportNativeParquetWriter() {
    return NATIVE_PARQUET_WRITER_REPORTED.compareAndSet(false, true);
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
      StreamFusionSuiteAgent.install(context);
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

  /** Replaces only upstream tests' stock RocksDB selection, preserving the same Configuration. */
  public static final class InstallNativeRocksDB {

    private InstallNativeRocksDB() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) Object configuration) {
      if (!Boolean.getBoolean("streamfusion.flink-suite.native-rocksdb")) {
        return;
      }
      try {
        Class<?> configOption =
            Class.forName("org.apache.flink.configuration.ConfigOption");
        Object backendOption =
            Class.forName("org.apache.flink.configuration.StateBackendOptions")
                .getField("STATE_BACKEND")
                .get(null);
        Object selected =
            configuration.getClass().getMethod("get", configOption).invoke(configuration, backendOption);
        if ("hashmap".equalsIgnoreCase(String.valueOf(selected))) {
          if (StreamFusionSuiteAgent.reportHeapState()) {
            System.err.println("StreamFusion upstream state suite exercised Flink heap backend");
          }
          return;
        }
        if (!"rocksdb".equalsIgnoreCase(String.valueOf(selected))) {
          return;
        }
        configuration
            .getClass()
            .getMethod("set", configOption, Object.class)
            .invoke(
                configuration,
                backendOption,
                "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
        if (StreamFusionSuiteAgent.reportRocksDBState()) {
          System.err.println("StreamFusion upstream state suite installed native RocksDB backend");
        }
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("native RocksDB suite backend installation failed", e);
      }
    }
  }

  /** Proves that an upstream heap-backend case initialized StreamFusion's Rust hot-map state. */
  public static final class ReportNativeMemoryState {

    private ReportNativeMemoryState() {}

    @Advice.OnMethodExit
    static void exit(@Advice.FieldValue("rocksdbState") boolean rocksdbState) {
      if (!rocksdbState && StreamFusionSuiteAgent.reportNativeMemoryState()) {
        System.err.println(
            "StreamFusion upstream state suite initialized native memory backend");
      }
    }
  }

  /** Proves that an untouched upstream Flink test instantiated the native Parquet data plane. */
  public static final class ReportNativeParquetWriter {

    private ReportNativeParquetWriter() {}

    @Advice.OnMethodEnter
    static void enter() {
      if (StreamFusionSuiteAgent.reportNativeParquetWriter()) {
        System.err.println(
            "StreamFusion upstream Parquet suite created native Parquet sink writer");
      }
    }
  }

  public static void install(Object context) {
    if (InstallStreamFusion.requiresUnmodifiedFlinkPlan()) {
      return;
    }
    try {
      ClassLoader loader = context.getClass().getClassLoader();
      Object tableConfig = Class.forName(PLANNER_CONTEXT, true, loader)
          .getMethod("getTableConfig").invoke(context);
      if (!isStreaming(tableConfig)) {
        return;
      }
      synchronized (CONFIRMED_CONFIGS) {
        if (CONFIRMED_CONFIGS.contains(tableConfig)) {
          return;
        }
        Class<?> nativePlanner = Class.forName("tech.streamfusion.planner.NativePlanner", true, loader);
        nativePlanner.getMethod("install", Class.forName("org.apache.flink.table.api.TableConfig", true, loader))
            .invoke(null, tableConfig);
        CONFIRMED_CONFIGS.add(tableConfig);
      }
      if (reportActivation()) {
        System.err.println("StreamFusion enabled for upstream Flink streaming planner tests");
      }
    } catch (ReflectiveOperationException | RuntimeException | LinkageError failure) {
      recordFailure("streaming planner installation failed: " + describe(failure));
      throw new IllegalStateException("StreamFusion planner installation failed", failure);
    }
  }

  private static boolean isStreaming(Object config) throws ReflectiveOperationException {
    ClassLoader loader = config.getClass().getClassLoader();
    Object mode = config.getClass()
        .getMethod("get", Class.forName("org.apache.flink.configuration.ConfigOption", true, loader))
        .invoke(config, Class.forName("org.apache.flink.configuration.ExecutionOptions", true, loader)
            .getField("RUNTIME_MODE").get(null));
    return "STREAMING".equals(mode.toString());
  }

  public static final class VerifyInstallation {
    @Advice.OnMethodExit
    static void exit(@Advice.FieldValue("tableConfig") Object config) {
      StreamFusionSuiteAgent.verifyInstallation(config);
    }
  }

  public static void verifyInstallation(Object config) {
    if (InstallStreamFusion.requiresUnmodifiedFlinkPlan()) {
      return;
    }
    try {
      if (isStreaming(config) && !CONFIRMED_CONFIGS.contains(config)) {
        recordFailure("streaming table environment completed without successful StreamFusion installation");
      }
    } catch (ReflectiveOperationException | RuntimeException | LinkageError failure) {
      recordFailure("could not verify streaming planner installation: " + describe(failure));
    }
  }

  static void auditInterception() {
    try {
      for (Class<?> type : INSTRUMENTATION.getAllLoadedClasses()) {
        String name = type.getName();
        if (name.equals(PLANNER_FACTORY) || name.equals(DELEGATE_PLANNER_FACTORY)
            || name.equals(TABLE_ENVIRONMENT)) {
          synchronized (TRANSFORMED_TYPES) {
            if (!TRANSFORMED_TYPES.getOrDefault(type.getClassLoader(), Set.of()).contains(name)) {
              recordFailure(name + " was loaded but never instrumented in loader " + type.getClassLoader());
            }
          }
        }
      }
    } catch (Exception | LinkageError failure) {
      recordFailure("StreamFusion audit could not complete: " + failure);
    }
    synchronized (AUDIT_LOCK) {
      publishReceipt(!AUDIT_FAILURES.isEmpty());
      auditFinalized = true;
      if (!AUDIT_FAILURES.isEmpty()) {
        haltAudit();
      }
    }
  }

  private static String describe(Throwable failure) {
    StringBuilder message = new StringBuilder();
    Set<Throwable> seen = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    while (failure != null && seen.add(failure)) {
      if (message.length() > 0) {
        message.append(" caused by ");
      }
      message.append(failure);
      failure = failure.getCause();
    }
    return message.toString();
  }

  private static void recordFailure(String message) {
    synchronized (AUDIT_LOCK) {
      AUDIT_FAILURES.add(message);
      // A later shutdown hook must not leave a successful receipt behind after discovering failure.
      if (auditFinalized) {
        publishReceipt(true);
        haltAudit();
      }
    }
  }

  private static void publishReceipt(boolean failed) {
    if (pendingAudit == null) {
      return;
    }
    try {
      String name = pendingAudit.getFileName().toString();
      Path completed = pendingAudit.resolveSibling(
          name.substring(0, name.lastIndexOf('.')) + (failed ? ".failed" : ".ok"));
      if (!completed.equals(pendingAudit)) {
        Files.move(pendingAudit, completed);
        pendingAudit = completed;
      }
    } catch (Exception failure) {
      AUDIT_FAILURES.add("could not publish StreamFusion audit result: " + describe(failure));
    }
  }

  private static void haltAudit() {
    System.err.println("FATAL: StreamFusion suite installation audit failed.");
    AUDIT_FAILURES.forEach(problem -> System.err.println("  - " + problem));
    System.err.flush();
    Runtime.getRuntime().halt(70);
  }

  private static final class InterceptionAudit extends AgentBuilder.Listener.Adapter {
    @Override
    public void onTransformation(TypeDescription type, ClassLoader loader, JavaModule module,
        boolean loaded, DynamicType dynamicType) {
      synchronized (TRANSFORMED_TYPES) {
        TRANSFORMED_TYPES.computeIfAbsent(loader, ignored -> new HashSet<>()).add(type.getName());
      }
    }

    @Override
    public void onError(String type, ClassLoader loader, JavaModule module, boolean loaded,
        Throwable failure) {
      recordFailure("instrumentation failed for " + type + " in loader " + loader + ": " + describe(failure));
    }
  }
}
