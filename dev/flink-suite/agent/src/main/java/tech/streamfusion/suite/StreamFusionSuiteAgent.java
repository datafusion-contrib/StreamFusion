package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.isAnnotatedWith;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

/**
 * Installs StreamFusion when an untouched upstream Flink test creates a streaming planner, and
 * loads the native library at that moment: a TaskManager loads it once at startup, whereas a fresh
 * test fork would otherwise pay the load inside the first native task and delay that job's first
 * checkpoint, which timing-sensitive upstream tests would misread as a behavioural difference.
 */
public final class StreamFusionSuiteAgent {

  private static final String PLANNER_FACTORY =
      "org.apache.flink.table.planner.delegation.DefaultPlannerFactory";
  private static final String DELEGATE_PLANNER_FACTORY =
      "org.apache.flink.table.planner.loader.DelegatePlannerFactory";
  private static final String JSON_PLAN_TEST_BASE =
      "org.apache.flink.table.planner.utils.JsonPlanTestBase";
  private static final String STREAM_EXECUTION_ENVIRONMENT =
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment";
  private static final String NATIVE_STATEFUL_OPERATOR =
      "tech.streamfusion.operator.AbstractNativeStatefulOperator";
  private static final String NATIVE_PARQUET_WRITER_FACTORY =
      "tech.streamfusion.operator.NativeFileBulkWriterFactory";
  private static final String PAIMON_FORMAT_FACTORY_UTIL =
      "org.apache.paimon.factories.FormatFactoryUtil";
  private static final String NATIVE_PAIMON_FORMAT_FACTORY =
      "tech.streamfusion.paimon.NativePaimonParquetFormatFactory";
  private static final String NATIVE_PAIMON_PARQUET_WRITER =
      "tech.streamfusion.paimon.NativePaimonFileWriter";
  private static final String NATIVE_PAIMON_KEY_VALUE_FILE_WRITER =
      "tech.streamfusion.paimon.NativePaimonKeyValueFileWriter";
  private static final String NATIVE_PAIMON_SNAPSHOT_READER =
      "tech.streamfusion.paimon.NativePaimonSnapshotReader";
  private static final AtomicBoolean NATIVE_PAIMON_SNAPSHOT_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean ACTIVATION_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean HEAP_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_MEMORY_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean ROCKSDB_STATE_REPORTED = new AtomicBoolean();
  private static final java.util.Set<String> NATIVE_FILE_WRITERS_REPORTED =
      java.util.concurrent.ConcurrentHashMap.newKeySet();
  private static final AtomicBoolean NATIVE_PAIMON_FORMAT_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_PAIMON_BUNDLE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_PAIMON_LEVEL_ZERO_FILE_REPORTED = new AtomicBoolean();
  private static final Set<Object> INSTALLED_CONFIGS =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
  private static final ThreadLocal<Boolean> UNMODIFIED_PLAN_SETUP = new ThreadLocal<>();
  private static final String JSON_PLAN_TEST_PACKAGE =
      "org.apache.flink.table.planner.runtime.stream.jsonplan.";

  private StreamFusionSuiteAgent() {}

  public static void premain(String arguments, Instrumentation instrumentation) {
    PaimonTestWatch.initialize(System.err);
    new AgentBuilder.Default()
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
        .type(named("org.apache.paimon.flink.FlinkTestBase"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(BuildLegacyPaimonCatalog.class)
                        .on(
                            named("createResolvedTable")
                                .and(takesArguments(4))
                                .and(takesArgument(1, java.util.List.class)))))
        .type(
            namedOneOf(
                "org.apache.flink.runtime.minicluster.MiniCluster$TerminatingFatalErrorHandler",
                "org.apache.flink.runtime.minicluster.MiniCluster$ShutDownFatalErrorHandler"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(Advice.to(ReportClusterFailure.class).on(named("onFatalError"))))
        .type(
            nameStartsWith("org.apache.paimon.flink.")
                .and(nameEndsWith("ITCase").or(nameEndsWith("ITCaseBase"))))
        .transform(
            (builder, type, classLoader, module, protectionDomain) -> {
              if (System.getProperty("streamfusion.flink-suite.flink-line", "2.2").equals("1.18")
                  && PaimonCheckpointFixture.supports(type.getName())) {
                builder = builder.visit(PaimonCheckpointFixture.adapter());
              }
              return builder.visit(
                  Advice.to(WatchPaimonTest.class)
                      .on(
                          isAnnotatedWith(named("org.junit.jupiter.api.Test"))
                              .or(
                                  isAnnotatedWith(
                                      named("org.junit.jupiter.params.ParameterizedTest")))));
            })
        .type(named("org.apache.flink.table.planner.delegation.DefaultExecutor"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ApplyLegacyPaimonRestore.class)
                        .on(named("createPipeline").and(takesArguments(4)))))
        .type(named("tech.streamfusion.planner.PhysicalPlanScan"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(Advice.to(RecordFallback.class).on(named("recordFallback"))))
        .type(namedOneOf(NativeExecution.testClasses()))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(RequireNativeExecution.class)
                        .on(namedOneOf(NativeExecution.testMethods(type.getName())))))
        .type(
            namedOneOf(
                "tech.streamfusion.operator.NativeCalcOperator",
                "tech.streamfusion.operator.NativeLookupJoinOperator",
                "tech.streamfusion.operator.NativeAsyncLookupJoinOperator",
                "tech.streamfusion.operator.NativeFilterOperator",
                "tech.streamfusion.operator.NativeColumnarGroupAggregateOperator",
                "tech.streamfusion.operator.NativeColumnarUpdatingJoinOperator",
                "tech.streamfusion.operator.NativeColumnarTopNOperator",
                "tech.streamfusion.operator.NativeWindowOperatorCore",
                "tech.streamfusion.operator.NativeColumnarGlobalWindowAggregateOperator"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) -> {
              builder = builder.visit(Advice.to(BindNativeExecution.class).on(named("open")));
              if (type.getName().endsWith("NativeColumnarGroupAggregateOperator")) {
                return builder.visit(Advice.to(RecordNativeBatch.class).on(named("update")));
              }
              if (type.getName().endsWith("NativeColumnarTopNOperator")) {
                return builder.visit(Advice.to(RecordNativeBatch.class).on(named("push")));
              }
              if (type.getName().endsWith("NativeColumnarUpdatingJoinOperator")) {
                return builder.visit(Advice.to(RecordNativeBatch.class).on(named("joinOpen")));
              }
              if (type.getName().endsWith("NativeWindowOperatorCore")) {
                return builder.visit(
                    Advice.to(RecordNativeBatch.class)
                        .on(namedOneOf("updateColumnarInternal", "updateColumnarAttached")));
              }
              return builder.visit(
                  Advice.to(RecordNativeElement.class)
                      .on(
                          named("processElement")
                              .and(
                                  takesArgument(
                                      0,
                                      named(
                                          "org.apache.flink.streaming.runtime.streamrecord.StreamRecord")))));
            })
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
        .type(named(STREAM_EXECUTION_ENVIRONMENT))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder
                    .visit(
                        Advice.to(InstallNativeRocksDB.class)
                            .on(named("configure").and(takesArguments(2))))
                    .visit(
                        Advice.to(InstallLegacyNativeRocksDB.class)
                            .on(named("setStateBackend").and(takesArguments(1)))))
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
        .type(named(PAIMON_FORMAT_FACTORY_UTIL))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(PreferNativePaimonFormat.class)
                        .on(named("discoverFactory").and(takesArguments(2)))))
        .type(named("tech.streamfusion.delta.NativeDeltaParquetHandler$NativeFile"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder
                    .visit(Advice.to(BindNativeExecution.class).on(isConstructor()))
                    .visit(
                        Advice.to(RecordNativeDeltaWrite.class)
                            .on(named("write").and(takesArguments(3)))))
        .type(named(NATIVE_PAIMON_PARQUET_WRITER))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativePaimonBundle.class)
                        .on(named("writeNative").and(takesArguments(1)))))
        .type(named(NATIVE_PAIMON_KEY_VALUE_FILE_WRITER))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativePaimonLevelZeroFile.class)
                        .on(
                            named("write")
                                .and(takesArgument(0, named("org.apache.paimon.data.BinaryRow"))))))
        .type(named(NATIVE_PAIMON_SNAPSHOT_READER))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativePaimonSnapshot.class)
                        .on(named("next").and(takesArguments(0)))))
        .installOn(instrumentation);
  }

  public static final class RequireNativeExecution {
    @Advice.OnMethodEnter
    static NativeExecution.Scope enter(
        @Advice.This Object fixture,
        @Advice.Origin("#t") String type,
        @Advice.Origin("#m") String method) {
      return NativeExecution.begin(type + "#" + method, fixture);
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    static void exit(@Advice.Enter NativeExecution.Scope scope, @Advice.Thrown Throwable failure) {
      try {
        NativeExecution.finish(scope);
      } catch (AssertionError proofFailure) {
        if (failure == null) {
          throw proofFailure;
        }
        failure.addSuppressed(proofFailure);
      }
    }
  }

  public static final class WatchPaimonTest {
    @Advice.OnMethodEnter
    static PaimonTestWatch enter(@Advice.This Object fixture, @Advice.Origin("#t.#m") String test) {
      return PaimonTestWatch.start(test, fixture);
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    static void exit(@Advice.Enter PaimonTestWatch watch, @Advice.Thrown Throwable failure) {
      watch.finish(failure);
    }
  }

  public static final class ReportClusterFailure {
    @Advice.OnMethodEnter
    static void enter(@Advice.Origin("#t") String handler, @Advice.Argument(0) Throwable failure) {
      PaimonTestWatch.clusterFailure(handler, failure);
    }
  }

  public static final class RecordFallback {
    @Advice.OnMethodExit
    static void exit(@Advice.Argument(0) String reason) {
      NativeExecution.fallback(reason);
    }
  }

  public static final class BindNativeExecution {
    @Advice.OnMethodExit
    static void exit(@Advice.This Object operator) {
      NativeExecution.opened(operator);
    }
  }

  public static final class RecordNativeDeltaWrite {
    @Advice.OnMethodExit
    static void exit(@Advice.This Object writer, @Advice.Argument(2) int rows) {
      NativeExecution.completed(writer, rows);
    }
  }

  public static final class RecordNativeBatch {
    @Advice.OnMethodEnter
    static int enter(@Advice.This Object operator, @Advice.Argument(0) Object root)
        throws ReflectiveOperationException {
      return NativeExecution.batchRows(operator, root);
    }

    @Advice.OnMethodExit
    static void exit(@Advice.This Object operator, @Advice.Enter int rows) {
      NativeExecution.completed(operator, rows);
    }
  }

  public static final class RecordNativeElement {
    @Advice.OnMethodEnter
    static int enter(@Advice.This Object operator, @Advice.Argument(0) Object record)
        throws ReflectiveOperationException {
      return NativeExecution.elementRows(operator, record);
    }

    @Advice.OnMethodExit
    static void exit(@Advice.This Object operator, @Advice.Enter int rows) {
      NativeExecution.completed(operator, rows);
    }
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

  public static boolean reportNativeFileWriter(String format) {
    return NATIVE_FILE_WRITERS_REPORTED.add(format);
  }

  public static boolean reportNativePaimonBundle() {
    return NATIVE_PAIMON_BUNDLE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportNativePaimonLevelZeroFile() {
    return NATIVE_PAIMON_LEVEL_ZERO_FILE_REPORTED.compareAndSet(false, true);
  }

  /**
   * The test-JVM stand-in for deploying {@code 01-streamfusion-paimon.jar} ahead of Paimon:
   * Surefire appends StreamFusion's classpath in no fixed order, so Paimon's first-factory-wins
   * discovery is resolved here to the native {@code parquet} factory whenever the module is
   * present.
   */
  public static Object nativePaimonFormatFactory(ClassLoader classLoader, String identifier) {
    if (!"parquet".equals(identifier) && !"orc".equals(identifier)) {
      return null;
    }
    try {
      Object factory =
          Class.forName(
                  identifier.equals("orc")
                      ? "tech.streamfusion.paimon.NativePaimonOrcFormatFactory"
                      : NATIVE_PAIMON_FORMAT_FACTORY,
                  true,
                  classLoader)
              .getConstructor()
              .newInstance();
      if (NATIVE_PAIMON_FORMAT_REPORTED.compareAndSet(false, true)) {
        System.err.println(
            "StreamFusion upstream Paimon suite resolved "
                + identifier
                + " to the native format factory");
      }
      return factory;
    } catch (ClassNotFoundException e) {
      return null;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("native Paimon format factory instantiation failed", e);
    }
  }

  public static boolean markConfigForInstallation(Object tableConfig) {
    return INSTALLED_CONFIGS.add(tableConfig);
  }

  public static void unmarkConfigForInstallation(Object tableConfig) {
    INSTALLED_CONFIGS.remove(tableConfig);
  }

  public static boolean installed(Object tableConfig) {
    return INSTALLED_CONFIGS.contains(tableConfig);
  }

  public static void enterUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.set(Boolean.TRUE);
  }

  public static void exitUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.remove();
  }

  public static final class InstallStreamFusion {

    private InstallStreamFusion() {}

    @Advice.OnMethodExit
    static void exit(
        @Advice.Argument(0) Object context,
        @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Object planner) {
      if (!planner
          .getClass()
          .getName()
          .equals("org.apache.flink.table.planner.delegation.StreamPlanner")) {
        return;
      }
      try {
        Class<?> contextClass =
            Class.forName("org.apache.flink.table.delegation.PlannerFactory$Context");
        Object tableConfig = contextClass.getMethod("getTableConfig").invoke(context);
        if (StreamFusionSuiteAgent.installed(tableConfig)) {
          planner =
              Class.forName(
                      "tech.streamfusion.planner.StreamFusionPlannerFactory",
                      true,
                      context.getClass().getClassLoader())
                  .getMethod("createStreamingPlanner", contextClass)
                  .invoke(null, context);
        }
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("StreamFusion complete-plan installation failed", e);
      }
    }

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
          Class.forName("tech.streamfusion.Native", true, classLoader);
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

  /** Replaces only upstream tests' stock RocksDB selection, preserving the same Configuration. */
  public static final class InstallNativeRocksDB {

    private InstallNativeRocksDB() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) Object configuration) {
      if (!Boolean.getBoolean("streamfusion.flink-suite.native-rocksdb")) {
        return;
      }
      try {
        Class<?> configOption = Class.forName("org.apache.flink.configuration.ConfigOption");
        if (System.getProperty("streamfusion.flink-suite.flink-line", "2.2").equals("1.18")) {
          Object changelogOption =
              Class.forName("org.apache.flink.configuration.StateChangelogOptions")
                  .getField("ENABLE_STATE_CHANGE_LOG")
                  .get(null);
          if (Boolean.TRUE.equals(
              configuration
                  .getClass()
                  .getMethod("get", configOption)
                  .invoke(configuration, changelogOption))) return;
        }
        Object backendOption =
            Class.forName("org.apache.flink.configuration.StateBackendOptions")
                .getField("STATE_BACKEND")
                .get(null);
        Object selected =
            configuration
                .getClass()
                .getMethod("get", configOption)
                .invoke(configuration, backendOption);
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

  /** Flink 1.18 upstream fixtures still use the legacy programmatic backend API. */
  public static final class InstallLegacyNativeRocksDB {
    @Advice.OnMethodEnter
    static void enter(
        @Advice.This Object environment,
        @Advice.Argument(
                value = 0,
                readOnly = false,
                typing = net.bytebuddy.implementation.bytecode.assign.Assigner.Typing.DYNAMIC)
            Object backend) {
      backend = LegacyStateBackend.replace(environment, backend);
    }
  }

  /** Proves that an upstream heap-backend case initialized StreamFusion's Rust hot-map state. */
  public static final class ReportNativeMemoryState {

    private ReportNativeMemoryState() {}

    @Advice.OnMethodExit
    static void exit(@Advice.FieldValue("rocksdbState") boolean rocksdbState) {
      if (!rocksdbState && StreamFusionSuiteAgent.reportNativeMemoryState()) {
        System.err.println("StreamFusion upstream state suite initialized native memory backend");
      }
    }
  }

  /** Proves that an untouched upstream Flink test instantiated the native Parquet data plane. */
  public static final class ReportNativeParquetWriter {

    private ReportNativeParquetWriter() {}

    @Advice.OnMethodExit
    static void exit(@Advice.FieldValue("codec") Object codec) {
      String format = codec.getClass().getSimpleName().replace("Codec", "");
      if (StreamFusionSuiteAgent.reportNativeFileWriter(format)) {
        System.err.println(
            "StreamFusion upstream " + format + " suite created native " + format + " sink writer");
      }
    }
  }

  /**
   * Gives the native Paimon Parquet factory the classpath precedence a deployment gives its jar.
   */
  public static final class PreferNativePaimonFormat {

    private PreferNativePaimonFormat() {}

    @Advice.OnMethodExit
    static void exit(
        @Advice.Argument(0) ClassLoader classLoader,
        @Advice.Argument(1) String identifier,
        @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Object factory) {
      Object preferred = StreamFusionSuiteAgent.nativePaimonFormatFactory(classLoader, identifier);
      if (preferred != null) {
        factory = preferred;
      }
    }
  }

  /** Proves that an untouched upstream Paimon test wrote a data file from a native Arrow bundle. */
  public static final class ReportNativePaimonBundle {

    private ReportNativePaimonBundle() {}

    @Advice.OnMethodExit
    static void exit() {
      if (StreamFusionSuiteAgent.reportNativePaimonBundle()) {
        System.err.println("StreamFusion upstream Paimon suite wrote a native Paimon bundle");
      }
    }
  }

  public static final class ReportNativePaimonLevelZeroFile {

    private ReportNativePaimonLevelZeroFile() {}

    @Advice.OnMethodExit
    static void exit() {
      if (StreamFusionSuiteAgent.reportNativePaimonLevelZeroFile()) {
        System.err.println("StreamFusion upstream Paimon suite wrote a native Paimon level-0 file");
      }
    }
  }

  public static boolean reportNativePaimonSnapshot() {
    return NATIVE_PAIMON_SNAPSHOT_REPORTED.compareAndSet(false, true);
  }

  public static final class ReportNativePaimonSnapshot {
    @Advice.OnMethodExit
    static void exit(@Advice.Return(typing = Assigner.Typing.DYNAMIC) Object batch) {
      if (batch != null && StreamFusionSuiteAgent.reportNativePaimonSnapshot()) {
        System.err.println("StreamFusion upstream Paimon suite merged a native snapshot batch");
      }
    }
  }

  public static final class ApplyLegacyPaimonRestore {
    @Advice.OnMethodExit
    static void exit(@Advice.Argument(1) Object configuration, @Advice.Return Object pipeline) {
      if (System.getProperty("streamfusion.flink-suite.flink-line", "2.2").equals("1.18")
          && PaimonCheckpointFixture.isRecoveryTest()) {
        PaimonCheckpointFixture.applyRestoreSettings(configuration, pipeline);
      }
    }
  }

  public static final class BuildLegacyPaimonCatalog {
    @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
    static Object enter(@Advice.Origin Class<?> fixture, @Advice.AllArguments Object[] arguments) {
      if (!System.getProperty("streamfusion.flink-suite.flink-line", "2.2").equals("1.18")) {
        return null;
      }
      return PaimonCatalogFixture.create(
          fixture.getClassLoader(),
          (java.util.Map<?, ?>) arguments[0],
          (java.util.List<?>) arguments[1],
          (java.util.List<?>) arguments[2],
          (java.util.List<?>) arguments[3]);
    }

    @Advice.OnMethodExit
    static void exit(
        @Advice.Enter Object replacement,
        @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Object result) {
      if (replacement != null) result = replacement;
    }
  }
}
