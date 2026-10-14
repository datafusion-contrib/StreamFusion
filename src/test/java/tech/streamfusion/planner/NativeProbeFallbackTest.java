package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class NativeProbeFallbackTest {
  static Stream<Supplier<LinkageError>> recoverableFailures() {
    return Stream.of(
        () -> new NoClassDefFoundError("optional/module/Class"),
        () -> new NoSuchMethodError("optional.method()"),
        () -> new NoSuchFieldError("optional.field"),
        () -> new AbstractMethodError("optional.method()"),
        () -> new UnsatisfiedLinkError("optional_native_symbol"));
  }

  private static RelNode node() {
    var cluster =
        RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
    return LogicalValues.createOneRow(cluster);
  }

  private static PlanContext context() {
    return new PlanContext(new PhysicalPlanScan(), Set.of());
  }

  @ParameterizedTest
  @MethodSource("recoverableFailures")
  void failedAdmissionYieldsAndDoesNotRepeatInDiagnostics(Supplier<LinkageError> failure) {
    RelNode node = node();
    PlanContext ctx = context();
    AtomicInteger admission = new AtomicInteger();
    AtomicInteger diagnostic = new AtomicInteger();
    Substitution<RelNode> entry =
        Substitution.of(RelNode.class, (n, c) -> n)
            .matching(
                n -> {
                  admission.incrementAndGet();
                  throw failure.get();
                })
            .reason(
                n -> {
                  diagnostic.incrementAndGet();
                  throw failure.get();
                });
    assertNull(entry.apply(node, ctx));
    String reason = entry.reasonFor(node, ctx);
    assertTrue(reason.contains(failure.get().getClass().getSimpleName()), reason);
    assertEquals(reason, entry.reasonFor(node, ctx));
    assertEquals(1, admission.get());
    assertEquals(0, diagnostic.get());
    Substitution<RelNode> next = Substitution.of(RelNode.class, (n, c) -> n);
    assertSame(node, next.apply(node, ctx));
  }

  @ParameterizedTest
  @MethodSource("recoverableFailures")
  void diagnosticAndOwnershipFailuresAreContained(Supplier<LinkageError> failure) {
    RelNode node = node();
    PlanContext ctx = context();
    Substitution<RelNode> reasonFailure =
        Substitution.of(RelNode.class, (n, c) -> null)
            .matching(n -> false)
            .reason(
                n -> {
                  throw failure.get();
                });
    assertNull(reasonFailure.apply(node, ctx));
    assertTrue(
        reasonFailure.reasonFor(node, ctx).contains(failure.get().getClass().getSimpleName()));
    Substitution<RelNode> ownerFailure =
        Substitution.of(RelNode.class, (n, c) -> null)
            .explaining(
                n -> {
                  throw failure.get();
                })
            .reason(n -> "unused");
    assertTrue(
        ownerFailure.reasonFor(node, ctx).contains(failure.get().getClass().getSimpleName()));
  }

  @Test
  void recordsSpecificLinkageCategories() {
    assertTrue(NativeProbeFailure.reason(new NoClassDefFoundError()).contains("class unavailable"));
    assertTrue(NativeProbeFailure.reason(new NoSuchMethodError()).contains("incompatible method"));
    assertTrue(
        NativeProbeFailure.reason(new UnsatisfiedLinkError()).contains("native library or symbol"));
  }

  @Test
  void ordinaryDeclineAndHealthyAdmissionKeepTheirBehavior() {
    RelNode node = node();
    PlanContext ctx = context();
    Substitution<RelNode> declining =
        Substitution.of(RelNode.class, (n, c) -> null).reason(n -> "unsupported");
    assertSame(node, declining.apply(node, ctx));
    assertEquals("unsupported", declining.reasonFor(node, ctx));
    assertNull(declining.yieldingOnDecline().apply(node, ctx));
    Substitution<RelNode> nonMatching =
        Substitution.of(RelNode.class, (n, c) -> fail("not matched")).matching(n -> false);
    assertNull(nonMatching.apply(node, ctx));
    assertSame(node, Substitution.of(RelNode.class, (n, c) -> n).apply(node, ctx));
  }

  @Test
  void disabledEntryNeverInitializesItsProbes() {
    Configuration config = new Configuration();
    config.setString("streamfusion.operator.calc.enabled", "false");
    try (NativeConfig.Scope ignored = NativeConfig.usePlannerConfig(config)) {
      RelNode node = node();
      PlanContext ctx = context();
      Substitution<RelNode> entry =
          Substitution.of(RelNode.class, "calc", (n, c) -> fail("disabled planner"))
              .matching(
                  n -> {
                    fail("disabled admission");
                    return false;
                  })
              .explaining(
                  n -> {
                    fail("disabled ownership");
                    return false;
                  })
              .reason(n -> fail("disabled diagnostic"));
      assertSame(node, entry.apply(node, ctx));
      assertEquals(Substitution.disabledReason("calc"), entry.reasonFor(node, ctx));
    }
  }

  @Test
  void disabledEntryDoesNotClaimAnotherShape() {
    Configuration config = new Configuration();
    config.setString("streamfusion.operator.filter.enabled", "false");
    try (NativeConfig.Scope ignored = NativeConfig.usePlannerConfig(config)) {
      RelNode node = node();
      PlanContext ctx = context();
      Substitution<RelNode> entry =
          Substitution.of(RelNode.class, "filter", (n, c) -> fail("wrong shape"))
              .shaped(n -> false)
              .matching(
                  n -> {
                    fail("disabled probe");
                    return false;
                  })
              .reason(n -> fail("disabled diagnostic"));
      assertNull(entry.apply(node, ctx));
      assertNull(entry.reasonFor(node, ctx));
      assertSame(node, Substitution.of(RelNode.class, "calc", (n, c) -> n).apply(node, ctx));
    }
  }

  @Test
  void failuresDoNotLeakAcrossNodesOrPlanningContexts() {
    AtomicInteger calls = new AtomicInteger();
    Substitution<RelNode> entry =
        Substitution.of(RelNode.class, (n, c) -> n)
            .matching(
                n -> {
                  if (calls.getAndIncrement() == 0) throw new NoSuchMethodError("first");
                  return true;
                })
            .reason(n -> "healthy");
    RelNode node = node();
    PlanContext first = context();
    assertNull(entry.apply(node, first));
    assertEquals("healthy", entry.reasonFor(node(), first));
    PlanContext second = context();
    assertSame(node, entry.apply(node, second));
    assertEquals("healthy", entry.reasonFor(node, second));
    assertTrue(entry.reasonFor(node, first).contains("NoSuchMethodError"));
  }

  @Test
  void semanticAndFatalFailuresStillPropagate() {
    RelNode node = node();
    PlanContext ctx = context();
    for (Error failure :
        List.of(
            new OutOfMemoryError("fatal"),
            new ExceptionInInitializerError("initializer"),
            new VerifyError("invalid bytecode"),
            new AssertionError("bug"))) {
      Substitution<RelNode> entry =
          Substitution.of(RelNode.class, (n, c) -> n)
              .matching(
                  n -> {
                    throw failure;
                  })
              .reason(
                  n -> {
                    throw failure;
                  });
      assertSame(failure, assertThrows(failure.getClass(), () -> entry.apply(node, ctx)));
      assertSame(failure, assertThrows(failure.getClass(), () -> entry.reasonFor(node, ctx)));
    }
    IllegalArgumentException semantic = new IllegalArgumentException("invalid SQL/UDF");
    Substitution<RelNode> entry =
        Substitution.of(RelNode.class, (n, c) -> n)
            .matching(
                n -> {
                  throw semantic;
                })
            .reason(
                n -> {
                  throw semantic;
                });
    assertSame(
        semantic, assertThrows(IllegalArgumentException.class, () -> entry.apply(node, ctx)));
    assertSame(
        semantic, assertThrows(IllegalArgumentException.class, () -> entry.reasonFor(node, ctx)));
    NoSuchMethodError udfFailure = new NoSuchMethodError("admitted replacement or UDF");
    Substitution<RelNode> planningFailure =
        Substitution.of(
            RelNode.class,
            (n, c) -> {
              throw udfFailure;
            });
    assertSame(
        udfFailure, assertThrows(NoSuchMethodError.class, () -> planningFailure.apply(node, ctx)));
  }

  @Test
  void laterHealthyEntryStillBuildsAndExecutesNativeCalc() throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    Substitution<StreamPhysicalCalc> broken =
        Substitution.of(StreamPhysicalCalc.class, (n, c) -> n)
            .changelogSafe()
            .matching(
                n -> {
                  throw new UnsatisfiedLinkError("broken optional entry");
                })
            .reason(n -> fail("successful replacement needs no fallback probe"));
    Substitution<StreamPhysicalCalc> healthy =
        Substitution.of(StreamPhysicalCalc.class, "calc", CalcMatcher::substitute)
            .changelogSafe()
            .matching(CalcMatcher::matches)
            .reason(CalcMatcher::unsupportedReason);
    PhysicalPlanScan scan = new PhysicalPlanScan(List.of(broken, healthy));
    install(table, scan);
    assertTrue(
        table
            .explainSql("SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)")
            .contains("NativeCalc"));
    assertEquals(Set.of(Row.of(6), Row.of(8), Row.of(10)), collect(table));
    assertTrue(scan.substitutions() > 0);
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
  }

  @Test
  void disablingFilterKeepsComputedProjectionEligible() throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.getConfig().set("streamfusion.operator.filter.enabled", "false");
    PhysicalPlanScan scan = NativePlanner.install(table);
    assertTrue(
        table
            .explainSql("SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)")
            .contains("NativeCalc"));
    assertEquals(Set.of(Row.of(6), Row.of(8), Row.of(10)), collect(table));
    assertTrue(scan.substitutions() > 0);
  }

  private static Set<Row> collect(TableEnvironment table) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (var result =
        table.executeSql("SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)").collect()) {
      while (result.hasNext()) rows.add(result.next());
    }
    return Set.copyOf(rows);
  }

  private static void install(TableEnvironment table, PhysicalPlanScan scan) {
    var program = FlinkStreamProgram.buildProgram(table.getConfig());
    program.addLast("fault_injected_native_scan", scan);
    table
        .getConfig()
        .setPlannerConfig(
            CalciteConfig$.MODULE$.createBuilder().replaceStreamProgram(program).build());
  }

  @ParameterizedTest
  @MethodSource("recoverableFailures")
  void validHostSqlSurvivesNativeOnlyProbeFailure(Supplier<LinkageError> failure) throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    AtomicInteger admission = new AtomicInteger();
    AtomicInteger diagnostic = new AtomicInteger();
    Substitution<StreamPhysicalCalc> entry =
        Substitution.of(StreamPhysicalCalc.class, (n, c) -> n)
            .changelogSafe()
            .matching(
                n -> {
                  admission.incrementAndGet();
                  throw failure.get();
                })
            .reason(
                n -> {
                  diagnostic.incrementAndGet();
                  throw failure.get();
                });
    PhysicalPlanScan scan = new PhysicalPlanScan(List.of(entry));
    install(table, scan);
    List<Row> rows = new ArrayList<>();
    try (var result =
        table.executeSql("SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)").collect()) {
      while (result.hasNext()) rows.add(result.next());
    }
    assertEquals(Set.of(Row.of(6), Row.of(8), Row.of(10)), Set.copyOf(rows));
    assertEquals(0, scan.substitutions());
    assertTrue(admission.get() > 0, "the injected probe must run");
    assertEquals(0, diagnostic.get());
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(reason -> reason.contains(failure.get().getClass().getSimpleName())),
        scan.explainSummary());
    int previousCalls = admission.get();
    table.getConfig().set("streamfusion.native.enabled", "false");
    table.explainSql("SELECT c0 * 2 FROM (VALUES (3), (4), (5)) AS t(c0)");
    assertEquals(previousCalls, admission.get());
  }
}
