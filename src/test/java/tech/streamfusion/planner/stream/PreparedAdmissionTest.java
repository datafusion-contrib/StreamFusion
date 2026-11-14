package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.common.Admission;

class PreparedAdmissionTest {
  private static RelNode node() {
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
    return LogicalValues.createOneRow(cluster);
  }

  private static PlanContext context() {
    return new PlanContext(new StreamPlanScan(), Set.of());
  }

  @Test
  void constructionAndRepeatedDiagnosticsUseTheSamePreparation() {
    var calls = new AtomicInteger();
    var builds = new AtomicInteger();
    var node = node();
    var context = context();
    var rule = Substitution.preparing(RelNode.class, "calc", (input, ctx) -> {
      calls.incrementAndGet();
      return Admission.accepted(() -> {
        builds.incrementAndGet();
        return input;
      });
    });
    assertSame(node, rule.apply(node, context));
    assertNull(rule.reasonFor(node, context));
    assertNull(rule.reasonFor(node, context));
    assertEquals(1, calls.get());
    assertEquals(1, builds.get());
  }

  @Test
  void declinedAdmissionRetainsItsExactReasonAndYields() {
    var calls = new AtomicInteger();
    var node = node();
    var context = context();
    var rule = Substitution.preparing(RelNode.class, "calc", (input, ctx) -> {
      int attempt = calls.incrementAndGet();
      return Admission.declined("unsupported argument in attempt " + attempt);
    });
    assertNull(rule.apply(node, context));
    assertEquals("unsupported argument in attempt 1", rule.reasonFor(node, context));
    assertEquals("unsupported argument in attempt 1", rule.reasonFor(node, context));
    assertEquals(1, calls.get());
    assertSame(node, Substitution.of(RelNode.class, (input, ctx) -> input).apply(node, context));
  }

  @Test
  void unrelatedShapeDoesNotProduceADecline() {
    var node = node();
    var context = context();
    var rule = Substitution.preparing(RelNode.class, "calc", (input, ctx) -> Admission.noMatch());
    assertNull(rule.apply(node, context));
    assertNull(rule.reasonFor(node, context));
  }

  @Test
  void disabledRuleNeverPreparesAndContextDoesNotCrossQueries() {
    var calls = new AtomicInteger();
    var rule = Substitution.preparing(RelNode.class, "calc", (input, ctx) -> {
      calls.incrementAndGet();
      return Admission.accepted(() -> input);
    });
    var node = node();
    var config = new Configuration();
    config.setString("streamfusion.operator.calc.enabled", "false");
    try (NativeConfig.Scope ignored = NativeConfig.usePlannerConfig(config)) {
      var context = context();
      assertSame(node, rule.apply(node, context));
      assertEquals(Substitution.disabledReason("calc"), rule.reasonFor(node, context));
    }
    assertEquals(0, calls.get());
    rule.apply(node, context());
    rule.apply(node, context());
    assertEquals(2, calls.get());
  }

  @Test
  void optionalProbeErrorsYieldButAdmittedConstructionErrorsPropagate() {
    var node = node();
    var context = context();
    var calls = new AtomicInteger();
    var broken = Substitution.preparing(RelNode.class, "calc", (input, ctx) -> {
      calls.incrementAndGet();
      throw new UnsatisfiedLinkError("optional native schema probe");
    });
    assertNull(broken.apply(node, context));
    assertTrue(broken.reasonFor(node, context).contains("optional native schema probe"));
    assertEquals(1, calls.get());
    var failure = new UnsatisfiedLinkError("admitted operator");
    var admitted = Substitution.preparing(RelNode.class, "calc", (input, ctx) ->
        Admission.accepted(() -> { throw failure; }));
    assertSame(failure, assertThrows(UnsatisfiedLinkError.class, () -> admitted.apply(node, context)));
  }
}
