package tech.streamfusion.planner.stream;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.common.NativeProbeFailure;
import tech.streamfusion.planner.common.StreamRule;

/**
 * One host operator shape the scan can replace with a native one.
 *
 * <p>An entry states declaratively what the substitution chain used to encode positionally: which
 * host rel it owns, the config key gating it, whether it is changelog-safe — which decides its side
 * of the insert-only guard, rather than its line number — and why it might decline, so a fallback
 * reason lives with the matcher that produced it instead of in a second dispatch over the same
 * shapes.
 *
 * @param <T> the host rel this entry owns
 */
final class Substitution<T extends RelNode> {

  /** Builds the native replacement for one host node, or returns null to decline it. */
  interface Planner<T> {
    RelNode plan(T node, PlanContext ctx);
  }

  private final StreamRule<T> rule;

  private Substitution(Class<T> shape, String operatorKey, Planner<T> planner) {
    this(
        StreamRule.of(
            shape, operatorKey, (node, context) -> planner.plan(node, (PlanContext) context)));
  }

  private Substitution(StreamRule<T> rule) {
    this.rule = rule;
  }

  static <T extends RelNode> Substitution<T> from(StreamRule<T> rule) {
    return new Substitution<>(rule);
  }

  /** An entry gated by {@code streamfusion.operator.<operatorKey>.enabled}. */
  static <T extends RelNode> Substitution<T> of(
      Class<T> shape, String operatorKey, Planner<T> planner) {
    return new Substitution<>(shape, operatorKey, planner);
  }

  /**
   * An entry the scan gates itself — either ungated, or one whose planner must run an earlier check
   * first so a disabled operator still reports the more specific reason.
   */
  static <T extends RelNode> Substitution<T> of(Class<T> shape, Planner<T> planner) {
    return new Substitution<>(shape, null, planner);
  }

  /** Structural ownership only: never initialize an optional native probe here. */
  Substitution<T> shaped(Predicate<T> shapeMatching) {
    rule.shaped(shapeMatching);
    return this;
  }

  /**
   * Optional acceleration admission, after host SQL validation. Do not execute user functions or
   * construct an admitted replacement here; a declined node passes to later entries.
   */
  Substitution<T> matching(Predicate<T> matching) {
    return matchingWithContext((node, ctx) -> matching.test(node));
  }

  Substitution<T> matchingWithContext(BiPredicate<T, PlanContext> matching) {
    rule.matchingWithContext((node, context) -> matching.test(node, (PlanContext) context));
    return this;
  }

  /** Places this entry before the insert-only guard, for an operator that carries a changelog. */
  Substitution<T> changelogSafe() {
    rule.changelogSafe();
    return this;
  }

  Substitution<T> keyedState() {
    rule.keyedState();
    return this;
  }

  /** Lets later entries see a node this one declined, for a shape two entries share unevenly. */
  Substitution<T> yieldingOnDecline() {
    rule.yieldingOnDecline();
    return this;
  }

  /** Why a node of this shape was not accelerated, for the fallback report. */
  Substitution<T> reason(Function<T, String> reason) {
    rule.reason(reason);
    return this;
  }

  /**
   * Restricts which nodes of the shape this entry explains, for a shape several entries share (a
   * time-ordered rank is deduplication, any other is Top-N). Independent of {@link #matching}: a
   * reason is wanted precisely when the matcher declined.
   */
  Substitution<T> explaining(Predicate<T> owns) {
    rule.explaining(owns);
    return this;
  }

  boolean isChangelogSafe() {
    return rule.isChangelogSafe();
  }

  /**
   * This entry's outcome for one node: the native replacement, the node itself when the entry owns
   * it but declined, or null when the node is not its business (or it declined and yields).
   */
  RelNode apply(RelNode node, PlanContext ctx) {
    if (!rule.shape().isInstance(node)) {
      return null;
    }
    T typed = rule.shape().cast(node);
    try {
      if (!rule.matchesShape(typed)) {
        return null;
      }
    } catch (NoClassDefFoundError
        | NoSuchMethodError
        | NoSuchFieldError
        | AbstractMethodError
        | UnsatisfiedLinkError failure) {
      ctx.probeFailed(this, node, NativeProbeFailure.reason(failure));
      return null;
    }
    if (rule.operatorKey() != null && !NativeConfig.operatorEnabled(rule.operatorKey())) {
      ctx.decline(disabledReason(rule.operatorKey()));
      return node;
    }
    try {
      if (!rule.matches(typed, ctx)) {
        return null;
      }
    } catch (NoClassDefFoundError
        | NoSuchMethodError
        | NoSuchFieldError
        | AbstractMethodError
        | UnsatisfiedLinkError failure) {
      ctx.probeFailed(this, node, NativeProbeFailure.reason(failure));
      return null;
    }
    if (rule.requiresKeyedState() && ctx.keyedStateUnsupportedReason() != null) {
      ctx.decline(ctx.keyedStateUnsupportedReason());
      return node;
    }
    RelNode planned = rule.plan(typed, ctx);
    if (planned != null) {
      ctx.substituted();
      return planned;
    }
    return rule.yieldsOnDecline() ? null : node;
  }

  /** How a matched operator kept on the host by config reports itself. */
  static String disabledReason(String operatorKey) {
    return operatorKey
        + ": disabled by config (streamfusion.operator."
        + operatorKey
        + ".enabled=false)";
  }

  /** Why this node fell back, or null when this entry does not explain nodes like it. */
  String reasonFor(RelNode node, PlanContext ctx) {
    String failedProbe = ctx.probeFailure(this, node);
    if (failedProbe != null) {
      return failedProbe;
    }
    if (!rule.shape().isInstance(node)) {
      return null;
    }
    T typed = rule.shape().cast(node);
    try {
      if (!rule.matchesShape(typed)) {
        return null;
      }
      if (rule.operatorKey() != null && !NativeConfig.operatorEnabled(rule.operatorKey())) {
        return disabledReason(rule.operatorKey());
      }
      return rule.reason(typed);
    } catch (NoClassDefFoundError
        | NoSuchMethodError
        | NoSuchFieldError
        | AbstractMethodError
        | UnsatisfiedLinkError failure) {
      String diagnostic = NativeProbeFailure.reason(failure);
      ctx.probeFailed(this, node, diagnostic);
      return diagnostic;
    }
  }
}
