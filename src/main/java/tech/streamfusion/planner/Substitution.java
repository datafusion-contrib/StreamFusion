package tech.streamfusion.planner;

import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;

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

  /** What the scan does with a node whose owning entry declined it. */
  enum OnDecline {
    /** The entry owned the node outright: leave it on the host and try nothing else. */
    STOP,
    /** The entry only partly owned it, so later entries still get a turn. */
    YIELD
  }

  private final Class<T> shape;
  private final String operatorKey;
  private final Planner<T> planner;
  private Predicate<T> matching = node -> true;
  private Predicate<T> owns = node -> true;
  private Function<T, String> reason;
  private boolean changelogSafe;
  private OnDecline onDecline = OnDecline.STOP;

  private Substitution(Class<T> shape, String operatorKey, Planner<T> planner) {
    this.shape = shape;
    this.operatorKey = operatorKey;
    this.planner = planner;
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

  /** Narrows the shape to the nodes this entry owns; a node it fails passes to later entries. */
  Substitution<T> matching(Predicate<T> matching) {
    this.matching = matching;
    return this;
  }

  /** Places this entry before the insert-only guard, for an operator that carries a changelog. */
  Substitution<T> changelogSafe() {
    this.changelogSafe = true;
    return this;
  }

  /** Lets later entries see a node this one declined, for a shape two entries share unevenly. */
  Substitution<T> yieldingOnDecline() {
    this.onDecline = OnDecline.YIELD;
    return this;
  }

  /** Why a node of this shape was not accelerated, for the fallback report. */
  Substitution<T> reason(Function<T, String> reason) {
    this.reason = reason;
    return this;
  }

  /**
   * Restricts which nodes of the shape this entry explains, for a shape several entries share (a
   * time-ordered rank is deduplication, any other is Top-N). Independent of {@link #matching}: a
   * reason is wanted precisely when the matcher declined.
   */
  Substitution<T> explaining(Predicate<T> owns) {
    this.owns = owns;
    return this;
  }

  boolean isChangelogSafe() {
    return changelogSafe;
  }

  /**
   * This entry's outcome for one node: the native replacement, the node itself when the entry owns it
   * but declined, or null when the node is not its business (or it declined and yields).
   */
  RelNode apply(RelNode node, PlanContext ctx) {
    if (!shape.isInstance(node)) {
      return null;
    }
    T typed = shape.cast(node);
    if (!matching.test(typed)) {
      return null;
    }
    if (operatorKey != null && !NativeConfig.operatorEnabled(operatorKey)) {
      ctx.decline(disabledReason(operatorKey));
      return node;
    }
    RelNode planned = planner.plan(typed, ctx);
    if (planned != null) {
      ctx.substituted();
      return planned;
    }
    return onDecline == OnDecline.STOP ? node : null;
  }

  /** How a matched operator kept on the host by config reports itself. */
  static String disabledReason(String operatorKey) {
    return operatorKey
        + ": disabled by config (streamfusion.operator."
        + operatorKey
        + ".enabled=false)";
  }

  /** Why this node fell back, or null when this entry does not explain nodes like it. */
  String reasonFor(RelNode node) {
    if (reason == null || !shape.isInstance(node)) {
      return null;
    }
    T typed = shape.cast(node);
    return owns.test(typed) ? reason.apply(typed) : null;
  }
}
