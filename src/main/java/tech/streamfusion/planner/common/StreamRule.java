package tech.streamfusion.planner.common;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;

/** Optional connector contribution; execution, ordering and probe failures belong to the scan. */
public final class StreamRule<T extends RelNode> {
  @FunctionalInterface
  public interface Planner<T> {
    RelNode plan(T node, StreamPlanningContext context);
  }

  private final Class<T> shape;
  private final String operatorKey;
  private final Planner<T> planner;
  private Predicate<T> shapeMatching = node -> true;
  private BiPredicate<T, StreamPlanningContext> matching = (node, context) -> true;
  private Predicate<T> owns = node -> true;
  private Function<T, String> reason;
  private boolean changelogSafe;
  private boolean keyedState;
  private boolean yielding;

  private StreamRule(Class<T> shape, String operatorKey, Planner<T> planner) {
    this.shape = shape;
    this.operatorKey = operatorKey;
    this.planner = planner;
  }

  public static <T extends RelNode> StreamRule<T> of(
      Class<T> shape, String operatorKey, Planner<T> planner) {
    return new StreamRule<>(shape, operatorKey, planner);
  }

  public static <T extends RelNode> StreamRule<T> of(Class<T> shape, Planner<T> planner) {
    return of(shape, null, planner);
  }

  public StreamRule<T> shaped(Predicate<T> predicate) {
    shapeMatching = predicate;
    return this;
  }

  public StreamRule<T> matching(Predicate<T> predicate) {
    return matchingWithContext((node, context) -> predicate.test(node));
  }

  public StreamRule<T> matchingWithContext(BiPredicate<T, StreamPlanningContext> predicate) {
    matching = predicate;
    return this;
  }

  public StreamRule<T> changelogSafe() {
    changelogSafe = true;
    return this;
  }

  public StreamRule<T> keyedState() {
    keyedState = true;
    return this;
  }

  public StreamRule<T> yieldingOnDecline() {
    yielding = true;
    return this;
  }

  public StreamRule<T> reason(Function<T, String> reason) {
    this.reason = reason;
    return this;
  }

  public StreamRule<T> explaining(Predicate<T> owns) {
    this.owns = owns;
    return this;
  }

  public Class<T> shape() {
    return shape;
  }

  public String operatorKey() {
    return operatorKey;
  }

  public boolean matchesShape(T node) {
    return shapeMatching.test(node);
  }

  public boolean matches(T node, StreamPlanningContext context) {
    return matching.test(node, context);
  }

  public RelNode plan(T node, StreamPlanningContext context) {
    return planner.plan(node, context);
  }

  public String reason(T node) {
    return reason != null && owns.test(node) ? reason.apply(node) : null;
  }

  public boolean isChangelogSafe() {
    return changelogSafe;
  }

  public boolean requiresKeyedState() {
    return keyedState;
  }

  public boolean yieldsOnDecline() {
    return yielding;
  }
}
