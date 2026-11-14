package tech.streamfusion.planner.common;

import org.apache.calcite.rel.RelNode;

/** A rejected operator or island, with provenance that does not retain the discarded plan. */
public final class PlanningRejection {
  public enum Scope { OPERATOR, ISLAND }

  private final Scope scope;
  private final int nodeId;
  private final String operator;
  private final String reason;

  public PlanningRejection(Scope scope, RelNode node, String reason) {
    this.scope = scope;
    this.nodeId = node == null ? -1 : node.getId();
    this.operator = node == null ? "unknown operator" : node.getRelTypeName();
    this.reason = java.util.Objects.requireNonNull(reason);
  }

  public Scope scope() { return scope; }
  public int nodeId() { return nodeId; }
  public String operator() { return operator; }
  public String reason() { return reason; }

  public String describe() {
    return scope.name().toLowerCase(java.util.Locale.ROOT) + " " + operator
        + (nodeId < 0 ? "" : " [rel#" + nodeId + "]") + ": " + reason;
  }
}
