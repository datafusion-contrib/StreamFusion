package tech.streamfusion.planner.common;

import org.apache.calcite.rel.RelNode;

/** Services exposed to optional connector rules without exposing the streaming rewrite state. */
public interface StreamPlanningContext {
  void decline(String reason);

  boolean repeatedSource(String sharingKey);

  String keyedStateUnsupportedReason();

  RelNode originalNode(RelNode rewritten);
}
