package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;

/** Contributes planner rewrites from an optional connector artifact. */
public interface NativePlannerExtension {

  /**
   * Adds this artifact's substitutions in planner priority order to its private contribution.
   * The rules and source-sharing hook become active only if registration completes successfully.
   */
  void addSubstitutions(List<Substitution<?>> substitutions);

  /** Returns a pre-substitution source-sharing key, or {@code null} for an unrelated node. */
  default String sourceSharingKey(RelNode node) {
    return null;
  }
}
