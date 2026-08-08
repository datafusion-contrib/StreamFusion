package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;

/**
 * The services a {@link Substitution} needs from the scan that runs it: rewriting a keyed host
 * exchange into a columnar one, counting a substitution, and recording why one was declined.
 *
 * <p>Passing these through a context rather than leaving them on the scan is what lets each
 * operator's planning live next to its matcher instead of inside the scan itself.
 */
final class PlanContext {

  private final PhysicalPlanScan scan;
  private final boolean kafkaExtension;

  PlanContext(PhysicalPlanScan scan, boolean kafkaExtension) {
    this.scan = scan;
    this.kafkaExtension = kafkaExtension;
  }

  /** Counts one host node replaced by a native one. */
  void substituted() {
    scan.countSubstitution();
  }

  /** Records why a node the scan recognized stays on the host. */
  void decline(String reason) {
    scan.recordFallback(reason);
  }

  /**
   * Whether the optional Kafka extension is linked. Only the Calc's projection pushdown needs this:
   * it inspects its input for native Kafka decode, and naming that class at all is
   * unsafe without the connector on the classpath.
   */
  boolean kafkaExtension() {
    return kafkaExtension;
  }

  /**
   * Replaces a keyed host exchange with a native columnar one (splitting the batch by the key), so the
   * shuffle is always part of the columnar island. When the exchange's producer is rowwise
   * the transition pass inserts a single transpose below the native exchange (the island perimeter);
   * when it is columnar no transpose is needed. A non-exchange input is returned unchanged.
   */
  RelNode columnarInput(RelNode input, int[] keyColumns) {
    if (input instanceof StreamPhysicalExchange) {
      return new StreamPhysicalNativeColumnarExchange(
          input.getCluster(),
          input.getTraitSet(),
          input.getInputs().get(0),
          input.getRowType(),
          keyColumns);
    }
    return input;
  }
}
