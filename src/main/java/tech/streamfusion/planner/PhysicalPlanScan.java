package tech.streamfusion.planner;

/** Compatibility entry point; streaming rewrites live in the mode-specific planner package. */
public final class PhysicalPlanScan extends tech.streamfusion.planner.stream.StreamPlanScan {
  public PhysicalPlanScan() {
    super();
  }
}
