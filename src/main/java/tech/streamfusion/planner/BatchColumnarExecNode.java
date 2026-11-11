package tech.streamfusion.planner;

/** Lets graph processors preserve the Arrow wire representation when inserting task boundaries. */
interface BatchColumnarExecNode {
  default boolean outputsArrow() {
    return true;
  }
}
