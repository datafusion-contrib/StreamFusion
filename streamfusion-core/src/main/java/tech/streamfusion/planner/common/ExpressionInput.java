package tech.streamfusion.planner.common;

/** Producer properties needed by expression admission, independent of physical-node classes. */
public interface ExpressionInput {
  /** Whether string consumers must retain the producer's Java value representation. */
  default boolean carriesStringValues() {
    return false;
  }

  /** Unknown columnar producers retain the existing conservative collection admission. */
  default boolean unverifiedCollectionInput() {
    return true;
  }

  /** Whether a function result can share mutable objects with another expression. */
  default boolean aliasesFunctionResults() {
    return false;
  }
}
