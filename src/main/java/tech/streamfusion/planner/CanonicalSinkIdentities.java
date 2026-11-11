package tech.streamfusion.planner;

interface CanonicalSinkIdentities {
  int materializerId();

  int originalMaterializerId();

  java.util.Collection<Integer> helperIds();
}
