package tech.streamfusion.planner.stream;

interface CanonicalSinkIdentities {
  int materializerId();

  int originalMaterializerId();

  java.util.Collection<Integer> helperIds();
}
