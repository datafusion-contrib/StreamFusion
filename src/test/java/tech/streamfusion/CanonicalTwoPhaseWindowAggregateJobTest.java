package tech.streamfusion;
class CanonicalTwoPhaseWindowAggregateJobTest extends CanonicalFixedWindowAggregateJobTest {
  @Override boolean twoPhase() { return true; }
}
