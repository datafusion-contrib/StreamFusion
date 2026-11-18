package tech.streamfusion;

class CanonicalTwoPhaseMiniBatchStringJobTest extends CanonicalMiniBatchStringJobTest {
  @Override
  boolean twoPhase() {
    return true;
  }
}
