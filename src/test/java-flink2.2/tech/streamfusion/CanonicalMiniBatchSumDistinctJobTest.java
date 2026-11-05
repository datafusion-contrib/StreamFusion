package tech.streamfusion;

class CanonicalMiniBatchSumDistinctJobTest extends CanonicalGroupSumDistinctJobTest {
  @Override
  boolean miniBatch() {
    return true;
  }
}
