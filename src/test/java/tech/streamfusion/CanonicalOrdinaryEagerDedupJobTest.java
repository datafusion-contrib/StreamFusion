package tech.streamfusion;

class CanonicalOrdinaryEagerDedupJobTest extends CanonicalEagerDedupJobTest {
  @Override
  boolean ordinary() {
    return true;
  }
}
