package tech.streamfusion;

class CanonicalOrdinaryBufferedDedupJobTest extends CanonicalBufferedDedupJobTest {
  @Override
  boolean ordinary() {
    return true;
  }
}
