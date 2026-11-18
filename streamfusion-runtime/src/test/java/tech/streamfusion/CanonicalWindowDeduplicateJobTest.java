package tech.streamfusion;

class CanonicalWindowDeduplicateJobTest extends CanonicalWindowRankJobTest {
  @Override
  boolean deduplicate() {
    return true;
  }
}
