package tech.streamfusion;

class CanonicalWindowDedupPayloadJobTest extends CanonicalWindowRankPayloadJobTest {
  @Override
  boolean deduplicate() {
    return true;
  }
}
