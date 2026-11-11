package tech.streamfusion;

class CanonicalLegacyDistinctSessionJobTest extends CanonicalLegacySessionJobTest {
  @Override
  boolean distinct() {
    return true;
  }
}
