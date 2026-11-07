package tech.streamfusion;

class CanonicalGlobalUnboundedOverJobTest extends CanonicalUnboundedOverJobTest {
  @Override
  boolean global() {
    return true;
  }
}
