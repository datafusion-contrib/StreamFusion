package tech.streamfusion;

class CanonicalGlobalUnboundedOverConstantJobTest extends CanonicalUnboundedOverConstantJobTest {
  @Override
  boolean global() {
    return true;
  }
}
