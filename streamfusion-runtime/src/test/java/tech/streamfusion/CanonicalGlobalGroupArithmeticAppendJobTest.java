package tech.streamfusion;

class CanonicalGlobalGroupArithmeticAppendJobTest extends CanonicalGlobalGroupArithmeticJobTest {
  @Override
  boolean appendOnly() {
    return true;
  }
}
