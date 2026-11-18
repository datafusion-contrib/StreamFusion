package tech.streamfusion;

class CanonicalUnboundedOverNarrowDistinctJobTest
    extends CanonicalUnboundedOverNullableDistinctArithmeticJobTest {
  @Override
  String argumentType() {
    return "TINYINT";
  }
}
