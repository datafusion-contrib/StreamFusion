package tech.streamfusion;

class CanonicalFloatingOverDistinctCountJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "DOUBLE";
  }
}
