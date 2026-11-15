package tech.streamfusion;

class CanonicalDecimalOverDistinctCountJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "DECIMAL(38,7)";
  }
}
