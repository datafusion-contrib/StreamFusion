package tech.streamfusion;

class CanonicalTimestampOverDistinctCountJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "TIMESTAMP(9)";
  }
}
