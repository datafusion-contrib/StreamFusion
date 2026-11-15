package tech.streamfusion;

class CanonicalLegacySessionJobTest extends CanonicalSessionWindowAggregateJobTest {
  @Override
  boolean legacySession() {
    return true;
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> continuedCases() {
    return CanonicalTestMatrix.select(
        java.util.stream.Stream.of(false, true)
            .flatMap(
                ordinary ->
                    java.util.stream.Stream.of(false, true)
                        .flatMap(
                            first ->
                                java.util.stream.Stream.of("hashmap", "rocksdb")
                                    .map(
                                        backend ->
                                            org.junit.jupiter.params.provider.Arguments.of(
                                                "continue", ordinary, first, backend)))));
  }
}
