package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUpdateRankOffsetJobTest extends CanonicalScalarRankJobTest {
  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("BIGINT", "DECIMAL(38,4)")
            .flatMap(
                type ->
                    CanonicalGroupArithmeticJobTest.cases()
                        .map(
                            args ->
                                Arguments.of(
                                    "update",
                                    type,
                                    args.get()[0],
                                    args.get()[1],
                                    args.get()[2],
                                    args.get()[3]))));
  }

  @Override
  int firstRank() {
    return 2;
  }
}
