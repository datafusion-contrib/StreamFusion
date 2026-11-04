package tech.streamfusion.operator;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalTemporalSortPrecisionTest {
  static Stream<Arguments> cases() {
    return Stream.of(0, 1, 2, 3)
        .flatMap(
            precision ->
                Stream.of(false, true)
                    .flatMap(
                        localZoned ->
                            Stream.of(false, true)
                                .flatMap(
                                    stock ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                nativeStore ->
                                                    Stream.of(1, 128)
                                                        .flatMap(
                                                            maximum ->
                                                                Stream.of(0, 6)
                                                                    .map(
                                                                        layout ->
                                                                            Arguments.of(
                                                                                precision,
                                                                                localZoned,
                                                                                stock,
                                                                                nativeStore,
                                                                                maximum,
                                                                                layout)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void everyReleasedTimePrecisionPreservesRawStateAndContinuation(
      int precision,
      boolean localZoned,
      boolean stock,
      boolean nativeStore,
      int maximum,
      int layout)
      throws Exception {
    var plan = new CanonicalTemporalSortInteropTest.Plan("long", maximum, precision, localZoned);
    new CanonicalTemporalSortInteropTest().assertInterchange(plan, stock, nativeStore, layout);
  }
}
