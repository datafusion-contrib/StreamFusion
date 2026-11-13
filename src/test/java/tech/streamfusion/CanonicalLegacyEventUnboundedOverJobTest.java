package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalLegacyEventUnboundedOverJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  private static Stream<Arguments> cases(boolean recovery) {
    return Stream.of("rows", "range")
        .flatMap(frame -> Stream.of(false, true)
            .flatMap(cleanup -> eventCases(frame, cleanup, false, recovery)));
  }

  @Override
  boolean global() {
    return false;
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(root.resolve("unbounded-over-version"), "1");
    return root;
  }
}
