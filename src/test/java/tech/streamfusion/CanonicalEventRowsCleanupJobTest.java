package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalEventRowsCleanupJobTest extends CanonicalBoundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  private static Stream<Arguments> cases(boolean recovery) {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(args -> recovery != args.get()[0].equals("continue"))
        .map(args -> Arguments.of("event-rows", args.get()[0], args.get()[1], args.get()[2], args.get()[3]));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(root.resolve("state-ttl"), "1 h");
    return root;
  }
}
