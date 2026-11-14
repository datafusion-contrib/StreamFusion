package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalDecimalUnboundedOverJobTest extends CanonicalUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  private static Stream<Arguments> cases(boolean recovery) {
    return CanonicalTestMatrix.select(
        Stream.of("rows", "range")
            .flatMap(
                frame ->
                    Stream.of("DECIMAL(18,2)", "DECIMAL(38,2)", "DECIMAL(38,20)")
                        .flatMap(
                            type ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .filter(args -> recovery != args.get()[0].equals("continue"))
                                    .map(
                                        args ->
                                            Arguments.of(
                                                frame + ":" + type,
                                                args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    String[] fields = kind.split(":");
    var root = super.prepare(name, fields[0], mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        Files.readString(root.resolve("input-columns.sql")).replace("v BIGINT", "v " + fields[1]));
    return root;
  }
}
