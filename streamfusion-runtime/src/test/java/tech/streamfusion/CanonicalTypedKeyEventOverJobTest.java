package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.compat.FlinkPlanCompat;

abstract class CanonicalTypedKeyEventOverJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> typedCases(boolean recovery, String types) {
    return CanonicalTestMatrix.select(
        Stream.of("rows", "range")
            .flatMap(
                frame ->
                    IntStream.rangeClosed(1, FlinkPlanCompat.unboundedOverVersion())
                        .boxed()
                        .flatMap(
                            version ->
                                eventCases(frame, false, false, recovery)
                                    .map(
                                        args ->
                                            Arguments.of(
                                                frame + ":" + types + ":" + version,
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3],
                                                args.get()[4])))));
  }

  @Override
  boolean global() {
    return false;
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var fields = kind.split(":");
    var root = super.prepare(name, fields[0], mode, ordinary);
    Files.writeString(root.resolve("unbounded-over-version"), fields[3]);
    Files.writeString(
        root.resolve("input-columns.sql"),
        Files.readString(root.resolve("input-columns.sql"))
            .replace("k BIGINT", "k " + fields[1])
            .replace("v BIGINT", "v " + fields[2]));
    Files.writeString(
        root.resolve("query.sql"),
        Files.readString(root.resolve("query.sql"))
            .replace("SELECT k,", "SELECT CAST(k AS BIGINT),")
            .replace("SUM(v) OVER w", "CAST(SUM(v) OVER w AS BIGINT)")
            .replace("AVG(v) OVER w", "CAST(AVG(v) OVER w AS BIGINT)"));
    return root;
  }
}
