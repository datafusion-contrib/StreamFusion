package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

abstract class CanonicalEventUnboundedOverJobTest extends CanonicalUnboundedOverJobTest {
  static Stream<Arguments> eventCases(
      String frame, boolean cleanup, boolean global, boolean recovery) {
    String kind = frame + (cleanup ? "-cleanup" : "");
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(args -> recovery != args.get()[0].equals("continue"))
        .filter(args -> !global || !args.get()[0].equals("rescale"))
        .map(args -> Arguments.of(kind, args.get()[0], args.get()[1], args.get()[2], args.get()[3]))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.eventOver.job.smoke")
                    || !global
                        && args.get()[1].equals(recovery ? "checkpoint" : "continue")
                        && (kind.equals("rows-cleanup")
                                && !(boolean) args.get()[2]
                                && (boolean) args.get()[3] == recovery
                                && args.get()[4].equals("hashmap")
                            || kind.equals("range")
                                && (boolean) args.get()[2]
                                && (boolean) args.get()[3]
                                && args.get()[4].equals("rocksdb")));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    boolean rows = kind.startsWith("rows");
    var root = super.prepare(name, rows ? "rows" : "range", mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.createFile(root.resolve("source-watermarks"));
    // Reuse the controlled event-time cuts from the existing bounded OVER job matrix.
    var events = new CanonicalBoundedOverJobTest();
    events.directory = directory.resolve(name + "-event-input");
    var input = events.prepare("rows", rows ? "event-rows" : "event-range", mode, ordinary);
    Files.copy(
        input.resolve("input.csv"),
        root.resolve("input.csv"),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    if (kind.endsWith("-cleanup")) Files.writeString(root.resolve("state-ttl"), "1 h");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT "
            + (global() ? "CAST(0 AS BIGINT)" : "k")
            + ",SUM(v) OVER w,AVG(v) OVER w FROM input_table WINDOW w AS ("
            + (global() ? "" : "PARTITION BY k ")
            + "ORDER BY rt "
            + (rows ? "ROWS" : "RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }
}
