package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalNestedJoinJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("UpdatingJoin", "IntervalJoin", "TemporalJoin", "WindowJoin")
            .flatMap(family -> Stream.of(false, true)
                .flatMap(nativeFirst -> Stream.of("hashmap", "rocksdb")
                    .flatMap(backend -> Stream.of(false, true)
                        .flatMap(compiled -> Stream.of("continuation", "no-input", "checkpoint", "rescale")
                            .filter(mode -> !mode.equals("checkpoint") || nativeFirst)
                            .map(mode -> Arguments.of(family, nativeFirst, backend, compiled, mode)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void completeNestedPayloadsContinueInCleanStockWorkers(
      String family, boolean nativeFirst, String backend, boolean compiled, String mode)
      throws Exception {
    var baseline = CanonicalNestedJoinFixture.prepare(
        directory.resolve("baseline"), family, mode.equals("rescale"));
    var switched = CanonicalNestedJoinFixture.prepare(
        directory.resolve("switched"), family, mode.equals("rescale"));
    if (!compiled)
      for (var root : List.of(baseline, switched)) Files.createFile(root.resolve("ordinary-sql"));
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, family);
    var outputType = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    for (var root : List.of(baseline, switched)) {
      var actual = CanonicalFixtureFactory.committedRows(root.resolve("committed"), outputType);
      actual.sort(String::compareTo);
      assertEquals(CanonicalNestedJoinFixture.expected(root, family), actual);
    }
  }
}
