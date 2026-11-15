package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalRetractingTopNStockReferenceTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            ordinary ->
                Stream.of("hashmap", "rocksdb")
                    .flatMap(
                        backend ->
                            Stream.of(5, 10, Integer.MAX_VALUE)
                                .map(cut -> Arguments.of(ordinary, backend, cut))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 4, unit = TimeUnit.MINUTES)
  void unmodifiedStockReproducesTheFixedReference(boolean ordinary, String backend, int cut)
      throws Exception {
    CanonicalRetractingTopNJobTest input =
        ordinary
            ? new CanonicalOrdinaryRetractingTopNJobTest()
            : new CanonicalRetractingTopNJobTest();
    input.directory = directory;
    Path root = input.prepare("stock-reference", "offset-hidden", "continue");
    Path csv = root.resolve("input.csv");
    Files.write(
        csv,
        Files.readAllLines(csv).stream().limit(cut).collect(java.util.stream.Collectors.toList()));
    var runner = new CanonicalJobRunner(directory);
    runner.run(
        runner.fixtureJar(), false, root, Integer.MAX_VALUE, "none", "reference", 3, backend);
    assertEquals(
        RetractingRankStockReference.table(cut, backend),
        CanonicalRetractingTopNJobTest.materialized(
            CanonicalFixtureFactory.committedRows(
                root.resolve("committed"), CanonicalRetractingTopNJobTest.resultType()),
            "offset-hidden"));
    assertTrue(Files.exists(root.resolve("reference-stock-classpath-verified")));
  }
}
