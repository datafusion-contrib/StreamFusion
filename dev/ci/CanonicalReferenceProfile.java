package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Invokes unchanged full-job assertions outside the default test inventory. */
public final class CanonicalReferenceProfile {
  public static void main(String[] arguments) throws Exception {
    Path root = Path.of(arguments[0]);
    String family = arguments[1];
    for (boolean first : new boolean[] {true, false})
      for (String backend : new String[] {"hashmap", "rocksdb"}) {
        Path scenario = Files.createDirectories(root.resolve(first + "-" + backend));
        if (family.equals("materializer")) {
          if (CanonicalTypedMaterializerIntJobTest.cases()
              .noneMatch(
                  args ->
                      Arrays.equals(
                          args.get(),
                          new Object[] {
                            "scalar-int",
                            "LEGACY",
                            false,
                            "ALWAYS",
                            "continue",
                            false,
                            first,
                            backend
                          })))
            throw new AssertionError(
                "Selected materializer invocation is absent from the original provider");
          var test = new CanonicalTypedMaterializerIntJobTest();
          test.directory = Files.createDirectories(scenario.resolve("materializer"));
          test.typedCandidateRowsReachReleasedStockWritersAfterHandoff(
              "scalar-int", "LEGACY", false, "ALWAYS", "continue", false, first, backend);
        } else if (family.equals("count")) {
          if (CanonicalCountPayloadGroupJobTest.cases()
              .noneMatch(
                  args ->
                      Arrays.equals(
                          args.get(), new Object[] {"mini:row:continue", false, first, backend})))
            throw new AssertionError(
                "Selected count invocation is absent from the original provider");
          var test = new CanonicalCountPayloadGroupJobTest();
          test.directory = Files.createDirectories(scenario.resolve("count"));
          test.generatedArithmeticStateContinuesAcrossIsolatedEngines(
              "mini:row:continue", false, first, backend);
        } else throw new IllegalArgumentException("Unknown family: " + family);
        System.out.println("CANONICAL_CASE " + family + " " + first + " " + backend + " passed");
      }
  }
}
