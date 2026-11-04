package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupArithmeticJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of("continue", "no-input", "checkpoint", "rescale")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        ordinary ->
                            Stream.of(false, true)
                                .filter(first -> !mode.equals("checkpoint") || first)
                                .flatMap(
                                    first ->
                                        Stream.of("hashmap", "rocksdb")
                                            .map(
                                                store ->
                                                    Arguments.of(mode, ordinary, first, store)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void generatedArithmeticStateContinuesAcrossIsolatedEngines(
      String mode, boolean ordinary, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var baseline = prepare("baseline", mode, ordinary);
    var switched = prepare("switched", mode, ordinary);
    configureBackend(baseline, backend);
    configureBackend(switched, backend);
    runner.verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, "GroupAgg");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var rows = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    var materialized = new java.util.HashMap<String, String>();
    for (String row : rows) {
      String[] cells = row.split("\\|", -1);
      if (cells[0].equals("+I") || cells[0].equals("+U"))
        materialized.put(cells[1], cells[2] + "|" + cells[3]);
      else if (cells[0].equals("-D") || cells[0].equals("-U")) materialized.remove(cells[1]);
      else fail("Unknown stock row kind: " + row);
    }
    assertEquals(expectedTable(mode), materialized);
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else {
      String portable = Files.readString(switched.resolve("plan.json"));
      assertTrue(portable.contains("stream-exec-group-aggregate"));
      assertFalse(portable.contains("tech.streamfusion"));
    }
  }

  java.util.Map<String, String> expectedTable(String mode) {
    if (mode.equals("rescale")) {
      var expected = new java.util.HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "33|5");
      return expected;
    }
    return java.util.Map.of("null", "null|null", "2", "11|11", "3", "11|5");
  }

  void configureBackend(Path root, String backend) throws Exception {}

  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    Path root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    var input =
        new ArrayList<>(
            List.of(
                "INSERT,1,null",
                "INSERT,1,7",
                "INSERT,1,4",
                "INSERT,2,10",
                "INSERT,null,20",
                "UPDATE_BEFORE,1,7",
                "UPDATE_AFTER,1,8",
                "INSERT,2,null",
                "DELETE,1,4",
                "DELETE,1,8",
                "INSERT,1,-5",
                "INSERT,1,2",
                "DELETE,2,10",
                "DELETE,2,null",
                "INSERT,2,11",
                "DELETE,null,20",
                "INSERT,null,null",
                "INSERT,1,null",
                "DELETE,1,-5",
                "DELETE,1,2",
                "DELETE,1,null",
                "DELETE,1,null",
                "INSERT,3,6",
                "INSERT,3,5"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      input.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          input.add("INSERT," + (key == 0 ? "null" : key) + "," + (round % 2 == 0 ? 6 : 5));
    }
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,SUM(v),AVG(v) FROM input_table GROUP BY k");
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarGroupAgg,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "GroupAgg");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-group-aggregate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }
}
