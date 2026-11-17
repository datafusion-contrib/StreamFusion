package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
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

abstract class CanonicalSinkMaterializerJobTest {
  @TempDir Path directory;

  static Stream<Arguments> allCases() {
    var strategies =
        tech.streamfusion.compat.MaterializerStateCompat.supportsV2()
            ? Stream.of("LEGACY", "VALUE", "MAP", "ADAPTIVE_VALUE", "ADAPTIVE_MAP")
            : Stream.of("LEGACY");
    return CanonicalTestMatrix.select(
        strategies.flatMap(
            strategy ->
                Stream.of(false, true)
                    .flatMap(
                        inputKey ->
                            Stream.of("DISABLED", "ALWAYS")
                                .flatMap(
                                    uids ->
                                        CanonicalGroupArithmeticJobTest.cases()
                                            .map(
                                                args ->
                                                    Arguments.of(
                                                        strategy,
                                                        inputKey,
                                                        uids,
                                                        args.get()[0],
                                                        args.get()[1],
                                                        args.get()[2],
                                                        args.get()[3]))))));
  }

  @ParameterizedTest
  @CanonicalCases(
      value = "cases",
      recoveryAxes = {3, 5, 6})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void releasedSinkContinuesThroughCanonicalNativeStateAndBack(
      String strategy,
      boolean inputKey,
      String uids,
      String mode,
      boolean ordinary,
      boolean nativeFirst,
      String backend)
      throws Exception {
    var baseline = prepareFixture(directory, "baseline", strategy, inputKey, uids, mode, ordinary);
    var switched = prepareFixture(directory, "switched", strategy, inputKey, uids, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, "SinkMaterializer");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var materialized = new HashMap<String, String>();
    for (String row : CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type)) {
      var cells = row.split("\\|", -1);
      if (cells[0].equals("+I") || cells[0].equals("+U"))
        materialized.put(cells[1], cells[2] + "|" + cells[3]);
      else if (cells[0].equals("-D")) materialized.remove(cells[1]);
      else fail("Materializer emitted an unexpected changelog kind: " + row);
    }
    if (mode.equals("rescale")) {
      var expected = new HashMap<String, String>();
      for (int key = 0; key < 64; key++) expected.put(Integer.toString(key), "30|300");
      assertEquals(expected, materialized);
    } else assertEquals(java.util.Map.of("1", "10|100"), materialized);
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else
      assertFalse(Files.readString(switched.resolve("plan.json")).contains("tech.streamfusion"));
  }

  static Path prepareFixture(
      Path directory,
      String name,
      String strategy,
      boolean inputKey,
      String uids,
      String mode,
      boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("upsert-sink"));
    Files.createFile(root.resolve("rescale-materializer"));
    Files.writeString(root.resolve("sink-materializer-strategy"), strategy);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT NOT NULL,v BIGINT,n BIGINT" + (inputKey ? ",PRIMARY KEY(k,v) NOT ENFORCED" : ""));
    Files.writeString(root.resolve("uid-generation"), uids);
    Files.writeString(
        root.resolve("native-operator"), "NativeSinkMaterializer,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "SinkMaterializer");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-sink");
    Files.writeString(
        root.resolve("query.sql"), "INSERT INTO output_table SELECT k,v,n FROM input_table");
    var rows =
        new ArrayList<>(
            List.of(
                "INSERT,1,10,100",
                "INSERT,1,20,200",
                "INSERT,2,30,300",
                "UPDATE_BEFORE,1,20,200",
                "UPDATE_AFTER,1,20,201",
                "DELETE,2,30,300",
                "INSERT,1,40,400",
                "DELETE,1,40,400",
                "DELETE,1,20,201"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      rows.clear();
      for (String change :
          List.of(
              "INSERT,%d,10,100",
              "INSERT,%d,20,200",
              "UPDATE_BEFORE,%d,20,200",
              "UPDATE_AFTER,%d,20,201",
              "DELETE,%d,20,201",
              "INSERT,%d,30,300"))
        for (int key = 0; key < 64; key++)
          rows.add(String.format(java.util.Locale.ROOT, change, key));
    }
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }
}
