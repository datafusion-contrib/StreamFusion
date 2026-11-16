package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

class CanonicalRetractingWindowJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("TUMBLE", "HOP", "CUMULATE")
            .flatMap(
                shape ->
                    Stream.of("INT", "BIGINT", "DECIMAL(18,2)")
                        .flatMap(
                            type ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .map(
                                        args ->
                                            Arguments.of(
                                                shape,
                                                type,
                                                args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retractingWindowsKeepSourceProgressRawStateAndTransactionalOutput(
      String shape, String type, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", shape, type, mode, ordinary);
    var switched = prepare("switched", shape, type, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "WindowAgg");
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    for (var root : List.of(baseline, switched)) {
      var actual = CanonicalFixtureFactory.committedRows(root.resolve("committed"), output);
      actual.sort(String::compareTo);
      assertEquals(expected(shape, input(mode)), actual);
    }
  }

  private Path prepare(String name, String shape, String type, String mode, boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("source-watermarks"));
    Files.createFile(root.resolve("wait-native-input"));
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v " + type + ",t BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    var rows = new ArrayList<String>();
    int index = 0;
    for (var row : input(mode))
      rows.add(row.kind + "," + row.key + "," + row.value + "," + index++ + "," + row.time);
    rows.add("WATERMARK,9999");
    Files.write(root.resolve("input.csv"), rows);
    String assignment =
        shape
            + "(TABLE input_table,DESCRIPTOR(rt),INTERVAL '"
            + (shape.equals("TUMBLE") ? "3" : "1")
            + "' SECOND"
            + (shape.equals("TUMBLE") ? "" : ",INTERVAL '3' SECOND")
            + ")";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,CAST(SUM(v) AS BIGINT),CAST(AVG(v) AS BIGINT)"
            + " FROM TABLE("
            + assignment
            + ") GROUP BY k,window_start,window_end");
    Files.writeString(
        root.resolve("native-operator"),
        (ordinary ? "NativeColumnarWindowAggExecNode" : "NativeColumnarWindowAggregate")
            + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "WindowAgg");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-window-aggregate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static List<Input> input(String mode) {
    if (mode.equals("rescale")) {
      var result = new ArrayList<Input>();
      String[] kinds = {"INSERT", "INSERT", "DELETE", "UPDATE_BEFORE", "UPDATE_AFTER", "INSERT"};
      long[] values = {6, 5, 6, 5, 7, 3};
      for (int round = 0; round < kinds.length; round++)
        for (long key = 0; key < 64; key++)
          result.add(new Input(kinds[round], key == 0 ? null : key, values[round], 1000));
      return result;
    }
    return List.of(
        new Input("INSERT", 1L, null, 1000), new Input("INSERT", 1L, 7L, 1000),
        new Input("INSERT", 2L, 5L, 1001), new Input("INSERT", null, 3L, 1000),
        new Input("UPDATE_BEFORE", 1L, 7L, 1000), new Input("UPDATE_AFTER", 1L, 8L, 1000),
        new Input("DELETE", 2L, 5L, 1001), new Input("INSERT", 2L, null, 1001),
        new Input("INSERT", 1L, 2L, 1100), new Input("DELETE", 1L, null, 1000),
        new Input("INSERT", 3L, 7L, 3100), new Input("INSERT", null, 5L, 3500));
  }

  private static List<String> expected(String shape, List<Input> rows) {
    var windows = new HashMap<String, Fold>();
    for (var row : rows) {
      long base = Math.floorDiv(row.time, 3000) * 3000;
      if (shape.equals("TUMBLE")) add(windows, row, base + 3000);
      else if (shape.equals("CUMULATE")) {
        for (long end = base + 1000; end <= base + 3000; end += 1000)
          if (row.time < end) add(windows, row, end);
      } else {
        for (long start = Math.floorDiv(row.time, 1000) * 1000;
            start + 3000 > row.time;
            start -= 1000) add(windows, row, start + 3000);
      }
    }
    var result = new ArrayList<String>();
    for (var fold : windows.values()) {
      if (fold.rows == 0) continue;
      result.add(
          "+I|"
              + fold.key
              + "|"
              + (fold.count == 0 ? "null|null" : fold.sum + "|" + fold.sum / fold.count));
    }
    result.sort(String::compareTo);
    return result;
  }

  private static void add(Map<String, Fold> windows, Input row, long end) {
    var fold = windows.computeIfAbsent(row.key + ":" + end, ignored -> new Fold(row.key));
    int sign = row.kind.equals("DELETE") || row.kind.equals("UPDATE_BEFORE") ? -1 : 1;
    fold.rows += sign;
    if (row.value != null) {
      fold.sum += sign * row.value;
      fold.count += sign;
    }
  }

  private static final class Input {
    final String kind;
    final Long key, value;
    final long time;

    Input(String kind, Long key, Long value, long time) {
      this.kind = kind;
      this.key = key;
      this.value = value;
      this.time = time;
    }
  }

  private static final class Fold {
    final Long key;
    long rows, count, sum;

    Fold(Long key) {
      this.key = key;
    }
  }
}
