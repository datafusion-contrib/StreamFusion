package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalFloatingWindowJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of(false, true)
            .flatMap(
                single ->
                    Stream.of("TUMBLE", "HOP", "CUMULATE", "SESSION")
                        .flatMap(
                            shape ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .map(
                                        args ->
                                            Arguments.of(
                                                single,
                                                shape,
                                                args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nullableFloatingBuffersRetainRawBitsAndCommittedResults(
      boolean single, String shape, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", single, shape, mode, ordinary);
    var switched = prepare("switched", single, shape, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(
            baseline,
            switched,
            first,
            backend,
            mode,
            shape.equals("SESSION") ? "SessionWindowAgg" : "WindowAgg",
            CanonicalFloatingWindowJobTest::committed);
    var expected = expected(single, shape, input(single, mode));
    for (var root : List.of(baseline, switched)) {
      var actual = committed(root);
      actual.sort(String::compareTo);
      assertEquals(
          expected, actual, "Full floating bits, NULL-only owners and window multiplicities");
    }
  }

  private Path prepare(String name, boolean single, String shape, String mode, boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("source-watermarks"));
    Files.createFile(root.resolve("wait-native-input"));
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    String type = single ? "FLOAT" : "DOUBLE";
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v " + type + ",rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.writeString(root.resolve("output-columns.sql"), "k BIGINT,n " + type + ",total BIGINT");
    boolean legacy =
        shape.equals("SESSION") && !tech.streamfusion.compat.CanonicalTestCompat.flink22();
    if (legacy) Files.createFile(root.resolve("same-cut-stock-reference"));
    String source;
    if (legacy) source = "input_table GROUP BY k,SESSION(rt,INTERVAL '1' SECOND)";
    else {
      String assignment =
          shape.equals("SESSION")
              ? "SESSION(TABLE input_table PARTITION BY k,DESCRIPTOR(rt),INTERVAL '1' SECOND)"
              : shape.equals("HOP")
                  ? "HOP(TABLE input_table,DESCRIPTOR(rt),INTERVAL '1' SECOND,INTERVAL '3' SECOND)"
                  : shape.equals("CUMULATE")
                      ? "CUMULATE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '1' SECOND,INTERVAL '3'"
                          + " SECOND)"
                      : "TUMBLE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)";
      source = "TABLE(" + assignment + ") GROUP BY k,window_start,window_end";
    }
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,SUM(v),COUNT(v) FROM " + source);
    String family = shape.equals("SESSION") ? "SessionWindowAgg" : "WindowAgg";
    Files.writeString(
        root.resolve("native-operator"),
        (shape.equals("SESSION")
                ? ordinary
                    ? "NativeColumnarSessionWindowAggExecNode"
                    : "NativeColumnarSessionWindowAggregate"
                : ordinary ? "NativeColumnarWindowAggExecNode" : "NativeColumnarWindowAggregate")
            + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), family);
    Files.writeString(
        root.resolve("required-plan-node"),
        legacy ? "stream-exec-group-window-aggregate" : "stream-exec-window-aggregate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    var serializer =
        new RowDataSerializer(
            RowType.of(new BigIntType(), valueType(single), new TimestampType(3)));
    var rows = new ArrayList<String>();
    for (var row : input(single, mode)) {
      Object value = row.value;
      if (single && row.value != null) value = row.value.floatValue();
      rows.add(
          "BINARY,"
              + encode(
                  serializer,
                  GenericRowData.of(row.key, value, TimestampData.fromEpochMillis(row.time))));
    }
    rows.add("WATERMARK,9999");
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }

  private static LogicalType valueType(boolean single) {
    return single ? new FloatType() : new DoubleType();
  }

  private static List<Input> input(boolean single, String mode) {
    double large = single ? 16_777_216.0 : 9_007_199_254_740_992.0;
    if (mode.equals("rescale")) {
      var result = new ArrayList<Input>();
      Double[] values = {-0.0, null, large, 1.0, -large, 2.5};
      for (int round = 0; round < values.length; round++)
        for (long key = 0; key < 64; key++)
          result.add(new Input(key == 0 ? null : key, values[round], 100 + round));
      return result;
    }
    return List.of(
        new Input(1L, -0.0, 100), new Input(1L, null, 100),
        new Input(2L, -0.0, 100), new Input(3L, null, 100),
        new Input(1L, large, 100), new Input(1L, 1.0, 100),
        new Input(1L, -large, 100), new Input(2L, null, 100),
        new Input(4L, -0.0, 1200), new Input(1L, 2.5, 3100));
  }

  private static List<String> expected(boolean single, String shape, List<Input> rows)
      throws Exception {
    var owners = new HashMap<String, Owner>();
    if (shape.equals("SESSION")) {
      var ordered = new ArrayList<>(rows);
      ordered.sort(Comparator.comparingLong(row -> row.time));
      var sessions = new HashMap<Long, Owner>();
      for (var row : ordered) {
        var owner = sessions.get(row.key);
        if (owner == null || row.time > owner.end) {
          owner = new Owner(row.key, row.time + 1000);
          sessions.put(row.key, owner);
          owners.put(row.key + ":" + row.time, owner);
        } else owner.end = Math.max(owner.end, row.time + 1000);
        owner.add(row.value, single);
      }
    } else {
      for (var row : rows) {
        long base = Math.floorDiv(row.time, 3000) * 3000;
        if (shape.equals("TUMBLE")) add(owners, row, base + 3000, single);
        else if (shape.equals("CUMULATE")) {
          for (long end = base + 1000; end <= base + 3000; end += 1000)
            if (row.time < end) add(owners, row, end, single);
        } else {
          for (long start = Math.floorDiv(row.time, 1000) * 1000;
              start + 3000 > row.time;
              start -= 1000) add(owners, row, start + 3000, single);
        }
      }
    }
    var output =
        new RowDataSerializer(RowType.of(new BigIntType(), valueType(single), new BigIntType()));
    var result = new ArrayList<String>();
    for (var owner : owners.values()) {
      Object sum = owner.sum;
      if (single && owner.sum != null) sum = owner.sum.floatValue();
      result.add(encode(output, GenericRowData.of(owner.key, sum, owner.count)));
    }
    result.sort(String::compareTo);
    return result;
  }

  private static void add(Map<String, Owner> owners, Input row, long end, boolean single) {
    owners
        .computeIfAbsent(row.key + ":" + end, ignored -> new Owner(row.key, end))
        .add(row.value, single);
  }

  private static List<String> committed(Path root) throws Exception {
    return CanonicalFixtureFactory.committedRecords(root.resolve("committed")).stream()
        .map(bytes -> Base64.getEncoder().encodeToString(bytes))
        .collect(java.util.stream.Collectors.toList());
  }

  private static String encode(RowDataSerializer serializer, RowData row) throws Exception {
    var bytes = new DataOutputSerializer(128);
    serializer.serialize(row, bytes);
    return Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
  }

  private static final class Input {
    final Long key;
    final Double value;
    final long time;

    Input(Long key, Double value, long time) {
      this.key = key;
      this.value = value;
      this.time = time;
    }
  }

  private static final class Owner {
    final Long key;
    Double sum;
    long end, count;

    Owner(Long key, long end) {
      this.key = key;
      this.end = end;
    }

    void add(Double value, boolean single) {
      if (value == null) return;
      if (single)
        sum =
            sum == null
                ? (double) value.floatValue()
                : (double) (sum.floatValue() + value.floatValue());
      else sum = sum == null ? value : sum + value;
      count++;
    }
  }
}
