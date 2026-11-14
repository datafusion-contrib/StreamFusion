package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.example.canonical.CanonicalFixtureFactory;

public final class CanonicalNestedJoinFixture {
  public static final RowType PAYLOAD =
      RowType.of(
          new LogicalType[] {
            new ArrayType(new BigIntType()),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH))
          },
          new String[] {"a", "m", "r"});
  public static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), PAYLOAD},
          new String[] {"k", "v", "p"});

  private CanonicalNestedJoinFixture() {}

  public static String columns(boolean primaryKey) {
    return "k BIGINT"
        + (primaryKey ? " NOT NULL" : "")
        + ",v BIGINT,p "
        + CanonicalFixtureFactory.NestedJoinChecksum.TYPE
        + ",rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' DAY"
        + (primaryKey ? ",PRIMARY KEY(k) NOT ENFORCED" : "");
  }

  public static String query(String family) {
    String left = "input_table l";
    String right = "right_input_table r";
    String condition = "l.k=r.k";
    if (family.equals("WindowJoin")) {
      left = "TABLE(TUMBLE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)) l";
      right = "TABLE(TUMBLE(TABLE right_input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)) r";
      condition += " AND l.window_start=r.window_start AND l.window_end=r.window_end";
    } else if (family.equals("IntervalJoin")) {
      condition += " AND l.rt BETWEEN r.rt - INTERVAL '1' SECOND AND r.rt + INTERVAL '1' SECOND";
    } else if (family.equals("TemporalJoin")) {
      right = "right_input_table FOR SYSTEM_TIME AS OF l.rt AS r";
    }
    return "INSERT INTO output_table SELECT l.k,l.v,nested_checksum(l.p,r.p) FROM "
        + left
        + " JOIN "
        + right
        + " ON "
        + condition;
  }

  public static Path prepare(Path root, String family, boolean rescale) throws Exception {
    Files.createDirectories(root);
    Files.writeString(root.resolve("input-columns.sql"), columns(false));
    Files.writeString(
        root.resolve("right_input_table-columns.sql"), columns(family.equals("TemporalJoin")));
    Files.writeString(root.resolve("query.sql"), query(family));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("nested-join-checksum"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(
        root.resolve("required-plan-node"),
        "stream-exec-"
            + (family.equals("UpdatingJoin")
                ? "join"
                : family.equals("IntervalJoin")
                    ? "interval-join"
                    : family.equals("TemporalJoin") ? "temporal-join" : "window-join"));
    Files.writeString(
        root.resolve("native-operator"),
        "Native"
            + (family.equals("UpdatingJoin") ? "ColumnarUpdatingJoin" : family)
            + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), family);
    var left = new ArrayList<String>();
    var right = new ArrayList<String>();
    if (rescale) {
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++) {
          left.add((key + 1) + "," + (round * 1000 + 100 + key));
          right.add((key + 1) + "," + (round * 1000 + 90 + key));
        }
    } else {
      left.addAll(
          List.of(
              "1,110",
              "1,110",
              "2,130",
              "null,140",
              "3,1150",
              "2,1250",
              "9,2400",
              "1,2500",
              "2,3100",
              "5,3200",
              "null,3300",
              "1,3400"));
      right.addAll(
          List.of(
              "1,100", "1,110", "4,120", "2,125", "3,1100", "2,1250", "9,2400", "1,2600", "2,3000",
              "6,3200", "1,3350", "1,3450"));
    }
    write(root, "input", left);
    write(root, "right-input", right);
    return root;
  }

  private static void write(Path root, String name, List<String> rows) throws Exception {
    var encoded = new ArrayList<String>();
    var serializer = new RowDataSerializer(INPUT);
    for (String line : rows) {
      String[] fields = line.split(",");
      Long key = fields[0].equals("null") ? null : Long.valueOf(fields[0]);
      long value = Long.parseLong(fields[1]);
      var row = GenericRowData.of(key, value, payload(value));
      var output = new DataOutputSerializer(128);
      serializer.serialize(row, output);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(output.getCopyOfBuffer()));
    }
    Files.write(root.resolve(name + ".csv"), encoded);
    Files.write(root.resolve(name + "-reference.csv"), rows);
  }

  public static RowData payload(long value) {
    if (value % 11 == 0) return null;
    var map = new LinkedHashMap<StringData, Long>();
    map.put(StringData.fromString("λ"), value + 7);
    map.put(StringData.fromString("empty"), null);
    return GenericRowData.of(
        new GenericArrayData(new Long[] {value, null, -value}),
        new GenericMapData(map),
        GenericRowData.of(value * 2, StringData.fromString("nested-" + value)));
  }

  public static List<String> expected(Path root, String family) throws Exception {
    var expected = new ArrayList<String>();
    var right = Files.readAllLines(root.resolve("right-input-reference.csv"));
    for (String line : Files.readAllLines(root.resolve("input-reference.csv"))) {
      String[] left = line.split(",");
      if (left[0].equals("null")) continue;
      long value = Long.parseLong(left[1]);
      long latest = Long.MIN_VALUE;
      for (String candidate : right) {
        String[] r = candidate.split(",");
        if (!left[0].equals(r[0])) continue;
        long rv = Long.parseLong(r[1]);
        if (family.equals("TemporalJoin")) {
          if (rv <= value) latest = Math.max(latest, rv);
        } else if (family.equals("UpdatingJoin")
            || family.equals("IntervalJoin") && Math.abs(value - rv) <= 1000
            || family.equals("WindowJoin")
                && Math.floorDiv(value, 3000) == Math.floorDiv(rv, 3000)) {
          expected.add(result(left[0], value, rv));
        }
      }
      if (latest != Long.MIN_VALUE) expected.add(result(left[0], value, latest));
    }
    expected.sort(String::compareTo);
    return expected;
  }

  private static String result(String key, long left, long right) {
    return "+I|" + key + "|" + left + "|" + (checksum(left) * 31 + checksum(right));
  }

  public static long checksum(long value) {
    if (value % 11 == 0) return -7;
    long result = ((17 * 37 + value) * 37 - 1) * 37 - value;
    result += "λ".hashCode() * 19L + value + 7 + "empty".hashCode() * 19L - 3;
    return result * 41 + value * 2 + ("nested-" + value).hashCode();
  }
}
