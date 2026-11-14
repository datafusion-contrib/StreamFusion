package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.compat.FlinkPlanCompat;

abstract class CanonicalOverCountPayloadJobTest extends CanonicalUnboundedOverJobTest {
  static Stream<Arguments> payloadCases(boolean recovery, String frame) {
    return CanonicalTestMatrix.select(
        Stream.of("proc", "event")
            .flatMap(
                clock ->
                    Stream.of("scalar-varbinary", "array", "row", "map")
                        .flatMap(
                            shape ->
                                Stream.of(1, FlinkPlanCompat.unboundedOverVersion())
                                    .distinct()
                                    .filter(version -> clock.equals("event") || version == 1)
                                    .flatMap(
                                        version ->
                                            CanonicalGroupArithmeticJobTest.cases()
                                                .filter(
                                                    args ->
                                                        recovery
                                                            != args.get()[0].equals("continue"))
                                                .map(
                                                    args ->
                                                        Arguments.of(
                                                            clock + ":" + frame + ":" + shape + ":"
                                                                + version,
                                                            args.get()[0],
                                                            args.get()[1],
                                                            args.get()[2],
                                                            args.get()[3]))))));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var parts = kind.split(":");
    boolean event = parts[0].equals("event");
    String shape = parts[2];
    var root = super.prepare(name, parts[1], mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v "
            + CanonicalMaterializerTypeFixture.sql(shape)
            + ",rt TIMESTAMP(3)"
            + (event ? ",WATERMARK FOR rt AS SOURCE_WATERMARK()" : ""));
    Files.writeString(root.resolve("unbounded-over-version"), parts[3]);
    var inputType =
        RowType.of(
            new BigIntType(), CanonicalMaterializerTypeFixture.type(shape), new TimestampType(3));
    var serializer = new RowDataSerializer(inputType);
    var encoded = new ArrayList<String>();
    var source = Files.readAllLines(root.resolve("input.csv"));
    for (int index = 0; index < source.size(); index++) {
      var cells = source.get(index).split(",");
      var row =
          GenericRowData.of(
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              CanonicalMaterializerTypeFixture.value(
                  shape, cells[2].equals("null") ? null : Long.valueOf(cells[2])),
              TimestampData.fromEpochMillis(Long.parseLong(cells[3])));
      var bytes = new DataOutputSerializer(256);
      serializer.serialize(row, bytes);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
      if (event && !mode.equals("rescale") && (index == 3 || index == 6))
        encoded.add("WATERMARK," + (index == 3 ? "1000" : "1003"));
    }
    if (event) {
      Files.createFile(root.resolve("source-watermarks"));
      encoded.add("WATERMARK,1100");
      Files.writeString(root.resolve("state-ttl"), "1 h");
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,COUNT(v) OVER w,COUNT(k) OVER w FROM input_table"
            + " WINDOW w AS (PARTITION BY k ORDER BY "
            + (event ? "rt" : "pt")
            + (parts[1].equals("rows") ? " ROWS" : " RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (mode.equals("rescale")) {
      var result = new HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        result.put(key == 0 ? "null" : Integer.toString(key), key == 0 ? "6|0" : "6|6");
      return result;
    }
    return Map.of("1", "4|6", "2", kind.contains(":map:") ? "1|3" : "2|3", "null", "2|0");
  }
}
